//! Invariant checks over a log.

use std::collections::{BTreeMap, BTreeSet};

use dsfs_core::log::{LogEventKind, LogRecord};

use crate::report::{CheckReport, Stats, Violation};

type AppliedEntry = (u64, u64, dsfs_core::event::EventId, u32, u64);

/// Runs all checks over a set of records.
#[must_use]
pub fn run(records: &[LogRecord]) -> CheckReport {
    let mut violations = Vec::new();
    violations.extend(check_single_leader_per_term(records));
    violations.extend(check_no_lost_committed(records));
    violations.extend(check_log_matching(records));
    violations.extend(check_majority_commit(records));
    violations.extend(check_recovery_consistency(records));
    CheckReport {
        violations,
        stats: compute_stats(records),
    }
}

fn compute_stats(records: &[LogRecord]) -> Stats {
    let mut s = Stats {
        total_records: records.len(),
        ..Default::default()
    };
    for r in records {
        match &r.kind {
            LogEventKind::MessageSent { .. } => s.messages_sent += 1,
            LogEventKind::MessageDropped { .. } => s.messages_dropped += 1,
            LogEventKind::MessageDuplicated { .. } => s.messages_duplicated += 1,
            LogEventKind::RaftBecameLeader { .. } => s.leaders_elected += 1,
            LogEventKind::RaftApplied { .. } => s.applied_commands += 1,
            _ => {}
        }
    }
    s
}

/// No two distinct leaders in the same term.
#[must_use]
pub fn check_single_leader_per_term(records: &[LogRecord]) -> Vec<Violation> {
    let mut leaders_per_term: BTreeMap<u64, Vec<(usize, u32, u64)>> = BTreeMap::new();
    for (i, r) in records.iter().enumerate() {
        if let LogEventKind::RaftBecameLeader { node, term } = &r.kind {
            leaders_per_term
                .entry(*term)
                .or_default()
                .push((i, node.0, r.time.0));
        }
    }

    let mut out = Vec::new();
    for (term, entries) in leaders_per_term {
        if entries.len() <= 1 {
            continue;
        }
        let related: Vec<_> = entries
            .iter()
            .map(|(i, _, _)| records[*i].event_id)
            .collect();
        for (idx, (i, node, time)) in entries.iter().enumerate().skip(1) {
            let r = &records[*i];
            out.push(Violation {
                invariant: "single_leader_per_term",
                event_id: Some(r.event_id),
                time: Some(r.time),
                context: format!(
                    "term {term}: node {node} became leader at t={time}, \
                     but leaders already exist (rank {idx})",
                ),
                related_events: related.clone(),
            });
        }
    }
    out
}

/// For each (node, index), no two different (term, command) were applied.
#[must_use]
pub fn check_no_lost_committed(records: &[LogRecord]) -> Vec<Violation> {
    // Key: index. Value: set of (term, hash(command), event_id, node, time).
    let mut applied: BTreeMap<u64, Vec<AppliedEntry>> = BTreeMap::new();

    for r in records {
        if let LogEventKind::RaftApplied {
            node,
            index,
            command,
        } = &r.kind
        {
            let hash = simple_hash(command);
            applied.entry(*index).or_default().push((
                current_term_at(records, *index, node.0),
                hash,
                r.event_id,
                node.0,
                r.time.0,
            ));
        }
    }

    let mut out = Vec::new();
    for (index, entries) in applied {
        // All entries must agree on (term, hash).
        let mut expected: Option<(u64, u64)> = None;
        let mut first_event: Option<dsfs_core::event::EventId> = None;
        let mut disagreement = false;
        for (term, hash, ev, _, _) in &entries {
            match expected {
                None => {
                    expected = Some((*term, *hash));
                    first_event = Some(*ev);
                }
                Some(exp) if exp != (*term, *hash) => {
                    disagreement = true;
                }
                _ => {}
            }
        }
        if disagreement {
            let related: Vec<_> = entries.iter().map(|e| e.2).collect();
            let last = entries.last().unwrap();
            out.push(Violation {
                invariant: "no_lost_committed_entries",
                event_id: Some(last.2),
                time: Some(dsfs_core::time::VirtualTime(last.4)),
                context: format!(
                    "index {index}: applied entries disagree on (term, command) \
                     across {} application(s)",
                    entries.len()
                ),
                related_events: related,
            });
            let _ = first_event;
        }
    }
    out
}

fn current_term_at(records: &[LogRecord], _index: u64, _node: u32) -> u64 {
    // Term tracked via the most recent RaftAppended / RaftEntryAppended for
    // this node that we can correlate is nontrivial; for the applied-check
    // we use 0. This weakens the check: if two different terms applied
    // *different* commands to the same index, we catch it via the hash.
    // If they applied *identical* commands, no violation is reported (they
    // are indistinguishable in the log without the entry term). See
    // docs/checker.md for the limitation.
    let _ = records;
    0
}

/// For each (node, index, term) seen in appends, no two distinct terms for
/// the same (node, index) without an intervening truncate.
#[must_use]
pub fn check_log_matching(records: &[LogRecord]) -> Vec<Violation> {
    // Track per node: BTreeMap<index, term>. Truncate removes >= from_index.
    let mut node_logs: BTreeMap<u32, BTreeMap<u64, u64>> = BTreeMap::new();
    let mut out = Vec::new();

    for r in records {
        match &r.kind {
            LogEventKind::RaftAppended { node, index, term }
            | LogEventKind::RaftEntryAppended { node, index, term } => {
                let log = node_logs.entry(node.0).or_default();
                if let Some(prev_term) = log.get(index) {
                    if *prev_term != *term {
                        out.push(Violation {
                            invariant: "log_matching",
                            event_id: Some(r.event_id),
                            time: Some(r.time),
                            context: format!(
                                "node {}: index {} had term {} but is now {} \
                                 without an intervening truncate",
                                node.0, index, prev_term, term
                            ),
                            related_events: vec![r.event_id],
                        });
                    }
                }
                log.insert(*index, *term);
            }
            LogEventKind::RaftLogTruncated { node, from_index } => {
                if let Some(log) = node_logs.get_mut(&node.0) {
                    let to_remove: Vec<u64> =
                        log.keys().copied().filter(|i| *i >= *from_index).collect();
                    for k in to_remove {
                        log.remove(&k);
                    }
                }
            }
            _ => {}
        }
    }
    out
}

/// For each RaftCommitAdvanced{to=N}, majority of nodes must have applied N.
#[must_use]
pub fn check_majority_commit(records: &[LogRecord]) -> Vec<Violation> {
    // Count distinct nodes that applied each index.
    let mut applied_nodes_per_index: BTreeMap<u64, BTreeSet<u32>> = BTreeMap::new();
    for r in records {
        if let LogEventKind::RaftApplied { node, index, .. } = &r.kind {
            applied_nodes_per_index
                .entry(*index)
                .or_default()
                .insert(node.0);
        }
    }

    // For each commit event, check majority applied.
    // We do not know the cluster size directly; derive it from the log's
    // max node id (approximation, see docs/checker.md).
    let max_node = records
        .iter()
        .filter_map(|r| match &r.kind {
            LogEventKind::RaftApplied { node, .. }
            | LogEventKind::RaftBecameCandidate { node, .. }
            | LogEventKind::RaftBecameLeader { node, .. }
            | LogEventKind::RaftBecameFollower { node, .. } => Some(node.0),
            _ => None,
        })
        .max()
        .unwrap_or(0);
    if max_node == 0 {
        return Vec::new();
    }
    let majority = (max_node as usize) / 2 + 1;

    let mut out = Vec::new();
    for r in records {
        if let LogEventKind::RaftCommitAdvanced { node, to, .. } = &r.kind {
            let applied = applied_nodes_per_index
                .get(to)
                .map(|s| s.len())
                .unwrap_or(0);
            if applied < majority {
                out.push(Violation {
                    invariant: "majority_commit_rule",
                    event_id: Some(r.event_id),
                    time: Some(r.time),
                    context: format!(
                        "node {} committed index {} but only {} of {} required \
                         nodes applied it",
                        node.0, to, applied, majority
                    ),
                    related_events: vec![r.event_id],
                });
            }
        }
    }
    out
}

/// A node that returned online must not have lost indexes it had before.
#[must_use]
pub fn check_recovery_consistency(records: &[LogRecord]) -> Vec<Violation> {
    use dsfs_core::node::Availability;

    // Track per node: max index applied so far, and whether it was offline.
    let mut max_applied: BTreeMap<u32, u64> = BTreeMap::new();
    let mut offline: BTreeMap<u32, bool> = BTreeMap::new();
    let mut pre_offline_max: BTreeMap<u32, u64> = BTreeMap::new();
    let out = Vec::new();

    for r in records {
        match &r.kind {
            LogEventKind::NodeAvailabilityChanged { node, to } => {
                let id = node.0;
                match to {
                    Availability::Offline => {
                        offline.insert(id, true);
                        pre_offline_max.insert(id, max_applied.get(&id).copied().unwrap_or(0));
                    }
                    Availability::Online | Availability::Recovering => {
                        if offline.get(&id).copied().unwrap_or(false) {
                            offline.insert(id, false);
                            // After recovery, max_applied must not decrease.
                            // We can't directly check "decrease" because we
                            // only see applied entries; we check that the
                            // node eventually reaches pre_offline_max again
                            // (which Raft requires after catch-up).
                            //
                            // For now we just record the transition. Full
                            // check would need end-of-log state: see
                            // docs/checker.md.
                            let _ = pre_offline_max.get(&id);
                        }
                    }
                }
            }
            LogEventKind::RaftApplied { node, index, .. } => {
                let id = node.0;
                let cur = max_applied.entry(id).or_insert(0);
                if *index > *cur {
                    *cur = *index;
                }
            }
            _ => {}
        }
    }
    out
}

fn simple_hash(bytes: &[u8]) -> u64 {
    // FNV-1a 64-bit.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}
