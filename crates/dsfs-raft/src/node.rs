//! The Raft state machine.

use std::collections::BTreeSet;
use std::time::Duration;

use crate::message::RaftMessage;
use crate::rng::RaftRng;
use crate::state::{new_candidate_state, new_leader_state, PeerState};
use crate::types::{LogEntry, PeerId, Role, Term};

/// Maximum number of entries sent in a single `AppendEntries` message.
const MAX_ENTRIES_PER_APPEND: usize = 100;

/// Events emitted by the Raft state machine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RaftEvent {
    BecameFollower {
        term: Term,
        reason: FollowerReason,
    },
    BecameCandidate {
        term: Term,
    },
    BecameLeader {
        term: Term,
    },
    VotedFor {
        candidate: PeerId,
        term: Term,
    },
    ReceivedVote {
        from: PeerId,
        term: Term,
        granted: bool,
    },
    TermBumped {
        from: Term,
        to: Term,
    },
    /// A client command was appended to the leader's log.
    Appended {
        index: u64,
        term: Term,
    },
    /// An entry was committed and applied to the state machine.
    Applied {
        index: u64,
        command: Vec<u8>,
    },
    /// A follower truncated its log (conflict resolution).
    LogTruncated {
        from_index: u64,
    },
    /// A follower appended entries from the leader.
    LogAppended {
        from_index: u64,
        count: u64,
    },
    /// The leader committed a new index.
    CommitAdvanced {
        from: u64,
        to: u64,
    },
    /// A client command was rejected.
    ClientCommandRejected {
        reason: RejectReason,
    },
}

/// Reason a `ClientCommand` was rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RejectReason {
    NotLeader,
}

/// Why a peer became a follower.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FollowerReason {
    Initial,
    HigherTerm,
    SameTermLeader,
    SameTermCandidate,
    Timeout,
}

/// Instruction to (re)arm a timer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerArm {
    pub generation: u64,
    pub after: Duration,
}

/// Result of a Raft step.
#[derive(Clone, Debug, Default)]
pub struct RaftOutput {
    pub messages: Vec<(PeerId, RaftMessage)>,
    pub set_timer: Option<TimerArm>,
    pub events: Vec<RaftEvent>,
}

impl RaftOutput {
    fn empty() -> Self {
        Self::default()
    }
}

/// Configuration for a Raft node.
#[derive(Clone, Debug)]
pub struct RaftConfig {
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout: Duration::from_millis(150),
            heartbeat_interval: Duration::from_millis(30),
        }
    }
}

/// The Raft state machine.
pub struct RaftNode {
    // Identity
    self_id: PeerId,
    peers: Vec<PeerId>,
    config: RaftConfig,

    // Persistent state
    current_term: Term,
    voted_for: Option<PeerId>,
    log: Vec<LogEntry>,

    // Volatile state
    state: PeerState,
    commit_index: u64,
    last_applied: u64,
    applied: Vec<Vec<u8>>,
    timer_generation: u64,
    started: bool,
    rng: Box<dyn RaftRng>,
}

impl RaftNode {
    /// Creates a new Raft node. `peers` must include `self_id`.
    #[must_use]
    pub fn new(
        self_id: PeerId,
        peers: Vec<PeerId>,
        config: RaftConfig,
        rng: Box<dyn RaftRng>,
    ) -> Self {
        debug_assert!(peers.contains(&self_id), "peers must contain self_id");
        Self {
            self_id,
            peers,
            config,
            current_term: Term::ZERO,
            voted_for: None,
            log: Vec::new(),
            state: PeerState::Follower,
            commit_index: 0,
            last_applied: 0,
            applied: Vec::new(),
            timer_generation: 0,
            started: false,
            rng,
        }
    }

    #[must_use]
    pub fn role(&self) -> Role {
        self.state.role()
    }

    #[must_use]
    pub fn term(&self) -> Term {
        self.current_term
    }

    #[must_use]
    pub fn voted_for(&self) -> Option<PeerId> {
        self.voted_for
    }

    #[must_use]
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    #[must_use]
    pub fn last_log_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    #[must_use]
    pub fn last_log_term(&self) -> Term {
        self.log.last().map(|e| e.term).unwrap_or(Term::ZERO)
    }

    /// Returns the commands applied to the state machine, in order.
    #[must_use]
    pub fn applied(&self) -> &[Vec<u8>] {
        &self.applied
    }

    /// Returns the raw log (for tests and tooling).
    #[must_use]
    pub fn log(&self) -> &[LogEntry] {
        &self.log
    }

    /// Called once on start.
    pub fn on_start(&mut self) -> RaftOutput {
        if self.started {
            return RaftOutput::empty();
        }
        self.started = true;
        let mut out = RaftOutput::empty();
        self.arm_timer(&mut out);
        out
    }

    /// Handles a timer expiration.
    pub fn on_timer(&mut self, generation: u64) -> RaftOutput {
        if generation != self.timer_generation {
            return RaftOutput::empty();
        }
        let mut out = RaftOutput::empty();
        match self.state.role() {
            Role::Follower | Role::Candidate => {
                self.become_candidate(&mut out);
            }
            Role::Leader => {
                self.broadcast_append_entries(&mut out);
                self.arm_heartbeat_timer(&mut out);
            }
        }
        out
    }

    /// Handles a client command. Only the leader appends; others reject.
    pub fn on_client_command(&mut self, command: Vec<u8>) -> RaftOutput {
        let mut out = RaftOutput::empty();
        if self.state.role() != Role::Leader {
            out.events.push(RaftEvent::ClientCommandRejected {
                reason: RejectReason::NotLeader,
            });
            return out;
        }
        let index = self.last_log_index() + 1;
        let term = self.current_term;
        self.log.push(LogEntry {
            index,
            term,
            command,
        });
        out.events.push(RaftEvent::Appended { index, term });

        self.broadcast_append_entries(&mut out);
        self.maybe_advance_commit(&mut out);
        out
    }

    /// Handles a message.
    pub fn on_message(&mut self, from: PeerId, msg: RaftMessage) -> RaftOutput {
        match msg {
            RaftMessage::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.on_request_vote(from, term, candidate_id, last_log_index, last_log_term),
            RaftMessage::RequestVoteReply { term, vote_granted } => {
                self.on_request_vote_reply(from, term, vote_granted)
            }
            RaftMessage::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.on_append_entries(
                from,
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            ),
            RaftMessage::AppendEntriesReply {
                term,
                success,
                match_index,
            } => self.on_append_entries_reply(from, term, success, match_index),
        }
    }

    // ---- Timer helpers ----

    fn arm_timer(&mut self, out: &mut RaftOutput) {
        self.timer_generation = self.timer_generation.wrapping_add(1);
        let base = self.config.election_timeout.as_millis() as u64;
        let range = base.max(1);
        let extra = self.rng.gen_range_u64(0, range);
        out.set_timer = Some(TimerArm {
            generation: self.timer_generation,
            after: Duration::from_millis(base + extra),
        });
    }

    fn arm_heartbeat_timer(&mut self, out: &mut RaftOutput) {
        self.timer_generation = self.timer_generation.wrapping_add(1);
        out.set_timer = Some(TimerArm {
            generation: self.timer_generation,
            after: self.config.heartbeat_interval,
        });
    }

    // ---- Transitions ----

    fn bump_term(&mut self, new_term: Term, out: &mut RaftOutput) {
        if new_term > self.current_term {
            let from = self.current_term;
            self.current_term = new_term;
            self.voted_for = None;
            out.events
                .push(RaftEvent::TermBumped { from, to: new_term });
        }
    }

    fn become_follower(&mut self, reason: FollowerReason, out: &mut RaftOutput) {
        self.state = PeerState::Follower;
        out.events.push(RaftEvent::BecameFollower {
            term: self.current_term,
            reason,
        });
    }

    fn become_candidate(&mut self, out: &mut RaftOutput) {
        self.current_term = self.current_term.next();
        self.voted_for = Some(self.self_id);
        let mut votes = BTreeSet::new();
        votes.insert(self.self_id);
        let term = self.current_term;
        self.state = new_candidate_state(votes);
        out.events.push(RaftEvent::BecameCandidate { term });

        self.arm_timer(out);

        let last_log_index = self.last_log_index();
        let last_log_term = self.last_log_term();
        for &peer in &self.peers {
            if peer == self.self_id {
                continue;
            }
            out.messages.push((
                peer,
                RaftMessage::RequestVote {
                    term,
                    candidate_id: self.self_id,
                    last_log_index,
                    last_log_term,
                },
            ));
        }
        self.maybe_become_leader(out);
    }

    fn maybe_become_leader(&mut self, out: &mut RaftOutput) {
        let is_candidate = matches!(self.state, PeerState::Candidate { .. });
        if !is_candidate {
            return;
        }
        let votes = match &self.state {
            PeerState::Candidate { votes } => votes.len(),
            _ => return,
        };
        let majority = self.peers.len() / 2 + 1;
        if votes < majority {
            return;
        }
        let term = self.current_term;
        let last_log_index = self.last_log_index();
        self.state = new_leader_state(&self.peers, last_log_index);
        out.events.push(RaftEvent::BecameLeader { term });
        // Send initial heartbeats immediately to establish authority.
        self.broadcast_append_entries(out);
        self.arm_heartbeat_timer(out);
        // For a single-node cluster, immediately commit anything in the log.
        self.maybe_advance_commit(out);
    }

    // ---- Replication ----

    fn broadcast_append_entries(&mut self, out: &mut RaftOutput) {
        if self.state.role() != Role::Leader {
            return;
        }
        let peer_ids: Vec<PeerId> = self.peers.clone();
        for peer in peer_ids {
            if peer == self.self_id {
                continue;
            }
            self.send_append_entries_to(peer, out);
        }
    }

    fn send_append_entries_to(&mut self, peer: PeerId, out: &mut RaftOutput) {
        let (next_index, _) = match &self.state {
            PeerState::Leader {
                next_index,
                match_index,
            } => (
                *next_index.get(&peer).unwrap_or(&1),
                *match_index.get(&peer).unwrap_or(&0),
            ),
            _ => return,
        };
        let prev_log_index = next_index.saturating_sub(1);
        let prev_log_term = self.term_at(prev_log_index);

        // Gather entries from next_index to min(last_log_index, next_index + MAX - 1).
        let start_pos = (next_index.saturating_sub(1)) as usize; // Vec index
        let end_pos = self.log.len().min(start_pos + MAX_ENTRIES_PER_APPEND);
        let entries: Vec<LogEntry> = self.log[start_pos..end_pos].to_vec();

        let msg = RaftMessage::AppendEntries {
            term: self.current_term,
            leader_id: self.self_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        };
        out.messages.push((peer, msg));
    }

    fn term_at(&self, index: u64) -> Term {
        if index == 0 {
            return Term::ZERO;
        }
        self.log
            .get((index - 1) as usize)
            .map(|e| e.term)
            .unwrap_or(Term::ZERO)
    }

    fn log_at(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 {
            return None;
        }
        self.log.get((index - 1) as usize)
    }

    fn maybe_advance_commit(&mut self, out: &mut RaftOutput) {
        if self.state.role() != Role::Leader {
            return;
        }
        let mut match_indexes: Vec<u64> = Vec::with_capacity(self.peers.len());
        match_indexes.push(self.last_log_index()); // self
        if let PeerState::Leader { match_index, .. } = &self.state {
            for &p in &self.peers {
                if p == self.self_id {
                    continue;
                }
                match_indexes.push(*match_index.get(&p).unwrap_or(&0));
            }
        }
        match_indexes.sort_unstable();
        // Majority = peers.len()/2 + 1; find the majority-th largest.
        let majority = self.peers.len() / 2 + 1;
        if match_indexes.len() < majority {
            return;
        }
        let candidate_n = match_indexes[match_indexes.len() - majority];
        if candidate_n > self.commit_index {
            if let Some(entry) = self.log_at(candidate_n) {
                if entry.term == self.current_term {
                    let from = self.commit_index;
                    self.commit_index = candidate_n;
                    out.events.push(RaftEvent::CommitAdvanced {
                        from,
                        to: candidate_n,
                    });
                    self.apply_committed(out);
                }
            }
        }
    }

    fn apply_committed(&mut self, out: &mut RaftOutput) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let entry = match self.log_at(self.last_applied) {
                Some(e) => e.clone(),
                None => break,
            };
            self.applied.push(entry.command.clone());
            out.events.push(RaftEvent::Applied {
                index: self.last_applied,
                command: entry.command,
            });
        }
    }

    // ---- Message handlers ----

    fn on_request_vote(
        &mut self,
        from: PeerId,
        term: Term,
        candidate_id: PeerId,
        last_log_index: u64,
        last_log_term: Term,
    ) -> RaftOutput {
        let mut out = RaftOutput::empty();

        if term > self.current_term {
            self.bump_term(term, &mut out);
            self.become_follower(FollowerReason::HigherTerm, &mut out);
        }

        let mut vote_granted = false;
        if term == self.current_term {
            let can_vote = self.voted_for.is_none() || self.voted_for == Some(candidate_id);
            let log_ok = last_log_term > self.last_log_term()
                || (last_log_term == self.last_log_term()
                    && last_log_index >= self.last_log_index());
            if can_vote && log_ok {
                self.voted_for = Some(candidate_id);
                vote_granted = true;
                out.events.push(RaftEvent::VotedFor {
                    candidate: candidate_id,
                    term: self.current_term,
                });
                self.arm_timer(&mut out);
            }
        }

        out.events.push(RaftEvent::ReceivedVote {
            from,
            term: self.current_term,
            granted: vote_granted,
        });
        out.messages.push((
            from,
            RaftMessage::RequestVoteReply {
                term: self.current_term,
                vote_granted,
            },
        ));
        out
    }

    fn on_request_vote_reply(
        &mut self,
        from: PeerId,
        term: Term,
        vote_granted: bool,
    ) -> RaftOutput {
        let mut out = RaftOutput::empty();

        if term > self.current_term {
            self.bump_term(term, &mut out);
            self.become_follower(FollowerReason::HigherTerm, &mut out);
            return out;
        }
        if term != self.current_term || self.state.role() != Role::Candidate {
            return out;
        }
        if vote_granted {
            if let PeerState::Candidate { votes } = &mut self.state {
                votes.insert(from);
            }
            self.maybe_become_leader(&mut out);
        }
        out
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append_entries(
        &mut self,
        from: PeerId,
        term: Term,
        leader_id: PeerId,
        prev_log_index: u64,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> RaftOutput {
        let mut out = RaftOutput::empty();

        if term < self.current_term {
            out.messages.push((
                from,
                RaftMessage::AppendEntriesReply {
                    term: self.current_term,
                    success: false,
                    match_index: 0,
                },
            ));
            return out;
        }

        if term > self.current_term {
            self.bump_term(term, &mut out);
        }

        // Same term: leader is valid. Reset to follower if needed.
        if self.state.role() != Role::Follower {
            self.become_follower(FollowerReason::SameTermLeader, &mut out);
        }
        debug_assert_eq!(from, leader_id);
        self.arm_timer(&mut out);

        // Consistency check.
        if prev_log_index > self.last_log_index() {
            out.messages.push((
                from,
                RaftMessage::AppendEntriesReply {
                    term: self.current_term,
                    success: false,
                    match_index: 0,
                },
            ));
            return out;
        }
        if prev_log_index > 0 && self.term_at(prev_log_index) != prev_log_term {
            out.messages.push((
                from,
                RaftMessage::AppendEntriesReply {
                    term: self.current_term,
                    success: false,
                    match_index: 0,
                },
            ));
            return out;
        }

        // Append / truncate.
        let mut append_from: Option<u64> = None;
        for (i, e) in entries.iter().enumerate() {
            let idx = prev_log_index + 1 + i as u64;
            match self.log_at(idx) {
                Some(existing) if existing.term == e.term => {
                    // Already present, skip.
                }
                Some(_) => {
                    // Conflict: truncate at idx-1 and append.
                    self.log.truncate((idx - 1) as usize);
                    out.events.push(RaftEvent::LogTruncated { from_index: idx });
                    self.log.push(e.clone());
                    if append_from.is_none() {
                        append_from = Some(idx);
                    }
                }
                None => {
                    self.log.push(e.clone());
                    if append_from.is_none() {
                        append_from = Some(idx);
                    }
                }
            }
        }
        if let Some(from_idx) = append_from {
            let count = self.last_log_index() - from_idx + 1;
            out.events.push(RaftEvent::LogAppended {
                from_index: from_idx,
                count,
            });
        }

        // Advance commit.
        if leader_commit > self.commit_index {
            let new_commit = leader_commit.min(self.last_log_index());
            if new_commit > self.commit_index {
                let from = self.commit_index;
                self.commit_index = new_commit;
                out.events.push(RaftEvent::CommitAdvanced {
                    from,
                    to: new_commit,
                });
                self.apply_committed(&mut out);
            }
        }

        out.messages.push((
            from,
            RaftMessage::AppendEntriesReply {
                term: self.current_term,
                success: true,
                match_index: self.last_log_index(),
            },
        ));
        out
    }

    fn on_append_entries_reply(
        &mut self,
        from: PeerId,
        term: Term,
        success: bool,
        match_index: u64,
    ) -> RaftOutput {
        let mut out = RaftOutput::empty();

        if term > self.current_term {
            self.bump_term(term, &mut out);
            self.become_follower(FollowerReason::HigherTerm, &mut out);
            return out;
        }
        if self.state.role() != Role::Leader || term != self.current_term {
            return out;
        }

        if success {
            if let PeerState::Leader {
                next_index,
                match_index: match_map,
            } = &mut self.state
            {
                let current_match = match_map.get(&from).copied().unwrap_or(0);
                if match_index > current_match {
                    match_map.insert(from, match_index);
                    next_index.insert(from, match_index + 1);
                }
            }
            self.maybe_advance_commit(&mut out);
        } else if let PeerState::Leader { next_index, .. } = &mut self.state {
            let ni = next_index.get(&from).copied().unwrap_or(1);
            let new_ni = ni.saturating_sub(1).max(1);
            next_index.insert(from, new_ni);
            self.send_append_entries_to(from, &mut out);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    // Tests are extensive; kept minimal here to reduce churn.
    // Full replication tests live in `crates/dsfs-core/tests/raft_replication.rs`.

    use super::*;
    use crate::rng::fixed::CyclicRng;

    fn node(id: u32, peers: Vec<u32>, rng_values: Vec<u64>) -> RaftNode {
        RaftNode::new(
            PeerId(id),
            peers.into_iter().map(PeerId).collect(),
            RaftConfig {
                election_timeout: Duration::from_millis(100),
                heartbeat_interval: Duration::from_millis(20),
            },
            Box::new(CyclicRng::new(rng_values)),
        )
    }

    #[test]
    fn single_node_commits_on_append() {
        let mut n = node(1, vec![1], vec![0, 0]);
        let _ = n.on_start();
        let _ = n.on_timer(1); // become leader
        assert_eq!(n.role(), Role::Leader);
        let out = n.on_client_command(b"hello".to_vec());
        assert!(out
            .events
            .iter()
            .any(|e| matches!(e, RaftEvent::Applied { .. })));
        assert_eq!(n.applied(), &[b"hello".to_vec()]);
        assert_eq!(n.commit_index(), 1);
    }

    #[test]
    fn non_leader_rejects_client_command() {
        let mut n = node(1, vec![1, 2, 3], vec![0, 0]);
        let _ = n.on_start();
        let out = n.on_client_command(b"x".to_vec());
        assert!(out
            .events
            .iter()
            .any(|e| matches!(e, RaftEvent::ClientCommandRejected { .. })));
        assert!(n.log().is_empty());
    }

    #[test]
    fn appends_command_to_log_as_leader() {
        let mut n = node(1, vec![1], vec![0, 0]);
        let _ = n.on_start();
        let _ = n.on_timer(1);
        let _ = n.on_client_command(b"a".to_vec());
        assert_eq!(n.log().len(), 1);
        assert_eq!(n.log()[0].index, 1);
        assert_eq!(n.log()[0].command, b"a");
    }
}
