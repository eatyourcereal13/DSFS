//! Adapter between `dsfs-core` and `dsfs-raft`.

use dsfs_raft::{FollowerReason, RaftEvent, RaftRng, RejectReason};

use crate::log::LogEventKind;
use crate::node::NodeId;
use crate::rng::StreamRng;

impl RaftRng for StreamRng {
    fn gen_range_u64(&mut self, low: u64, high: u64) -> u64 {
        StreamRng::gen_range_u64(self, low, high)
    }
}

/// Converts a Raft-level event into a core-level `LogEventKind`.
#[must_use]
pub fn raft_event_to_log_event(node: NodeId, event: &RaftEvent) -> LogEventKind {
    match event {
        RaftEvent::BecameFollower { term, reason } => LogEventKind::RaftBecameFollower {
            node,
            term: term.0,
            reason: follower_reason_name(reason),
        },
        RaftEvent::BecameCandidate { term } => {
            LogEventKind::RaftBecameCandidate { node, term: term.0 }
        }
        RaftEvent::BecameLeader { term } => LogEventKind::RaftBecameLeader { node, term: term.0 },
        RaftEvent::VotedFor { candidate, term } => LogEventKind::RaftVotedFor {
            node,
            candidate: candidate.0,
            term: term.0,
        },
        RaftEvent::ReceivedVote {
            from,
            term,
            granted,
        } => LogEventKind::RaftReceivedVote {
            node,
            from: from.0,
            term: term.0,
            granted: *granted,
        },
        RaftEvent::TermBumped { from, to } => LogEventKind::RaftTermBumped {
            node,
            from: from.0,
            to: to.0,
        },
        RaftEvent::Appended { index, term } => LogEventKind::RaftAppended {
            node,
            index: *index,
            term: term.0,
        },
        RaftEvent::Applied { index, command } => LogEventKind::RaftApplied {
            node,
            index: *index,
            command: command.clone(),
        },
        RaftEvent::LogTruncated { from_index } => LogEventKind::RaftLogTruncated {
            node,
            from_index: *from_index,
        },
        RaftEvent::LogAppended { from_index, count } => LogEventKind::RaftLogAppended {
            node,
            from_index: *from_index,
            count: *count,
        },
        RaftEvent::CommitAdvanced { from, to } => LogEventKind::RaftCommitAdvanced {
            node,
            from: *from,
            to: *to,
        },
        RaftEvent::ClientCommandRejected { reason } => LogEventKind::RaftClientCommandRejected {
            node,
            reason: reject_reason_name(reason),
        },
    }
}

fn follower_reason_name(r: &FollowerReason) -> &'static str {
    match r {
        FollowerReason::Initial => "initial",
        FollowerReason::HigherTerm => "higher_term",
        FollowerReason::SameTermLeader => "same_term_leader",
        FollowerReason::SameTermCandidate => "same_term_candidate",
        FollowerReason::Timeout => "timeout",
    }
}

fn reject_reason_name(r: &RejectReason) -> &'static str {
    match r {
        RejectReason::NotLeader => "not_leader",
    }
}
