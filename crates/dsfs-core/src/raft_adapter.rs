//! Adapter between `dsfs-core` and `dsfs-raft`.
//!
//! The Raft crate is deliberately independent; here we provide the glue:
//! a `RaftRng` implementation backed by `StreamRng`, and helpers to map
//! Raft-level events to core-level `LogRecord`s.

use dsfs_raft::{FollowerReason, RaftEvent, RaftRng};

use crate::log::LogEventKind;
use crate::node::NodeId;
use crate::rng::StreamRng;

impl RaftRng for StreamRng {
    fn gen_range_u64(&mut self, low: u64, high: u64) -> u64 {
        // Delegate to the inherent method.
        StreamRng::gen_range_u64(self, low, high)
    }
}

/// Converts a Raft-level event into a core-level `LogEventKind`.
///
/// `node` is the id of the Raft peer emitting the event.
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
