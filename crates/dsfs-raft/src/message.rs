//! Raft messages exchanged between peers.
//!
//! All variants carry a `term` field, which is the sender's `currentTerm`
//! at send time. The receiver always applies the "if RPC term > my term,
//! step down and update term" rule (§5.1 of the Raft paper).

use crate::types::{LogEntry, PeerId, Term};

/// A Raft message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RaftMessage {
    /// Sent by a candidate to gather votes (§5.2).
    RequestVote {
        term: Term,
        candidate_id: PeerId,
        last_log_index: u64,
        last_log_term: Term,
    },
    /// Reply to `RequestVote`.
    RequestVoteReply { term: Term, vote_granted: bool },
    /// Sent by a leader to replicate entries or as a heartbeat (§5.3).
    AppendEntries {
        term: Term,
        leader_id: PeerId,
        prev_log_index: u64,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    /// Reply to `AppendEntries`.
    AppendEntriesReply {
        term: Term,
        success: bool,
        match_index: u64,
    },
}
