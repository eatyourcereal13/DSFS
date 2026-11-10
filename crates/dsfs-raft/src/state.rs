//! Internal state of a Raft peer, distinct from persistent state.
//!
//! Stage 5: only election-related state. Stage 6 adds `next_index`,
//! `match_index`, `last_applied`.

use std::collections::BTreeSet;

use crate::types::{PeerId, Role};

/// Volatile state of a Raft peer.
#[derive(Clone, Debug)]
pub(crate) enum PeerState {
    Follower,
    Candidate {
        /// Votes received in the current election, including self-vote.
        votes: BTreeSet<PeerId>,
    },
    Leader,
}

impl PeerState {
    pub(crate) fn role(&self) -> Role {
        match self {
            PeerState::Follower => Role::Follower,
            PeerState::Candidate { .. } => Role::Candidate,
            PeerState::Leader => Role::Leader,
        }
    }
}

/// Creates a fresh `Candidate` state with the given vote set.
pub(crate) fn new_candidate_state(votes: BTreeSet<PeerId>) -> PeerState {
    PeerState::Candidate { votes }
}

/// Creates a fresh `Leader` state.
pub(crate) fn new_leader_state() -> PeerState {
    PeerState::Leader
}
