//! Internal state of a Raft peer.
//!
//! Stage 6 adds `next_index` and `match_index` for the leader.

use std::collections::{BTreeMap, BTreeSet};

use crate::types::{PeerId, Role};

/// Volatile state of a Raft peer.
#[derive(Clone, Debug)]
pub(crate) enum PeerState {
    Follower,
    Candidate {
        votes: BTreeSet<PeerId>,
    },
    Leader {
        /// `next_index[peer]` — next log index to send to `peer`.
        next_index: BTreeMap<PeerId, u64>,
        /// `match_index[peer]` — highest log index known replicated on `peer`.
        match_index: BTreeMap<PeerId, u64>,
    },
}

impl PeerState {
    pub(crate) fn role(&self) -> Role {
        match self {
            PeerState::Follower => Role::Follower,
            PeerState::Candidate { .. } => Role::Candidate,
            PeerState::Leader { .. } => Role::Leader,
        }
    }
}

pub(crate) fn new_candidate_state(votes: BTreeSet<PeerId>) -> PeerState {
    PeerState::Candidate { votes }
}

pub(crate) fn new_leader_state(peers: &[PeerId], last_log_index: u64) -> PeerState {
    let mut next_index = BTreeMap::new();
    let mut match_index = BTreeMap::new();
    for &p in peers {
        next_index.insert(p, last_log_index + 1);
        match_index.insert(p, 0);
    }
    PeerState::Leader {
        next_index,
        match_index,
    }
}
