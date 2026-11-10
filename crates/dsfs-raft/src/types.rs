//! Primitive Raft types.

/// Raft term. Monotonically increasing across the cluster's lifetime.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Term(pub u64);

impl Term {
    /// Zero, the initial term before any election.
    pub const ZERO: Self = Self(0);

    /// Returns the next term.
    #[must_use]
    pub fn next(self) -> Self {
        Self(
            self.0
                .checked_add(1)
                .expect("raft term overflow (simulation ran for too long)"),
        )
    }
}

impl std::fmt::Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "T{}", self.0)
    }
}

/// Peer identifier within the Raft cluster.
///
/// This is `u32` and is the caller's responsibility to map to a
/// node identity; `dsfs-raft` does not care.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct PeerId(pub u32);

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "p{}", self.0)
    }
}

/// Current role of a Raft peer. Raft guarantees at most one leader per
/// term; there may be multiple candidates in the same term in different
/// partitions, but not multiple leaders.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
}

/// A single log entry.
///
/// Stage 5 only carries `index` and `term`; `command` is added in Stage 6.
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct LogEntry {
    pub index: u64,
    pub term: Term,
}
