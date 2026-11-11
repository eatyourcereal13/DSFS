//! Minimal RNG trait used for randomized election timeouts.
//!
//! `dsfs-raft` does not depend on any RNG crate; the caller supplies a
//! `RaftRng` implementation. This keeps the crate pure and its unit tests
//! fully deterministic (tests can supply a fixed RNG).

/// RNG interface for the Raft state machine.
pub trait RaftRng: Send {
    fn gen_range_u64(&mut self, low: u64, high: u64) -> u64;
}

#[cfg(test)]
pub(crate) mod fixed {
    //! Deterministic RNG for tests. Cycles through a fixed sequence.

    use super::RaftRng;

    pub struct CyclicRng {
        values: Vec<u64>,
        index: usize,
    }

    impl CyclicRng {
        pub fn new(values: Vec<u64>) -> Self {
            assert!(!values.is_empty(), "CyclicRng needs at least one value");
            Self { values, index: 0 }
        }
    }

    impl RaftRng for CyclicRng {
        fn gen_range_u64(&mut self, low: u64, high: u64) -> u64 {
            assert!(low < high, "empty range");
            let v = self.values[self.index % self.values.len()];
            self.index += 1;
            low + (v % (high - low))
        }
    }
}
