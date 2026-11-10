//! Stream-partitioned deterministic RNG (DD-003).
//!
//! Each [`StreamId`] maps to an independent `ChaCha8Rng` derived from a
//! single master seed. This isolates the effects of adding new random
//! decisions in one module from all other modules.

use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// Stable identifier of a random stream.
///
/// Numeric values are part of the reproducibility contract: existing values
/// must never change. New values are appended. Ranges are documented in
/// `docs/design-decisions.md` DD-003.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct StreamId(pub u64);

impl StreamId {
    /// Network: per-link latency jitter.
    pub const NETWORK_LATENCY: Self = Self(0);
    /// Network: per-link packet loss.
    pub const NETWORK_LOSS: Self = Self(1);
    /// Network: per-link duplication.
    pub const NETWORK_DUPLICATE: Self = Self(2);
    /// Network: per-link reordering.
    pub const NETWORK_REORDER: Self = Self(3);

    /// Fault injection: scenario-driven random choices.
    pub const FAULT_INJECTION: Self = Self(1000);

    /// Raft stream for a given node and purpose.
    #[must_use]
    pub const fn raft(node_id: u32, purpose: RaftStreamPurpose) -> Self {
        Self(100 + (node_id as u64) * 10 + purpose as u64)
    }
}

/// Raft per-node random purposes.
///
/// Each purpose must use a **separate** stream so that, for example,
/// heartbeat jitter does not shift the election-timeout sequence.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
#[repr(u64)]
pub enum RaftStreamPurpose {
    /// Randomized election timeout.
    ElectionTimeout = 0,
    /// Heartbeat interval jitter (if used).
    HeartbeatJitter = 1,
    /// Reserved.
    Other = 9,
}

/// Mixes a `StreamId` into a `u64` seed via SplitMix64.
///
/// SplitMix64 is a well-known bijection on `u64`; used here purely to spread
/// nearby `StreamId` values across the seed space.
#[must_use]
fn mix_id(stream_id: u64) -> u64 {
    let mut z = stream_id.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// An independent random stream derived from a master seed and a [`StreamId`].
///
/// Constructing two `StreamRng`s with the same `(master_seed, stream_id)`
/// always yields identical sequences. Constructing them with different
/// `stream_id`s yields independent sequences, and — crucially — changing
/// one `stream_id` never changes the sequence of another.
#[derive(Clone, Debug)]
pub struct StreamRng {
    inner: ChaCha8Rng,
}

impl StreamRng {
    /// Creates a stream RNG from a master seed and a stream identifier.
    #[must_use]
    pub fn new(master_seed: u64, stream_id: StreamId) -> Self {
        let seed = master_seed ^ mix_id(stream_id.0);
        Self {
            inner: ChaCha8Rng::seed_from_u64(seed),
        }
    }

    /// Returns the next `u64`.
    pub fn next_u64(&mut self) -> u64 {
        self.inner.next_u64()
    }

    /// Returns a `f64` in `[0.0, 1.0)`.
    pub fn next_f64(&mut self) -> f64 {
        // 53-bit mantissa is enough for a double.
        (self.inner.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }

    /// Returns a `u64` in `[low, high)`; panics if `low >= high`.
    pub fn gen_range_u64(&mut self, low: u64, high: u64) -> u64 {
        assert!(low < high, "empty range: {low}..{high}");
        let span = high - low;
        low + (self.inner.next_u64() % span)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_stream_same_sequence() {
        let mut a = StreamRng::new(42, StreamId::NETWORK_LATENCY);
        let mut b = StreamRng::new(42, StreamId::NETWORK_LATENCY);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_streams_diverge() {
        let mut a = StreamRng::new(42, StreamId::NETWORK_LATENCY);
        let mut b = StreamRng::new(42, StreamId::NETWORK_LOSS);
        let mut equal = 0;
        for _ in 0..1000 {
            if a.next_u64() == b.next_u64() {
                equal += 1;
            }
        }
        assert!(equal < 5, "streams should be effectively independent");
    }

    #[test]
    fn different_master_seeds_diverge() {
        let mut a = StreamRng::new(1, StreamId::NETWORK_LATENCY);
        let mut b = StreamRng::new(2, StreamId::NETWORK_LATENCY);
        let mut equal = 0;
        for _ in 0..1000 {
            if a.next_u64() == b.next_u64() {
                equal += 1;
            }
        }
        assert!(equal < 5);
    }

    /// Core of DD-003: consuming one stream does not affect another.
    #[test]
    fn consuming_one_stream_does_not_affect_another() {
        let baseline: Vec<u64> = {
            let mut r = StreamRng::new(7, StreamId::NETWORK_LATENCY);
            (0..100).map(|_| r.next_u64()).collect()
        };

        let after_other_consumed: Vec<u64> = {
            let mut loss = StreamRng::new(7, StreamId::NETWORK_LOSS);
            for _ in 0..10_000 {
                let _ = loss.next_u64();
            }
            let mut r = StreamRng::new(7, StreamId::NETWORK_LATENCY);
            (0..100).map(|_| r.next_u64()).collect()
        };

        assert_eq!(baseline, after_other_consumed);
    }

    #[test]
    fn gen_range_basic() {
        let mut r = StreamRng::new(42, StreamId::NETWORK_LATENCY);
        for _ in 0..10_000 {
            let v = r.gen_range_u64(10, 20);
            assert!((10..20).contains(&v));
        }
    }

    #[test]
    fn raft_stream_id_layout() {
        assert_ne!(
            StreamId::raft(1, RaftStreamPurpose::ElectionTimeout),
            StreamId::raft(2, RaftStreamPurpose::ElectionTimeout),
        );
        assert_ne!(
            StreamId::raft(1, RaftStreamPurpose::ElectionTimeout),
            StreamId::raft(1, RaftStreamPurpose::HeartbeatJitter),
        );
    }
}
