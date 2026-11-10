//! Network simulation.
//!
//! [`NetworkModel`] is the engine-facing interface. [`NullNetwork`] is a
//! trivial stand-in (instant, lossless). [`FullNetwork`] implements per-link
//! latency, jitter, loss, duplication and partitions.
//!
//! Reordering emerges naturally from jitter: when `jitter >= base_latency`,
//! consecutive messages on the same link can arrive out of order. We do not
//! model reordering as an explicit operation because that would require
//! buffering and shuffling at the engine level, which conflicts with the
//! replay guarantee. See DD-009 in `docs/design-decisions.md`.

use std::collections::HashMap;

use crate::message::Message;
use crate::node::NodeId;
use crate::rng::{StreamId, StreamRng};
use crate::time::{Duration, VirtualTime};

/// A single delivery decision produced by a [`NetworkModel`].
///
/// A message may yield zero (dropped), one (normal), or several
/// (`duplicate_probability` > 0) deliveries. Each delivery carries the
/// virtual time at which it should occur and whether it is a duplicate.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Delivery {
    pub at: VirtualTime,
    pub is_duplicate: bool,
}

/// Interface the engine uses to decide when (and whether) a message is
/// delivered.
///
/// Implementations must be deterministic: given the same call sequence and
/// the same initial RNG state, they must return the same results.
pub trait NetworkModel {
    /// Returns all deliveries of a message.
    ///
    /// An empty vector means the message was dropped. A vector with more
    /// than one element means the message was duplicated.
    fn deliveries(
        &mut self,
        now: VirtualTime,
        from: NodeId,
        to: NodeId,
        msg: &Message,
    ) -> Vec<Delivery>;
}

/// Instantaneous, lossless network. Always delivers exactly once, now.
#[derive(Debug, Default)]
pub struct NullNetwork;

impl NetworkModel for NullNetwork {
    fn deliveries(
        &mut self,
        now: VirtualTime,
        _from: NodeId,
        _to: NodeId,
        _msg: &Message,
    ) -> Vec<Delivery> {
        vec![Delivery {
            at: now,
            is_duplicate: false,
        }]
    }
}

/// Per-direction link parameters.
#[derive(Clone, Debug)]
pub struct LinkConfig {
    /// Minimal one-way delay.
    pub base_latency: Duration,
    /// Maximum extra random delay added on top of `base_latency`.
    pub jitter: Duration,
    /// Probability of dropping a message, in `[0.0, 1.0]`.
    pub loss_probability: f64,
    /// Probability of delivering the message an extra time, in `[0.0, 1.0]`.
    pub duplicate_probability: f64,
    /// When `true`, all messages on this link are dropped regardless of
    /// `loss_probability`.
    pub partitioned: bool,
}

impl LinkConfig {
    /// A perfect link: zero latency, no loss, no duplication.
    #[must_use]
    pub fn perfect() -> Self {
        Self {
            base_latency: Duration::ZERO,
            jitter: Duration::ZERO,
            loss_probability: 0.0,
            duplicate_probability: 0.0,
            partitioned: false,
        }
    }

    /// A link with the given base latency, no jitter, no loss, no duplication.
    #[must_use]
    pub fn with_latency(base_latency: Duration) -> Self {
        Self {
            base_latency,
            ..Self::perfect()
        }
    }

    fn validate(&self) {
        debug_assert!(
            (0.0..=1.0).contains(&self.loss_probability),
            "loss_probability must be in [0, 1], got {}",
            self.loss_probability
        );
        debug_assert!(
            (0.0..=1.0).contains(&self.duplicate_probability),
            "duplicate_probability must be in [0, 1], got {}",
            self.duplicate_probability
        );
    }
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self::perfect()
    }
}

/// Full network model.
///
/// Random decisions use per-purpose streams (`NETWORK_LATENCY`,
/// `NETWORK_LOSS`, `NETWORK_DUPLICATE`). Within each stream, draws happen in
/// the order dictated by the engine's `deliveries()` calls, which is
/// deterministic.
///
/// Important ordering guarantee (DD-007): when a message is dropped, the
/// latency RNG is **not** consumed. This keeps the latency stream's index
/// aligned with the number of *delivered* messages, not the number of
/// *attempted* messages. Without this, the presence of loss on one link
/// would shift the latency sequence for all other links.
pub struct FullNetwork {
    default_link: LinkConfig,
    overrides: HashMap<(NodeId, NodeId), LinkConfig>,
    latency_rng: StreamRng,
    loss_rng: StreamRng,
    dup_rng: StreamRng,
}

impl FullNetwork {
    /// Creates a network with the given default link configuration.
    #[must_use]
    pub fn new(master_seed: u64, default_link: LinkConfig) -> Self {
        default_link.validate();
        Self {
            default_link,
            overrides: HashMap::new(),
            latency_rng: StreamRng::new(master_seed, StreamId::NETWORK_LATENCY),
            loss_rng: StreamRng::new(master_seed, StreamId::NETWORK_LOSS),
            dup_rng: StreamRng::new(master_seed, StreamId::NETWORK_DUPLICATE),
        }
    }

    /// Sets the configuration for the directed link `from -> to`.
    pub fn set_link(&mut self, from: NodeId, to: NodeId, cfg: LinkConfig) {
        cfg.validate();
        self.overrides.insert((from, to), cfg);
    }

    /// Returns the effective configuration of `from -> to`.
    #[must_use]
    pub fn link(&self, from: NodeId, to: NodeId) -> &LinkConfig {
        self.overrides
            .get(&(from, to))
            .unwrap_or(&self.default_link)
    }

    /// Returns a mutable reference to the effective configuration of
    /// `from -> to`, inserting a default entry if none exists.
    pub fn link_mut(&mut self, from: NodeId, to: NodeId) -> &mut LinkConfig {
        let default = self.default_link.clone();
        self.overrides.entry((from, to)).or_insert(default)
    }

    /// Cuts the link in both directions between `a` and `b`.
    pub fn partition(&mut self, a: NodeId, b: NodeId) {
        self.link_mut(a, b).partitioned = true;
        self.link_mut(b, a).partitioned = true;
    }

    /// Restores both directions of the link between `a` and `b`.
    pub fn heal(&mut self, a: NodeId, b: NodeId) {
        self.link_mut(a, b).partitioned = false;
        self.link_mut(b, a).partitioned = false;
    }

    /// Partitions the cluster into two groups. Links within a group are
    /// untouched; links across groups are cut in both directions.
    pub fn partition_groups(&mut self, group_a: &[NodeId], group_b: &[NodeId]) {
        for &a in group_a {
            for &b in group_b {
                self.partition(a, b);
            }
        }
    }

    /// Restores all links between two groups.
    pub fn heal_groups(&mut self, group_a: &[NodeId], group_b: &[NodeId]) {
        for &a in group_a {
            for &b in group_b {
                self.heal(a, b);
            }
        }
    }
}

impl NetworkModel for FullNetwork {
    fn deliveries(
        &mut self,
        now: VirtualTime,
        from: NodeId,
        to: NodeId,
        _msg: &Message,
    ) -> Vec<Delivery> {
        let cfg = self
            .overrides
            .get(&(from, to))
            .cloned()
            .unwrap_or_else(|| self.default_link.clone());

        if cfg.partitioned {
            return Vec::new();
        }

        // Loss decision first (see DD-007). If dropped, we must not consume
        // the latency RNG.
        if cfg.loss_probability > 0.0 && self.loss_rng.next_f64() < cfg.loss_probability {
            return Vec::new();
        }

        let base_ns = cfg.base_latency.as_nanos();
        let jitter_ns = cfg.jitter.as_nanos();
        let extra_ns = if jitter_ns > 0 {
            self.latency_rng.gen_range_u64(0, jitter_ns)
        } else {
            0
        };
        let delay_ns = base_ns.saturating_add(extra_ns);

        // Duplicate decision. We always draw (even at probability 0) so the
        // stream's index is deterministic per delivery attempt; this matters
        // if someone later flips `duplicate_probability` at runtime.
        let dup = self.dup_rng.next_f64() < cfg.duplicate_probability;

        let first_at = now + Duration::from_nanos(delay_ns);
        let mut out = Vec::with_capacity(if dup { 2 } else { 1 });
        out.push(Delivery {
            at: first_at,
            is_duplicate: false,
        });
        if dup {
            // Duplicate arrives at the same time as the original. This is a
            // deliberate simplification; Raft must be idempotent anyway, and
            // a delayed duplicate would require its own latency draw, which
            // would complicate the per-link latency stream. See DD-006.
            out.push(Delivery {
                at: first_at,
                is_duplicate: true,
            });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(id: u32) -> NodeId {
        NodeId(id)
    }

    #[test]
    fn default_link_is_perfect() {
        let net = FullNetwork::new(42, LinkConfig::perfect());
        assert_eq!(net.link(n(1), n(2)).base_latency, Duration::ZERO);
        assert_eq!(net.link(n(1), n(2)).loss_probability, 0.0);
    }

    #[test]
    fn null_network_delivers_now_once() {
        let mut net = NullNetwork;
        let t = VirtualTime::from_millis(10);
        let out = net.deliveries(t, n(1), n(2), &Message::Ping);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].at, t);
        assert!(!out[0].is_duplicate);
    }

    #[test]
    fn base_latency_applied() {
        let mut cfg = LinkConfig::perfect();
        cfg.base_latency = Duration::from_millis(20);
        let mut net = FullNetwork::new(42, cfg);
        let now = VirtualTime::from_millis(100);
        let out = net.deliveries(now, n(1), n(2), &Message::Ping);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].at, VirtualTime::from_millis(120));
    }

    #[test]
    fn jitter_within_bounds() {
        let mut cfg = LinkConfig::perfect();
        cfg.base_latency = Duration::from_millis(10);
        cfg.jitter = Duration::from_millis(5);
        let mut net = FullNetwork::new(42, cfg);
        for _ in 0..1_000 {
            let out = net.deliveries(VirtualTime::ZERO, n(1), n(2), &Message::Ping);
            assert_eq!(out.len(), 1);
            let ms = out[0].at.as_millis();
            assert!((10..15).contains(&ms), "delivery {ms}ms out of [10, 15)");
        }
    }

    #[test]
    fn loss_rate_is_close_to_configured() {
        let mut cfg = LinkConfig::perfect();
        cfg.loss_probability = 0.3;
        let mut net = FullNetwork::new(42, cfg);
        let mut dropped = 0;
        let n_trials = 10_000;
        for _ in 0..n_trials {
            let out = net.deliveries(VirtualTime::ZERO, n(1), n(2), &Message::Ping);
            if out.is_empty() {
                dropped += 1;
            }
        }
        let rate = dropped as f64 / n_trials as f64;
        assert!(
            (rate - 0.3).abs() < 0.02,
            "loss rate {rate} too far from 0.3"
        );
    }

    #[test]
    fn duplicate_rate_is_close_to_configured() {
        let mut cfg = LinkConfig::perfect();
        cfg.duplicate_probability = 0.2;
        let mut net = FullNetwork::new(42, cfg);
        let mut duplicates = 0;
        let n_trials = 10_000;
        for _ in 0..n_trials {
            let out = net.deliveries(VirtualTime::ZERO, n(1), n(2), &Message::Ping);
            let dup = out.iter().filter(|d| d.is_duplicate).count();
            assert!(dup <= 1, "at most one duplicate per attempt");
            duplicates += dup;
        }
        let rate = duplicates as f64 / n_trials as f64;
        assert!(
            (rate - 0.2).abs() < 0.02,
            "duplicate rate {rate} too far from 0.2"
        );
    }

    #[test]
    fn partitioned_link_drops_everything() {
        let mut net = FullNetwork::new(42, LinkConfig::perfect());
        net.partition(n(1), n(2));
        for _ in 0..100 {
            assert!(net
                .deliveries(VirtualTime::ZERO, n(1), n(2), &Message::Ping)
                .is_empty());
            assert!(net
                .deliveries(VirtualTime::ZERO, n(2), n(1), &Message::Ping)
                .is_empty());
        }
        assert!(!net
            .deliveries(VirtualTime::ZERO, n(1), n(3), &Message::Ping)
            .is_empty());
    }

    #[test]
    fn heal_restores_link() {
        let mut net = FullNetwork::new(42, LinkConfig::perfect());
        net.partition(n(1), n(2));
        net.heal(n(1), n(2));
        assert!(!net
            .deliveries(VirtualTime::ZERO, n(1), n(2), &Message::Ping)
            .is_empty());
    }

    #[test]
    fn partition_groups_cuts_all_cross_links() {
        let mut net = FullNetwork::new(42, LinkConfig::perfect());
        let group_a = [n(1), n(2)];
        let group_b = [n(3), n(4), n(5)];
        net.partition_groups(&group_a, &group_b);
        for &a in &group_a {
            for &b in &group_b {
                assert!(net
                    .deliveries(VirtualTime::ZERO, a, b, &Message::Ping)
                    .is_empty());
                assert!(net
                    .deliveries(VirtualTime::ZERO, b, a, &Message::Ping)
                    .is_empty());
            }
        }
        assert!(!net
            .deliveries(VirtualTime::ZERO, n(1), n(2), &Message::Ping)
            .is_empty());
        assert!(!net
            .deliveries(VirtualTime::ZERO, n(3), n(4), &Message::Ping)
            .is_empty());
        net.heal_groups(&group_a, &group_b);
        assert!(!net
            .deliveries(VirtualTime::ZERO, n(1), n(3), &Message::Ping)
            .is_empty());
    }

    #[test]
    fn determinism_same_seed_same_decisions() {
        fn run(seed: u64) -> Vec<Vec<Delivery>> {
            let mut cfg = LinkConfig::perfect();
            cfg.base_latency = Duration::from_millis(5);
            cfg.jitter = Duration::from_millis(10);
            cfg.loss_probability = 0.2;
            cfg.duplicate_probability = 0.1;
            let mut net = FullNetwork::new(seed, cfg);
            (0..500)
                .map(|i| {
                    let from = n((i % 5) as u32);
                    let to = n(((i + 1) % 5) as u32);
                    net.deliveries(VirtualTime::from_millis(i), from, to, &Message::Ping)
                })
                .collect()
        }
        assert_eq!(run(1234), run(1234));
    }

    #[test]
    fn different_seeds_diverge() {
        fn run(seed: u64) -> Vec<Vec<Delivery>> {
            let mut cfg = LinkConfig::perfect();
            cfg.base_latency = Duration::from_millis(5);
            cfg.jitter = Duration::from_millis(50);
            let mut net = FullNetwork::new(seed, cfg);
            (0..200)
                .map(|i| {
                    let from = n((i % 5) as u32);
                    let to = n(((i + 1) % 5) as u32);
                    net.deliveries(VirtualTime::ZERO, from, to, &Message::Ping)
                })
                .collect()
        }
        assert_ne!(run(1), run(2));
    }

    /// DD-007: loss does not consume the latency RNG. So the latency
    /// sequence for a *lossless* run must match the latency sequence of a
    /// *lossy* run on the same seed, restricted to non-dropped messages.
    #[test]
    fn loss_does_not_perturb_latency_stream() {
        let jitter_only = {
            let mut cfg = LinkConfig::perfect();
            cfg.base_latency = Duration::from_millis(10);
            cfg.jitter = Duration::from_millis(20);
            let mut net = FullNetwork::new(7, cfg);
            (0..200)
                .map(|_| net.deliveries(VirtualTime::ZERO, n(1), n(2), &Message::Ping)[0].at)
                .collect::<Vec<_>>()
        };

        let lossy_survivors = {
            let mut cfg = LinkConfig::perfect();
            cfg.base_latency = Duration::from_millis(10);
            cfg.jitter = Duration::from_millis(20);
            cfg.loss_probability = 0.5;
            let mut net = FullNetwork::new(7, cfg);
            let mut out = Vec::new();
            for _ in 0..1_000 {
                let ds = net.deliveries(VirtualTime::ZERO, n(1), n(2), &Message::Ping);
                if let Some(d) = ds.first() {
                    out.push(d.at);
                    if out.len() == 200 {
                        break;
                    }
                }
            }
            out
        };

        assert_eq!(jitter_only, lossy_survivors);
    }
}
