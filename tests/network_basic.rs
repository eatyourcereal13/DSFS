//! Integration tests for `FullNetwork` connected to the engine.

use dsfs_core::engine::{EngineConfig, FullNetwork, LinkConfig, RunOutcome, SimulationEngine};
use dsfs_core::log::{InMemoryLog, LogEventKind};
use dsfs_core::message::Message;
use dsfs_core::network::NetworkModel;
use dsfs_core::node::{NodeId, PingNode};
use dsfs_core::time::{Duration, VirtualTime};

fn build_with_network(
    seed: u64,
    link: LinkConfig,
    max_events: u64,
) -> SimulationEngine<InMemoryLog, FullNetwork> {
    let cfg = EngineConfig {
        master_seed: seed,
        max_events,
    };
    let net = FullNetwork::new(seed, link);
    let mut e = SimulationEngine::new(cfg, net, InMemoryLog::new());
    for id in 1..=5 {
        e.add_ping_node(PingNode::new(NodeId(id), Duration::from_millis(100)))
            .unwrap();
    }
    e
}

#[test]
fn cluster_works_with_zero_latency_network() {
    let mut e = build_with_network(42, LinkConfig::perfect(), 500);
    let outcome = e.run().unwrap();
    assert!(matches!(outcome, RunOutcome::HitEventLimit { .. }));
    let log = e.into_log();
    assert!(log
        .records()
        .iter()
        .any(|r| matches!(r.kind, LogEventKind::MessageDelivered { .. })));
}

#[test]
fn partition_produces_drops_across_groups() {
    let mut e = build_with_network(42, LinkConfig::perfect(), 500);
    let group_a = [NodeId(1), NodeId(2)];
    let group_b = [NodeId(3), NodeId(4), NodeId(5)];
    e.network_mut().partition_groups(&group_a, &group_b);
    e.run().unwrap();
    let log = e.into_log();
    let cross_drops: usize = log
        .records()
        .iter()
        .filter(|r| match &r.kind {
            LogEventKind::MessageDropped { from, to } => {
                (group_a.contains(from) && group_b.contains(to))
                    || (group_b.contains(from) && group_a.contains(to))
            }
            _ => false,
        })
        .count();
    assert!(cross_drops > 0, "expected cross-group drops after partition");
}

#[test]
fn determinism_with_loss_and_jitter() {
    fn run_once() -> Vec<(u64, u64, String)> {
        let mut link = LinkConfig::perfect();
        link.base_latency = Duration::from_millis(20);
        link.jitter = Duration::from_millis(10);
        link.loss_probability = 0.15;
        let mut e = build_with_network(1234, link, 500);
        e.run().unwrap();
        let log = e.into_log();
        log.records()
            .iter()
            .map(|r| (r.event_id.0, r.time.0, format!("{:?}", r.kind)))
            .collect()
    }
    assert_eq!(run_once(), run_once());
}

#[test]
fn duplicate_probability_produces_duplicated_log_entries() {
    let mut link = LinkConfig::perfect();
    link.duplicate_probability = 0.5;
    let mut e = build_with_network(42, link, 500);
    e.run().unwrap();
    let log = e.into_log();
    assert!(
        log.records()
            .iter()
            .any(|r| matches!(r.kind, LogEventKind::MessageDuplicated { .. })),
        "expected at least one duplicated message"
    );
}

#[test]
fn healing_partition_restores_cross_group_traffic() {
    let mut link = LinkConfig::perfect();
    link.base_latency = Duration::from_millis(1);
    let mut net = FullNetwork::new(42, link);
    let a = NodeId(1);
    let b = NodeId(3);
    net.partition(a, b);
    assert!(net
        .deliveries(VirtualTime::ZERO, a, b, &Message::Ping)
        .is_empty());
    net.heal(a, b);
    assert!(!net
        .deliveries(VirtualTime::ZERO, a, b, &Message::Ping)
        .is_empty());
}