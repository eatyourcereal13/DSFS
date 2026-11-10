//! Integration tests for Raft leader election through the engine.

use dsfs_core::engine::{new_in_memory_engine, EngineConfig, RunOutcome};
use dsfs_core::log::LogEventKind;
use dsfs_core::network::NullNetwork;
use dsfs_core::node::{Availability, NodeId, NodeImpl, RaftAdapter};
use dsfs_core::rng::{RaftStreamPurpose, StreamId, StreamRng};
use dsfs_raft::{PeerId, RaftConfig, RaftNode};

// `RaftConfig` uses `std::time::Duration` (dsfs-raft is independent of
// dsfs-core), so we import it explicitly. `dsfs_core::time::Duration` is
// not needed in this file — no PingNode is used here.
use std::time::Duration;

fn build_raft_cluster(
    n_nodes: u32,
    max_events: u64,
    seed: u64,
) -> dsfs_core::engine::SimulationEngine<dsfs_core::log::InMemoryLog, NullNetwork> {
    let mut e = new_in_memory_engine(EngineConfig {
        master_seed: seed,
        max_events,
    });
    let peers: Vec<PeerId> = (1..=n_nodes).map(PeerId).collect();
    let cfg = RaftConfig {
        election_timeout: Duration::from_millis(100),
        heartbeat_interval: Duration::from_millis(20),
    };
    for &p in &peers {
        let rng = StreamRng::new(
            seed,
            StreamId::raft(p.0, RaftStreamPurpose::ElectionTimeout),
        );
        let raft = RaftNode::new(p, peers.clone(), cfg.clone(), Box::new(rng));
        let adapter = RaftAdapter::new(NodeId(p.0), raft);
        e.add_node(NodeImpl::Raft(adapter)).unwrap();
    }
    e
}

fn count_leaders(log: &dsfs_core::log::InMemoryLog) -> usize {
    log.records()
        .iter()
        .filter(|r| matches!(r.kind, LogEventKind::RaftBecameLeader { .. }))
        .count()
}

#[test]
fn cluster_of_three_elects_a_leader() {
    let mut e = build_raft_cluster(3, 2_000, 42);
    let outcome = e.run().unwrap();
    assert!(matches!(outcome, RunOutcome::HitEventLimit { .. }));
    let log = e.into_log();
    assert!(
        count_leaders(&log) >= 1,
        "expected at least one leader elected"
    );
}

#[test]
fn cluster_of_five_elects_a_leader() {
    let mut e = build_raft_cluster(5, 5_000, 7);
    e.run().unwrap();
    let log = e.into_log();
    assert!(count_leaders(&log) >= 1);
}

#[test]
fn single_node_cluster_becomes_leader_immediately() {
    let mut e = build_raft_cluster(1, 100, 42);
    e.run().unwrap();
    let log = e.into_log();
    assert!(
        count_leaders(&log) >= 1,
        "single-node cluster must elect itself"
    );
}

#[test]
fn offline_leader_triggers_reelection() {
    // Build a 5-node cluster with node 1 offline before running. A leader
    // must still emerge among nodes 2..5.
    let mut e = build_raft_cluster(5, 5_000, 42);
    e.set_availability(NodeId(1), Availability::Offline)
        .unwrap();
    e.run().unwrap();
    let log = e.into_log();
    let leaders: Vec<u32> = log
        .records()
        .iter()
        .filter_map(|r| match &r.kind {
            LogEventKind::RaftBecameLeader { node, .. } => Some(node.0),
            _ => None,
        })
        .collect();
    assert!(
        leaders.iter().any(|&l| l != 1),
        "a non-offline node must become leader, got {:?}",
        leaders
    );
}

#[test]
fn determinism_with_same_seed() {
    fn run_once(seed: u64) -> Vec<String> {
        let mut e = build_raft_cluster(5, 1_000, seed);
        e.run().unwrap();
        let log = e.into_log();
        log.records()
            .iter()
            .map(|r| format!("{} {} {:?}", r.event_id.0, r.time.0, r.kind))
            .collect()
    }
    assert_eq!(run_once(123), run_once(123));
}

#[test]
fn no_two_leaders_in_same_term() {
    // Run with several seeds; check the log never contains two
    // BecameLeader events with the same term.
    for seed in 1..=20 {
        let mut e = build_raft_cluster(5, 2_000, seed);
        e.run().unwrap();
        let log = e.into_log();
        let mut leaders_per_term: std::collections::BTreeMap<u64, Vec<u32>> =
            std::collections::BTreeMap::new();
        for r in log.records() {
            if let LogEventKind::RaftBecameLeader { node, term } = &r.kind {
                leaders_per_term.entry(*term).or_default().push(node.0);
            }
        }
        for (term, nodes) in leaders_per_term {
            assert!(
                nodes.len() <= 1,
                "seed={seed}: two leaders in term {term}: {:?}",
                nodes
            );
        }
    }
}
