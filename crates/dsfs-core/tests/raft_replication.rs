//! Integration tests for Raft log replication.

use std::time::Duration as StdDuration;

use dsfs_core::engine::{new_in_memory_engine, EngineConfig};
use dsfs_core::log::{InMemoryLog, LogEventKind};
use dsfs_core::network::NullNetwork;
use dsfs_core::node::{Availability, NodeId, NodeImpl, RaftAdapter};
use dsfs_core::rng::{RaftStreamPurpose, StreamId, StreamRng};
use dsfs_raft::{PeerId, RaftConfig, RaftNode};

fn build_cluster(
    n: u32,
    seed: u64,
) -> dsfs_core::engine::SimulationEngine<InMemoryLog, NullNetwork> {
    let mut e = new_in_memory_engine(EngineConfig {
        master_seed: seed,
        max_events: u64::MAX,
    });
    let peers: Vec<PeerId> = (1..=n).map(PeerId).collect();
    let cfg = RaftConfig {
        election_timeout: StdDuration::from_millis(100),
        heartbeat_interval: StdDuration::from_millis(20),
    };
    for &p in &peers {
        let rng = StreamRng::new(
            seed,
            StreamId::raft(p.0, RaftStreamPurpose::ElectionTimeout),
        );
        let raft = RaftNode::new(p, peers.clone(), cfg.clone(), Box::new(rng));
        let adapter = RaftAdapter::new(NodeId(p.0), raft);
        e.add_node(NodeImpl::Raft(Box::new(adapter))).unwrap();
    }
    e
}

#[test]
fn single_node_commits_client_command() {
    let mut e = build_cluster(1, 42);
    e.run_until(500).unwrap(); // let election happen
    assert!(e.find_leader().is_some(), "single node must be leader");
    e.submit_command(b"hello".to_vec()).unwrap();
    e.run_until(500).unwrap(); // let commit happen
    let applied = e.raft_applied(NodeId(1)).unwrap();
    assert_eq!(applied, &[b"hello".to_vec()]);
}

#[test]
fn three_node_cluster_replicates_commit() {
    let mut e = build_cluster(3, 42);
    e.run_until(5_000).unwrap(); // let election settle
    let leader = e.find_leader().expect("leader must exist");
    e.submit_command(b"alpha".to_vec()).unwrap();
    e.run_until(5_000).unwrap(); // replication + commit

    let mut count_applied = 0;
    for id in 1..=3 {
        if let Some(applied) = e.raft_applied(NodeId(id)) {
            if applied.contains(&b"alpha".to_vec()) {
                count_applied += 1;
            }
        }
    }
    assert!(
        count_applied >= 2,
        "majority must apply; leader={:?}, applied_count={}",
        leader,
        count_applied
    );
}

#[test]
fn commands_applied_in_order() {
    let mut e = build_cluster(3, 42);
    e.run_until(5_000).unwrap();
    for c in [b"one".to_vec(), b"two".to_vec(), b"three".to_vec()] {
        e.submit_command(c).unwrap();
        // Give each command time to be appended + one round of AppendEntries
        // before submitting the next, so the order is unambiguous.
        e.run_until(500).unwrap();
    }
    e.run_until(5_000).unwrap();

    let mut ok = false;
    for id in 1..=3 {
        if let Some(applied) = e.raft_applied(NodeId(id)) {
            if applied == [b"one".to_vec(), b"two".to_vec(), b"three".to_vec()] {
                ok = true;
                break;
            }
        }
    }
    assert!(ok, "expected ordered applied list on some node");
}

#[test]
fn no_two_leaders_per_term_with_replication() {
    for seed in 1..=10 {
        let mut e = build_cluster(5, seed);
        e.run_until(10_000).unwrap();
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

#[test]
fn commit_requires_majority() {
    let mut e = build_cluster(5, 42);
    e.set_availability(NodeId(4), Availability::Offline)
        .unwrap();
    e.set_availability(NodeId(5), Availability::Offline)
        .unwrap();
    e.run_until(10_000).unwrap();
    let _ = e.find_leader();
    e.submit_command(b"majority_test".to_vec()).unwrap();
    e.run_until(10_000).unwrap();

    let mut applied = 0;
    for id in 1..=3 {
        if let Some(a) = e.raft_applied(NodeId(id)) {
            if a.contains(&b"majority_test".to_vec()) {
                applied += 1;
            }
        }
    }
    assert!(applied >= 2, "majority must apply, got {}", applied);
}
