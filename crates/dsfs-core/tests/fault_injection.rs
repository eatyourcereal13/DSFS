//! Integration tests for scenario-driven fault injection.

use std::time::Duration as StdDuration;

use dsfs_core::engine::{EngineConfig, FullNetwork, LinkConfig, RunOutcome, SimulationEngine};
use dsfs_core::fault_injector::FaultInjector;
use dsfs_core::log::{InMemoryLog, LogEventKind};
use dsfs_core::node::{Availability, NodeId, NodeImpl, RaftAdapter};
use dsfs_core::rng::{RaftStreamPurpose, StreamId, StreamRng};
use dsfs_core::scenario::Scenario;
use dsfs_core::time::Duration;
use dsfs_raft::{PeerId, RaftConfig, RaftNode};

fn build_engine_from_scenario(scenario: &Scenario) -> SimulationEngine<InMemoryLog, FullNetwork> {
    let mut link = LinkConfig::perfect();
    link.base_latency = Duration::from_millis(scenario.network.default.base_latency_ms);
    link.jitter = Duration::from_millis(scenario.network.default.jitter_ms);
    link.loss_probability = scenario.network.default.loss_probability;
    link.duplicate_probability = scenario.network.default.duplicate_probability;
    let net = FullNetwork::new(scenario.seed, link);

    let mut e = SimulationEngine::new(
        EngineConfig {
            master_seed: scenario.seed,
            max_events: u64::MAX,
        },
        net,
        InMemoryLog::new(),
    );

    let n = scenario.cluster.nodes;
    let peers: Vec<PeerId> = (1..=n).map(PeerId).collect();
    let cfg = RaftConfig {
        election_timeout: StdDuration::from_millis(scenario.cluster.raft.election_timeout_ms),
        heartbeat_interval: StdDuration::from_millis(scenario.cluster.raft.heartbeat_interval_ms),
    };
    for &p in &peers {
        let rng = StreamRng::new(
            scenario.seed,
            StreamId::raft(p.0, RaftStreamPurpose::ElectionTimeout),
        );
        let raft = RaftNode::new(p, peers.clone(), cfg.clone(), Box::new(rng));
        let adapter = RaftAdapter::new(NodeId(p.0), raft);
        e.add_node(NodeImpl::Raft(Box::new(adapter))).unwrap();
    }
    e.with_fault_injector(FaultInjector::from_scenario(scenario))
}

const LEADER_FAILURE: &str = r#"
version: 1
name: leader_failure
seed: 42
cluster:
  nodes: 5
  raft:
    election_timeout_ms: 150
    heartbeat_interval_ms: 30
network:
  default:
    base_latency_ms: 5
    jitter_ms: 2
    loss_probability: 0.0
    duplicate_probability: 0.0
events:
  - wait_for_leader: {}
  - at_ms: 1000
    crash_node: 3
"#;

#[test]
fn leader_failure_scenario_runs() {
    let scenario = Scenario::from_yaml(LEADER_FAILURE).unwrap();
    let mut e = build_engine_from_scenario(&scenario);
    let outcome = e.run_until(20_000).unwrap();
    assert!(matches!(outcome, RunOutcome::HitEventLimit { .. }));

    let log = e.into_log();
    let faults = log
        .records()
        .iter()
        .filter(|r| matches!(r.kind, LogEventKind::FaultApplied { .. }))
        .count();
    assert!(faults >= 2, "expected at least wait + crash, got {faults}");
    // Node 3 should be offline by end.
    // (Log shows NodeAvailabilityChanged; re-fetch via records.)
    let mut node3_offline = false;
    for r in log.records() {
        if let LogEventKind::NodeAvailabilityChanged { node, to } = &r.kind {
            if *node == NodeId(3) && *to == Availability::Offline {
                node3_offline = true;
            }
        }
    }
    assert!(node3_offline, "node 3 must be offline");
}

const PARTITION: &str = r#"
version: 1
name: partition
seed: 42
cluster:
  nodes: 5
  raft:
    election_timeout_ms: 150
    heartbeat_interval_ms: 30
network:
  default:
    base_latency_ms: 5
    jitter_ms: 2
    loss_probability: 0.0
    duplicate_probability: 0.0
events:
  - wait_for_leader: {}
  - at_ms: 1000
    partition:
      groups: [[1, 2], [3, 4, 5]]
  - at_ms: 5000
    heal:
      groups: [[1, 2], [3, 4, 5]]
"#;

#[test]
fn partition_and_heal_scenario_runs() {
    let scenario = Scenario::from_yaml(PARTITION).unwrap();
    let mut e = build_engine_from_scenario(&scenario);
    e.run_until(50_000).unwrap();

    let log = e.into_log();
    let faults: Vec<_> = log
        .records()
        .iter()
        .filter(|r| matches!(r.kind, LogEventKind::FaultApplied { .. }))
        .collect();
    assert!(faults.len() >= 3, "wait + partition + heal");
}

const WRITE: &str = r#"
version: 1
name: submit_write
seed: 42
cluster:
  nodes: 3
  raft:
    election_timeout_ms: 150
    heartbeat_interval_ms: 30
network:
  default:
    base_latency_ms: 5
    jitter_ms: 2
    loss_probability: 0.0
    duplicate_probability: 0.0
events:
  - wait_for_leader: {}
  - at_ms: 500
    submit_write:
      key: balance
      value: "500"
"#;

#[test]
fn submit_write_is_committed() {
    let scenario = Scenario::from_yaml(WRITE).unwrap();
    let mut e = build_engine_from_scenario(&scenario);
    e.run_until(50_000).unwrap();

    let mut count = 0;
    for id in 1..=3 {
        if let Some(applied) = e.raft_applied(NodeId(id)) {
            if applied.iter().any(|c| c.starts_with(b"put:")) {
                count += 1;
            }
        }
    }
    assert!(count >= 2, "majority must apply the write, got {count}");
}

#[test]
fn determinism_under_faults() {
    fn run_once() -> Vec<String> {
        let scenario = Scenario::from_yaml(LEADER_FAILURE).unwrap();
        let mut e = build_engine_from_scenario(&scenario);
        e.run_until(10_000).unwrap();
        let log = e.into_log();
        log.records()
            .iter()
            .map(|r| format!("{} {} {:?}", r.event_id.0, r.time.0, r.kind))
            .collect()
    }
    assert_eq!(run_once(), run_once());
}
