//! Property tests over randomly generated scenarios.

use std::time::Duration as StdDuration;

use dsfs_core::engine::{EngineConfig, FullNetwork, LinkConfig, SimulationEngine};
use dsfs_core::fault_injector::FaultInjector;
use dsfs_core::log::{InMemoryLog, LogRecord};
use dsfs_core::node::{NodeId, NodeImpl, RaftAdapter};
use dsfs_core::rng::{RaftStreamPurpose, StreamId, StreamRng};
use dsfs_core::scenario::{
    ClusterConfig, FaultAction, FaultEvent, LinkConfigYaml, NetworkConfig, RaftConfigYaml, Scenario,
};
use dsfs_core::time::Duration as VDuration;
use dsfs_raft::{PeerId, RaftConfig, RaftNode};

use dsfs_checker::invariants::run as run_checker;

/// Generates a deterministic pseudo-random scenario for a given seed.
fn random_scenario(seed: u64) -> Scenario {
    let mut rng = StreamRng::new(seed, StreamId(9999));
    let nodes = 5u32;
    let raft = RaftConfigYaml {
        election_timeout_ms: 150,
        heartbeat_interval_ms: 30,
    };
    let link = LinkConfigYaml {
        base_latency_ms: 5,
        jitter_ms: 2,
        loss_probability: rng.next_f64() * 0.05,
        duplicate_probability: rng.next_f64() * 0.02,
    };

    let mut events = vec![FaultEvent {
        at_ms: None,
        action: FaultAction::WaitForLeader {},
    }];

    // 3..=8 random faults between t=500 and t=3000ms.
    let n_faults = 3 + (rng.gen_range_u64(0, 6) as usize);
    for _ in 0..n_faults {
        let at = 500 + rng.gen_range_u64(0, 2500);
        let which = rng.gen_range_u64(0, 5);
        let action = match which {
            0 => {
                let node = 1 + (rng.gen_range_u64(0, nodes as u64) as u32);
                FaultAction::CrashNode { node }
            }
            1 => {
                let node = 1 + (rng.gen_range_u64(0, nodes as u64) as u32);
                FaultAction::RestartNode { node }
            }
            2 => FaultAction::SubmitWrite {
                key: format!("k{}", rng.gen_range_u64(0, 100)),
                value: format!("v{}", rng.gen_range_u64(0, 100)),
            },
            3 => {
                let node = 1 + (rng.gen_range_u64(0, nodes as u64) as u32);
                let factor = 2.0 + rng.gen_range_u64(0, 20) as f64;
                FaultAction::SetSlow { node, factor }
            }
            _ => {
                let node = 1 + (rng.gen_range_u64(0, nodes as u64) as u32);
                FaultAction::ClearSlow { node }
            }
        };
        events.push(FaultEvent {
            at_ms: Some(at),
            action,
        });
    }

    Scenario {
        version: 1,
        name: format!("random_{seed}"),
        seed,
        cluster: ClusterConfig { nodes, raft },
        network: NetworkConfig { default: link },
        events,
    }
}

fn build_engine(scenario: &Scenario) -> SimulationEngine<InMemoryLog, FullNetwork> {
    let mut link = LinkConfig::perfect();
    link.base_latency = VDuration::from_millis(scenario.network.default.base_latency_ms);
    link.jitter = VDuration::from_millis(scenario.network.default.jitter_ms);
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
        e.add_node(NodeImpl::Raft(Box::new(RaftAdapter::new(
            NodeId(p.0),
            raft,
        ))))
        .unwrap();
    }
    e.with_fault_injector(FaultInjector::from_scenario(scenario))
}

fn run_once(seed: u64) -> Vec<LogRecord> {
    let scenario = random_scenario(seed);
    let mut engine = build_engine(&scenario);
    engine.run_until(50_000).unwrap();
    engine.into_log().into_records()
}

#[test]
fn determinism_across_seeds() {
    for seed in 1..=30 {
        let a = run_once(seed);
        let b = run_once(seed);
        assert_eq!(
            a.len(),
            b.len(),
            "seed {seed}: record counts differ between runs"
        );
        for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
            assert_eq!(
                (x.event_id, x.time, &x.kind, x.caused_by),
                (y.event_id, y.time, &y.kind, y.caused_by),
                "seed {seed}: divergence at index {i}"
            );
        }
    }
}

#[test]
fn checker_never_reports_violations() {
    for seed in 1..=30 {
        let records = run_once(seed);
        let report = run_checker(&records);
        if !report.is_ok() {
            eprintln!("seed {seed}: {} violation(s)", report.violations.len());
            for v in &report.violations {
                eprintln!("  [{}] t={:?} {}", v.invariant, v.time, v.context);
            }
            panic!("checker detected a violation on seed {seed}");
        }
    }
}

#[test]
fn at_least_one_leader_is_elected_per_run() {
    for seed in 1..=30 {
        let records = run_once(seed);
        let leaders: usize = records
            .iter()
            .filter(|r| {
                matches!(
                    r.kind,
                    dsfs_core::log::LogEventKind::RaftBecameLeader { .. }
                )
            })
            .count();
        assert!(leaders >= 1, "seed {seed}: no leader was elected");
    }
}
