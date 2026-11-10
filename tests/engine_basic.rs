//! Integration tests for the Stage 2 simulation engine.

use dsfs_core::engine::{EngineConfig, NullNetwork, RunOutcome, SimulationEngine};
use dsfs_core::log::{InMemoryLog, LogEventKind};
use dsfs_core::node::{NodeId, PingNode};
use dsfs_core::time::Duration;

fn cfg(seed: u64, max_events: u64) -> EngineConfig {
    EngineConfig {
        master_seed: seed,
        max_events,
    }
}

fn build(seed: u64) -> SimulationEngine<InMemoryLog, NullNetwork> {
    let mut e = SimulationEngine::new(cfg(seed, 500), NullNetwork, InMemoryLog::new());
    for id in 1..=5 {
        e.add_ping_node(PingNode::new(NodeId(id), Duration::from_millis(100)))
            .unwrap();
    }
    e
}

#[test]
fn five_nodes_exchange_messages() {
    let mut e = build(42);
    let outcome = e.run().unwrap();
    assert!(matches!(outcome, RunOutcome::HitEventLimit { .. }));

    let log = e.into_log();
    assert!(
        log.records()
            .iter()
            .any(|r| matches!(r.kind, LogEventKind::MessageSent { .. })),
        "expected at least one sent message"
    );
    assert!(
        log.records()
            .iter()
            .any(|r| matches!(r.kind, LogEventKind::MessageDelivered { .. })),
        "expected at least one delivered message"
    );
    assert!(
        log.records()
            .iter()
            .any(|r| matches!(r.kind, LogEventKind::PongReceived { .. })),
        "expected at least one PongReceived"
    );
}

#[test]
fn determinism_identical_log_across_runs() {
    fn run_once(seed: u64) -> Vec<(u64, u64, String)> {
        let mut e = build(seed);
        e.run().unwrap();
        let log = e.into_log();
        log.records()
            .iter()
            .map(|r| {
                (
                    r.event_id.0,
                    r.time.0,
                    format!("{:?}", r.kind),
                )
            })
            .collect()
    }

    let a = run_once(42);
    let b = run_once(42);
    assert_eq!(a, b, "same seed must yield identical log");
}

#[test]
fn event_times_are_monotonic_in_log() {
    let mut e = build(7);
    e.run().unwrap();
    let log = e.into_log();
    let mut prev = 0u64;
    for r in log.records() {
        assert!(
            r.time.0 >= prev,
            "virtual time must be non-decreasing across the log"
        );
        prev = r.time.0;
    }
}