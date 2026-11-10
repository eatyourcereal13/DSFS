//! Integration tests for node availability and slow factors.

use dsfs_core::engine::{new_in_memory_engine, EngineConfig, RunOutcome};
use dsfs_core::log::{InMemoryLog, LogEventKind};
use dsfs_core::network::NullNetwork;
use dsfs_core::node::{Availability, NodeId, PingNode};
use dsfs_core::time::Duration;

fn cfg(seed: u64, max_events: u64) -> EngineConfig {
    EngineConfig {
        master_seed: seed,
        max_events,
    }
}

#[test]
fn offline_node_does_not_send_pings() {
    let mut e = new_in_memory_engine(cfg(42, 500));
    for id in 1..=5 {
        e.add_ping_node(PingNode::new(NodeId(id), Duration::from_millis(100)))
            .unwrap();
    }
    e.set_availability(NodeId(3), Availability::Offline)
        .unwrap();
    e.run().unwrap();
    let log = e.into_log();
    let sent_by_3 = log
        .records()
        .iter()
        .filter(|r| matches!(r.kind, LogEventKind::MessageSent { from, .. } if from == NodeId(3)))
        .count();
    assert_eq!(sent_by_3, 0, "offline node must not send messages");
}

#[test]
fn offline_node_is_not_pinged_by_broadcast() {
    let mut e = new_in_memory_engine(cfg(42, 500));
    for id in 1..=5 {
        e.add_ping_node(PingNode::new(NodeId(id), Duration::from_millis(100)))
            .unwrap();
    }
    e.set_availability(NodeId(3), Availability::Offline)
        .unwrap();
    e.run().unwrap();
    let log = e.into_log();
    let to_3 = log
        .records()
        .iter()
        .filter(|r| matches!(r.kind, LogEventKind::MessageSent { to, .. } if to == NodeId(3)))
        .count();
    assert_eq!(to_3, 0, "offline node must not be targeted by broadcast");
}

#[test]
fn setting_offline_midflight_does_not_crash() {
    // Offline set before run; verify no error and log records NodeAvailabilityChanged.
    let mut e = new_in_memory_engine(cfg(42, 200));
    e.add_ping_node(PingNode::new(NodeId(1), Duration::from_millis(100)))
        .unwrap();
    e.set_availability(NodeId(1), Availability::Offline)
        .unwrap();
    let outcome = e.run().unwrap();
    // With node offline, no events are produced; the queue drains.
    assert!(matches!(outcome, RunOutcome::Completed { .. }));
    let log = e.into_log();
    assert!(log
        .records()
        .iter()
        .any(|r| matches!(r.kind, LogEventKind::NodeAvailabilityChanged { .. })));
}

#[test]
fn slow_factor_delays_incoming_messages() {
    // Compare the time of the first PongReceived at node 2 with and without
    // node 2 being slow.
    fn first_pong_time(slow: bool) -> Option<u64> {
        let mut e = new_in_memory_engine(cfg(42, 500));
        e.add_ping_node(PingNode::new(NodeId(1), Duration::from_millis(100)))
            .unwrap();
        e.add_ping_node(PingNode::new(NodeId(2), Duration::from_millis(100)))
            .unwrap();
        if slow {
            e.set_slow_factor(NodeId(2), 1000.0).unwrap();
        }
        e.run().unwrap();
        let log = e.into_log();
        log.records()
            .iter()
            .find(
                |r| matches!(r.kind, LogEventKind::PongReceived { node, .. } if node == NodeId(2)),
            )
            .map(|r| r.time.0)
    }

    let fast = first_pong_time(false);
    let slow = first_pong_time(true);
    assert!(fast.is_some() && slow.is_some());
    assert!(
        slow.unwrap() > fast.unwrap(),
        "slow node must receive pongs later than fast node"
    );
}

#[test]
fn determinism_with_availability_changes() {
    fn run_once() -> Vec<String> {
        let mut e = new_in_memory_engine(cfg(7, 300));
        for id in 1..=5 {
            e.add_ping_node(PingNode::new(NodeId(id), Duration::from_millis(100)))
                .unwrap();
        }
        e.set_availability(NodeId(2), Availability::Offline)
            .unwrap();
        e.set_slow_factor(NodeId(3), 50.0).unwrap();
        e.run().unwrap();
        let log = e.into_log();
        log.records()
            .iter()
            .map(|r| format!("{} {} {:?}", r.event_id.0, r.time.0, r.kind))
            .collect()
    }
    assert_eq!(run_once(), run_once());
}

#[test]
fn availability_can_be_restored() {
    let mut e = new_in_memory_engine(cfg(42, 500));
    for id in 1..=3 {
        e.add_ping_node(PingNode::new(NodeId(id), Duration::from_millis(100)))
            .unwrap();
    }
    e.set_availability(NodeId(2), Availability::Offline)
        .unwrap();
    assert_eq!(e.availability(NodeId(2)), Some(Availability::Offline));
    e.set_availability(NodeId(2), Availability::Online).unwrap();
    assert_eq!(e.availability(NodeId(2)), Some(Availability::Online));
}

// Silence unused-import warnings if NullNetwork is only used implicitly.
#[allow(dead_code)]
fn _unused(_: NullNetwork, _: InMemoryLog) {}
