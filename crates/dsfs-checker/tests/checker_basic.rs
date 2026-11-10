//! Basic checker tests.

use dsfs_checker::invariants::{check_single_leader_per_term, run};
use dsfs_checker::read_jsonl;
use dsfs_core::event::EventId;
use dsfs_core::log::{LogEventKind, LogRecord};
use dsfs_core::node::NodeId;
use dsfs_core::time::VirtualTime;

fn rec(id: u64, t: u64, kind: LogEventKind) -> LogRecord {
    LogRecord {
        event_id: EventId(id),
        time: VirtualTime(t),
        kind,
        caused_by: None,
    }
}

#[test]
fn empty_log_is_ok() {
    let report = run(&[]);
    assert!(report.is_ok());
    assert_eq!(report.stats.total_records, 0);
}

#[test]
fn single_leader_per_term_ok() {
    let records = vec![
        rec(
            1,
            100,
            LogEventKind::RaftBecameLeader {
                node: NodeId(1),
                term: 1,
            },
        ),
        rec(
            2,
            200,
            LogEventKind::RaftBecameLeader {
                node: NodeId(2),
                term: 2,
            },
        ),
    ];
    let v = check_single_leader_per_term(&records);
    assert!(v.is_empty());
}

#[test]
fn two_leaders_in_same_term_is_violation() {
    let records = vec![
        rec(
            1,
            100,
            LogEventKind::RaftBecameLeader {
                node: NodeId(1),
                term: 1,
            },
        ),
        rec(
            2,
            200,
            LogEventKind::RaftBecameLeader {
                node: NodeId(2),
                term: 1,
            },
        ),
    ];
    let v = check_single_leader_per_term(&records);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].invariant, "single_leader_per_term");
}

#[test]
fn read_jsonl_skips_meta_and_parses_records() {
    let jsonl = r#"{"meta":{"schema":1,"sim_version":"0.1.0","scenario":"test","seed":42}}
{"event_id":1,"time":100,"kind":{"RaftBecameLeader":{"node":1,"term":1}},"caused_by":null}
"#;
    let out = read_jsonl(jsonl.as_bytes()).unwrap();
    assert!(out.meta.is_some());
    assert_eq!(out.records.len(), 1);
    assert_eq!(out.meta.unwrap().seed, 42);
}

#[test]
fn stats_are_computed() {
    let records = vec![
        rec(
            1,
            100,
            LogEventKind::MessageSent {
                from: NodeId(1),
                to: NodeId(2),
            },
        ),
        rec(
            2,
            200,
            LogEventKind::MessageDropped {
                from: NodeId(1),
                to: NodeId(2),
            },
        ),
        rec(
            3,
            300,
            LogEventKind::RaftBecameLeader {
                node: NodeId(1),
                term: 1,
            },
        ),
        rec(
            4,
            400,
            LogEventKind::RaftApplied {
                node: NodeId(1),
                index: 1,
                command: vec![1],
            },
        ),
    ];
    let report = run(&records);
    assert_eq!(report.stats.messages_sent, 1);
    assert_eq!(report.stats.messages_dropped, 1);
    assert_eq!(report.stats.leaders_elected, 1);
    assert_eq!(report.stats.applied_commands, 1);
}
