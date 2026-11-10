//! JSONL roundtrip.

use dsfs_core::engine::{new_in_memory_engine, EngineConfig};
use dsfs_core::log::{EventLog, InMemoryLog, JsonlLog, LogMeta, LOG_SCHEMA_VERSION, SIM_VERSION};
use dsfs_core::node::{NodeId, PingNode};
use dsfs_core::time::Duration;

#[test]
fn jsonl_roundtrip_preserves_records() {
    let mut e = new_in_memory_engine(EngineConfig {
        master_seed: 42,
        max_events: 200,
    });
    e.add_ping_node(PingNode::new(NodeId(1), Duration::from_millis(100)))
        .unwrap();
    e.add_ping_node(PingNode::new(NodeId(2), Duration::from_millis(100)))
        .unwrap();
    e.run().unwrap();

    let log = e.into_log();
    let mut mem = InMemoryLog::new();
    for r in log.records() {
        mem.record(r.clone()).unwrap();
    }

    let mut buf = Vec::new();
    {
        let meta = LogMeta {
            schema: LOG_SCHEMA_VERSION,
            sim_version: SIM_VERSION.to_string(),
            scenario: "roundtrip".to_string(),
            seed: 42,
        };
        let mut w = JsonlLog::new(&mut buf, meta).unwrap();
        for r in mem.records() {
            w.record(r.clone()).unwrap();
        }
        w.flush().unwrap();
    }

    let text = std::str::from_utf8(&buf).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert!(lines.len() >= 2);
    assert!(lines[0].contains("\"meta\""));
    let mut count = 0;
    for line in &lines[1..] {
        let _r: dsfs_core::log::LogRecord = serde_json::from_str(line).unwrap();
        count += 1;
    }
    assert_eq!(count, mem.records().len());
}
