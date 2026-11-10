//! In-memory and JSONL event logs.

use std::io::{self, Write};

use serde::{Deserialize, Serialize};

use crate::event::EventId;
use crate::node::{Availability, NodeId, TimerId};
use crate::time::VirtualTime;

/// Schema version of the JSONL format.
pub const LOG_SCHEMA_VERSION: u32 = 1;

/// Simulation version, embedded in JSONL metadata.
pub const SIM_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogEventKind {
    MessageSent {
        from: NodeId,
        to: NodeId,
    },
    MessageDelivered {
        from: NodeId,
        to: NodeId,
    },
    MessageDropped {
        from: NodeId,
        to: NodeId,
    },
    MessageDuplicated {
        from: NodeId,
        to: NodeId,
    },
    TimerFired {
        node: NodeId,
        timer_id: TimerId,
    },
    PingReceived {
        node: NodeId,
        from: NodeId,
    },
    PongReceived {
        node: NodeId,
        from: NodeId,
    },
    NodeAvailabilityChanged {
        node: NodeId,
        to: Availability,
    },
    NodeSlowFactorChanged {
        node: NodeId,
        slow_factor_x1000: u32,
    },
    NodeIgnoredWhileOffline {
        node: NodeId,
        event_id: EventId,
    },
    MessageDelayedBySlowNode {
        from: NodeId,
        to: NodeId,
    },

    RaftBecameFollower {
        node: NodeId,
        term: u64,
        reason: String,
    },
    RaftBecameCandidate {
        node: NodeId,
        term: u64,
    },
    RaftBecameLeader {
        node: NodeId,
        term: u64,
    },
    RaftVotedFor {
        node: NodeId,
        candidate: u32,
        term: u64,
    },
    RaftReceivedVote {
        node: NodeId,
        from: u32,
        term: u64,
        granted: bool,
    },
    RaftTermBumped {
        node: NodeId,
        from: u64,
        to: u64,
    },

    RaftAppended {
        node: NodeId,
        index: u64,
        term: u64,
    },
    RaftEntryAppended {
        node: NodeId,
        index: u64,
        term: u64,
    },
    RaftApplied {
        node: NodeId,
        index: u64,
        command: Vec<u8>,
    },
    RaftLogTruncated {
        node: NodeId,
        from_index: u64,
    },
    RaftCommitAdvanced {
        node: NodeId,
        from: u64,
        to: u64,
    },
    RaftClientCommandRejected {
        node: NodeId,
        reason: String,
    },

    FaultApplied {
        index: usize,
        description: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRecord {
    pub event_id: EventId,
    pub time: VirtualTime,
    pub kind: LogEventKind,
    pub caused_by: Option<EventId>,
}

/// Sink for log records.
pub trait EventLog {
    fn record(&mut self, record: LogRecord) -> io::Result<()>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// In-memory log.
#[derive(Debug, Default)]
pub struct InMemoryLog {
    records: Vec<LogRecord>,
}

impl InMemoryLog {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    #[must_use]
    pub fn records(&self) -> &[LogRecord] {
        &self.records
    }
    #[must_use]
    pub fn into_records(self) -> Vec<LogRecord> {
        self.records
    }
}

impl EventLog for InMemoryLog {
    fn record(&mut self, record: LogRecord) -> io::Result<()> {
        self.records.push(record);
        Ok(())
    }
    fn len(&self) -> usize {
        self.records.len()
    }
}

/// Metadata written at the top of a JSONL file.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogMeta {
    /// Schema version of the log file.
    pub schema: u32,
    /// Simulation version (from Cargo.toml at build time).
    pub sim_version: String,
    /// Scenario name (informational).
    pub scenario: String,
    /// Master seed used for this run.
    pub seed: u64,
}

#[derive(Serialize, Deserialize)]
struct MetaWrapper {
    meta: LogMeta,
}

/// JSONL writer: one JSON object per line, prefixed by a meta line.
pub struct JsonlLog<W: Write> {
    writer: W,
    len: usize,
}

impl<W: Write> JsonlLog<W> {
    /// Creates a new JSONL writer and writes the meta line.
    ///
    /// # Errors
    ///
    /// Propagates I/O errors from writing the header.
    pub fn new(mut writer: W, meta: LogMeta) -> io::Result<Self> {
        let wrapper = MetaWrapper { meta };
        serde_json::to_writer(&mut writer, &wrapper).map_err(io::Error::other)?;
        writer.write_all(b"\n")?;
        Ok(Self { writer, len: 0 })
    }

    /// Returns the number of records written (excluding the meta line).
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Flushes the underlying writer.
    ///
    /// # Errors
    ///
    /// Propagates I/O errors.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    /// Consumes the log and returns the underlying writer.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W: Write> EventLog for JsonlLog<W> {
    fn record(&mut self, record: LogRecord) -> io::Result<()> {
        serde_json::to_writer(&mut self.writer, &record).map_err(io::Error::other)?;
        self.writer.write_all(b"\n")?;
        self.len += 1;
        Ok(())
    }

    fn len(&self) -> usize {
        self.len
    }
}
