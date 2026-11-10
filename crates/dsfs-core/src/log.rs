//! In-memory event log.
//!
//! On Stage 2 we only need `InMemory`. JSONL persistence will be added when
//! the first concrete consumer (CLI or checker) appears. The `EventLog`
//! trait is defined now so that `SimulationEngine` depends on the trait,
//! not on `Vec`, and swapping in a buffered writer later is a one-line
//! change.

use std::io;

use crate::event::EventId;
use crate::node::{NodeId, TimerId};
use crate::time::VirtualTime;

/// Kind of entry recorded in the simulation log.
///
/// Grows in later stages: Raft-specific variants (became leader, appended
/// entry, committed) will be added in Stage 5.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogEventKind {
    /// A node sent a message.
    MessageSent { from: NodeId, to: NodeId },
    /// A message was delivered to a node.
    MessageDelivered { from: NodeId, to: NodeId },
    /// A timer expired on a node.
    TimerFired { node: NodeId, timer_id: TimerId },
    /// A node received a `Ping`.
    PingReceived { node: NodeId, from: NodeId },
    /// A node received a `Pong`.
    PongReceived { node: NodeId, from: NodeId },
}

/// One entry in the simulation log.
///
/// `event_id` is the id of the *engine event* that produced this record.
/// `caused_by` is the engine event that causally led to this one, when
/// applicable. See DD-007 (to be written) for the exact semantics of
/// `caused_by` on message-sent vs. message-delivered records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    pub event_id: EventId,
    pub time: VirtualTime,
    pub kind: LogEventKind,
    pub caused_by: Option<EventId>,
}

/// Sink for log records.
///
/// Object-safe; usable as `Box<dyn EventLog>`. Implementations must be
/// deterministic: given the same sequence of `record` calls, they must
/// produce the same observable output. `InMemory` trivially satisfies this.
pub trait EventLog {
    /// Records a log entry.
    ///
    /// # Errors
    ///
    /// Implementations that write to disk may return an I/O error. The
    /// engine propagates this error up to the caller; an in-memory log
    /// never errors.
    fn record(&mut self, record: LogRecord) -> io::Result<()>;

    /// Returns the number of records written so far.
    fn len(&self) -> usize;

    /// Returns `true` if no records have been written.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// In-memory log, suitable for tests and interactive runs.
#[derive(Debug, Default)]
pub struct InMemoryLog {
    records: Vec<LogRecord>,
}

impl InMemoryLog {
    /// Creates an empty log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a slice over all recorded entries.
    #[must_use]
    pub fn records(&self) -> &[LogRecord] {
        &self.records
    }

    /// Consumes the log and returns its entries.
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
