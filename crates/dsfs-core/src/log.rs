//! In-memory event log.

use std::io;

use crate::event::EventId;
use crate::node::{Availability, NodeId, TimerId};
use crate::time::VirtualTime;

/// Kind of entry recorded in the simulation log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogEventKind {
    /// A node sent a message.
    MessageSent { from: NodeId, to: NodeId },
    /// A message was delivered to a node.
    MessageDelivered { from: NodeId, to: NodeId },
    /// A message was dropped by the network.
    MessageDropped { from: NodeId, to: NodeId },
    /// A message was duplicated by the network.
    MessageDuplicated { from: NodeId, to: NodeId },
    /// A timer expired on a node.
    TimerFired { node: NodeId, timer_id: TimerId },
    /// A node received a `Ping`.
    PingReceived { node: NodeId, from: NodeId },
    /// A node received a `Pong`.
    PongReceived { node: NodeId, from: NodeId },
    /// A node's availability changed.
    NodeAvailabilityChanged { node: NodeId, to: Availability },
    /// A node's slow factor changed.
    NodeSlowFactorChanged {
        node: NodeId,
        slow_factor_x1000: u32,
    },
    /// An event for an offline node was ignored by the engine.
    NodeIgnoredWhileOffline { node: NodeId, event_id: EventId },
    /// A message delivery was delayed because the recipient is slow.
    MessageDelayedBySlowNode { from: NodeId, to: NodeId },
}

/// One entry in the simulation log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    pub event_id: EventId,
    pub time: VirtualTime,
    pub kind: LogEventKind,
    pub caused_by: Option<EventId>,
}

/// Sink for log records.
pub trait EventLog {
    /// Records a log entry.
    ///
    /// # Errors
    ///
    /// Disk-backed implementations may return I/O errors; in-memory ones
    /// never do.
    fn record(&mut self, record: LogRecord) -> io::Result<()>;

    /// Returns the number of records written so far.
    fn len(&self) -> usize;

    /// Returns `true` if no records have been written.
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
