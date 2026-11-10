//! In-memory event log.

use std::io;

use crate::event::EventId;
use crate::node::{Availability, NodeId, TimerId};
use crate::time::VirtualTime;

#[derive(Clone, Debug, PartialEq, Eq)]
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
        reason: &'static str,
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
    RaftApplied {
        node: NodeId,
        index: u64,
        command: Vec<u8>,
    },
    RaftLogTruncated {
        node: NodeId,
        from_index: u64,
    },
    RaftLogAppended {
        node: NodeId,
        from_index: u64,
        count: u64,
    },
    RaftCommitAdvanced {
        node: NodeId,
        from: u64,
        to: u64,
    },
    RaftClientCommandRejected {
        node: NodeId,
        reason: &'static str,
    },
    FaultApplied {
        index: usize,
        description: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    pub event_id: EventId,
    pub time: VirtualTime,
    pub kind: LogEventKind,
    pub caused_by: Option<EventId>,
}

pub trait EventLog {
    fn record(&mut self, record: LogRecord) -> io::Result<()>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

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
