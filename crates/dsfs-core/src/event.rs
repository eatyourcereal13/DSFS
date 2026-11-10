//! Event identifiers, priorities and keys.

use crate::time::VirtualTime;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct EventSeq(pub u64);

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[repr(u8)]
pub enum Priority {
    Timer = 0,
    Network = 1,
    Client = 2,
    System = 3,
}

impl Priority {
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct EventKey {
    pub time: VirtualTime,
    pub priority: Priority,
    pub seq: EventSeq,
}

impl EventKey {
    #[must_use]
    pub const fn new(time: VirtualTime, priority: Priority, seq: EventSeq) -> Self {
        Self {
            time,
            priority,
            seq,
        }
    }
}

/// Identifier of an event as it is being processed.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct EventId(pub u64);

impl std::fmt::Display for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ev{}", self.0)
    }
}

#[derive(Clone, Debug)]
pub struct EventPayload {
    pub kind: EventKind,
    pub caused_by: Option<EventId>,
}

impl EventPayload {
    #[must_use]
    pub fn new(kind: EventKind, caused_by: Option<EventId>) -> Self {
        Self { kind, caused_by }
    }

    #[must_use]
    pub fn root(kind: EventKind) -> Self {
        Self {
            kind,
            caused_by: None,
        }
    }
}

/// The concrete kind of event delivered to the engine.
#[derive(Clone, Debug)]
pub enum EventKind {
    TimerFired {
        node: crate::node::NodeId,
        timer_id: crate::node::TimerId,
    },
    MessageDelivered {
        to: crate::node::NodeId,
        from: crate::node::NodeId,
        msg: crate::message::Message,
    },
    /// A client command destined for the current leader's log.
    ClientCommand {
        to: crate::node::NodeId,
        command: Vec<u8>,
    },
}
