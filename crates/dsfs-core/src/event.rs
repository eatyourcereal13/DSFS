//! Event identifiers, priorities and keys.
//!
//! See `docs/design-decisions.md` DD-002 for the rationale behind
//! `EventKey = (time, priority, seq)`.

use crate::time::VirtualTime;

/// Monotonic identifier assigned by the [`crate::scheduler::EventQueue`]
/// on every successful registration. Used for traceability and for
/// deterministic ordering when `(time, priority)` ties.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct EventSeq(pub u64);

/// Semantic class of an event. Lower value = processed earlier when
/// `time` is equal.
///
/// This ordering is part of the simulation contract. Changing it
/// changes observable behaviour of the simulation and requires a
/// `SIM_VERSION` bump (see `docs/design-decisions.md` DD-002).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[repr(u8)]
pub enum Priority {
    /// Timer expiry (election timeout, heartbeat).
    Timer = 0,
    /// Delivery of a network message.
    Network = 1,
    /// Client request entering the system.
    Client = 2,
    /// Engine-internal events (metrics, random replies, etc.).
    System = 3,
}

impl Priority {
    /// Returns the numeric value of the priority.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Full sort key for a scheduled event.
///
/// Ordering is lexicographic: `time`, then `priority`, then `seq`.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct EventKey {
    pub time: VirtualTime,
    pub priority: Priority,
    pub seq: EventSeq,
}

impl EventKey {
    /// Convenience constructor.
    #[must_use]
    pub const fn new(time: VirtualTime, priority: Priority, seq: EventSeq) -> Self {
        Self {
            time,
            priority,
            seq,
        }
    }
}

/// Identifier of an event **as it is being processed** by the engine.
///
/// Distinct from [`EventSeq`]: `EventSeq` is assigned on registration,
/// `EventId` on actual delivery to the engine. A scheduled event that
/// is cancelled before firing never receives an `EventId`. This is why
/// causal links in [`crate::log::LogRecord`] use `EventId`, not `EventSeq`.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct EventId(pub u64);

impl std::fmt::Display for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ev{}", self.0)
    }
}

/// Payload carried inside a [`crate::scheduler::QueuedEvent`].
///
/// At Stage 2 this is deliberately minimal: just enough to exercise the
/// engine loop with 5 nodes exchanging `Ping`/`Pong` messages. It will grow
/// with Raft, network, and fault-injection variants in later stages.
///
/// The `caused_by` field, when `Some`, points at the `EventId` whose
/// processing produced this event. This is the backbone of the causal
/// DAG built by the offline checker (DD-004).
#[derive(Clone, Debug)]
pub struct EventPayload {
    pub kind: EventKind,
    pub caused_by: Option<EventId>,
}

impl EventPayload {
    /// Convenience constructor.
    #[must_use]
    pub fn new(kind: EventKind, caused_by: Option<EventId>) -> Self {
        Self { kind, caused_by }
    }

    /// Constructor for an event with no causal parent.
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
    /// A timer expired on `node`.
    TimerFired {
        node: crate::node::NodeId,
        timer_id: crate::node::TimerId,
    },
    /// The network delivered `msg` to `to` from `from`.
    MessageDelivered {
        to: crate::node::NodeId,
        from: crate::node::NodeId,
        msg: crate::message::Message,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_order_is_timer_network_client_system() {
        assert!(Priority::Timer < Priority::Network);
        assert!(Priority::Network < Priority::Client);
        assert!(Priority::Client < Priority::System);
    }

    #[test]
    fn key_orders_by_time_first() {
        let a = EventKey::new(VirtualTime(1), Priority::System, EventSeq(0));
        let b = EventKey::new(VirtualTime(2), Priority::Timer, EventSeq(0));
        assert!(a < b, "time must dominate priority");
    }

    #[test]
    fn key_orders_by_priority_when_time_equal() {
        let a = EventKey::new(VirtualTime(1), Priority::Timer, EventSeq(100));
        let b = EventKey::new(VirtualTime(1), Priority::Network, EventSeq(0));
        assert!(a < b, "priority must dominate seq");
    }

    #[test]
    fn key_orders_by_seq_when_time_and_priority_equal() {
        let a = EventKey::new(VirtualTime(1), Priority::Network, EventSeq(1));
        let b = EventKey::new(VirtualTime(1), Priority::Network, EventSeq(2));
        assert!(a < b);
    }
}
