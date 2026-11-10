//! Virtual node identifiers, availability states, and the `NodeImpl`
//! dispatch enum.
//!
//! The `NodeImpl` enum exists so the engine can hold heterogeneous node
//! kinds (`PingNode` now, `RaftNode` in Stage 5) without `Box<dyn Node>`
//! overhead. See DD-010 in `docs/design-decisions.md`.

use crate::action::Action;
use crate::event::{EventId, EventKind, EventPayload};
use crate::log::LogEventKind;
use crate::message::Message;
use crate::time::{Duration, VirtualTime};

/// Stable identifier of a virtual node within a single simulation.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct NodeId(pub u32);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "n{}", self.0)
    }
}

/// Identifier of a timer local to a node.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct TimerId(pub u64);

/// Availability of a node, orthogonal to its protocol role.
///
/// - `Online`: normal operation.
/// - `Offline`: does not process any event (message or timer). The engine
///   drops deliveries and ignores timer expirations for offline nodes.
/// - `Recovering`: reserved for Stage 7; the engine treats it identically
///   to `Online` for now.
///
/// Role (`Follower` / `Candidate` / `Leader`) is a Raft concept and lives
/// inside `RaftNode` (Stage 5), not here.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub enum Availability {
    #[default]
    Online,
    Offline,
    Recovering,
}

impl Availability {
    /// Returns `true` if the node processes events in this state.
    #[must_use]
    pub fn is_active(self) -> bool {
        matches!(self, Availability::Online | Availability::Recovering)
    }
}

/// Minimal ping node used to exercise the engine.
#[derive(Debug)]
pub struct PingNode {
    pub id: NodeId,
    pub ping_interval: Duration,
    next_timer_id: u64,
    initialised: bool,
}

impl PingNode {
    /// Creates a ping node.
    #[must_use]
    pub fn new(id: NodeId, ping_interval: Duration) -> Self {
        Self {
            id,
            ping_interval,
            next_timer_id: 1,
            initialised: false,
        }
    }

    /// Emits start actions (arm the initial timer) exactly once.
    pub fn on_start(&mut self) -> Vec<Action> {
        if self.initialised {
            return Vec::new();
        }
        self.initialised = true;
        let tid = self.alloc_timer_id();
        vec![Action::ScheduleTimer {
            delay: self.ping_interval,
            timer_id: tid,
        }]
    }

    /// Handles a single event.
    pub fn handle(
        &mut self,
        _now: VirtualTime,
        _event_id: EventId,
        payload: &EventPayload,
    ) -> Vec<Action> {
        let mut actions = Vec::new();
        match &payload.kind {
            EventKind::TimerFired { node, timer_id } => {
                debug_assert_eq!(*node, self.id, "engine dispatched timer to wrong node");
                actions.push(Action::Log(LogEventKind::TimerFired {
                    node: self.id,
                    timer_id: *timer_id,
                }));
                let tid = self.alloc_timer_id();
                actions.push(Action::ScheduleTimer {
                    delay: self.ping_interval,
                    timer_id: tid,
                });
                actions.push(Action::BroadcastPing);
            }
            EventKind::MessageDelivered { to, from, msg } => {
                debug_assert_eq!(*to, self.id, "engine dispatched message to wrong node");
                match msg {
                    Message::Ping => {
                        actions.push(Action::Log(LogEventKind::PingReceived {
                            node: self.id,
                            from: *from,
                        }));
                        actions.push(Action::SendMessage {
                            to: *from,
                            msg: Message::Pong,
                        });
                    }
                    Message::Pong => {
                        actions.push(Action::Log(LogEventKind::PongReceived {
                            node: self.id,
                            from: *from,
                        }));
                    }
                }
            }
        }
        actions
    }

    fn alloc_timer_id(&mut self) -> TimerId {
        let id = TimerId(self.next_timer_id);
        self.next_timer_id += 1;
        id
    }
}

/// Dispatch enum over node kinds.
///
/// Adding a new kind (e.g., `Raft`) is a matter of adding a variant and a
/// match arm; the compiler forces every call site to handle it.
#[derive(Debug)]
pub enum NodeImpl {
    Ping(PingNode),
}

impl NodeImpl {
    /// Returns the node's `NodeId`.
    #[must_use]
    pub fn id(&self) -> NodeId {
        match self {
            NodeImpl::Ping(n) => n.id,
        }
    }

    /// Emits start actions (called once, when the node is added).
    pub fn on_start(&mut self) -> Vec<Action> {
        match self {
            NodeImpl::Ping(n) => n.on_start(),
        }
    }

    /// Handles one event.
    pub fn handle(
        &mut self,
        now: VirtualTime,
        event_id: EventId,
        payload: &EventPayload,
    ) -> Vec<Action> {
        match self {
            NodeImpl::Ping(n) => n.handle(now, event_id, payload),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn on_start_arms_timer_once() {
        let mut n = PingNode::new(NodeId(1), Duration::from_millis(100));
        let a1 = n.on_start();
        let a2 = n.on_start();
        assert_eq!(a1.len(), 1);
        assert!(matches!(a1[0], Action::ScheduleTimer { .. }));
        assert!(a2.is_empty(), "on_start must be idempotent");
    }

    #[test]
    fn timer_fire_reschedules_and_broadcasts() {
        let mut n = PingNode::new(NodeId(1), Duration::from_millis(100));
        let _ = n.on_start();
        let ev = EventPayload::new(
            EventKind::TimerFired {
                node: NodeId(1),
                timer_id: TimerId(1),
            },
            None,
        );
        let actions = n.handle(VirtualTime(100), EventId(0), &ev);
        assert!(actions
            .iter()
            .any(|a| matches!(a, Action::ScheduleTimer { .. })));
        assert!(actions.iter().any(|a| matches!(a, Action::BroadcastPing)));
    }

    #[test]
    fn ping_replies_with_pong() {
        let mut n = PingNode::new(NodeId(1), Duration::from_millis(100));
        let ev = EventPayload::new(
            EventKind::MessageDelivered {
                to: NodeId(1),
                from: NodeId(2),
                msg: Message::Ping,
            },
            None,
        );
        let actions = n.handle(VirtualTime(0), EventId(0), &ev);
        assert!(actions.iter().any(|a| {
            matches!(a, Action::SendMessage { to, msg: Message::Pong } if *to == NodeId(2))
        }));
    }

    #[test]
    fn availability_is_active() {
        assert!(Availability::Online.is_active());
        assert!(Availability::Recovering.is_active());
        assert!(!Availability::Offline.is_active());
    }

    #[test]
    fn node_impl_dispatches_to_ping() {
        let mut n = NodeImpl::Ping(PingNode::new(NodeId(7), Duration::from_millis(100)));
        assert_eq!(n.id(), NodeId(7));
        let a = n.on_start();
        assert!(matches!(a[0], Action::ScheduleTimer { .. }));
    }
}
