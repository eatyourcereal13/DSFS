//! Virtual node identifiers and the minimal node state for Stage 2.
//!
//! The trait/struct split here is deliberately *not* an abstraction yet:
//! Stage 2 has exactly one node kind (`PingNode`). When Raft arrives in
//! Stage 5, an `enum NodeImpl { Ping(PingNode), Raft(RaftNode) }` will be
//! introduced (see docs/design-decisions.md DD-008, to be written then).
//! We do not pre-abstract with `dyn Trait` because there is no second
//! implementor yet — and that would violate the "abstraction after the
//! second consumer" rule.

use crate::action::Action;
use crate::event::{EventId, EventKind, EventPayload};
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
///
/// Uniqueness is only required within a single node; the engine pairs
/// `(node, timer_id)` when dispatching.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct TimerId(pub u64);

/// Minimal node used to exercise the engine loop before Raft exists.
///
/// Behaviour: on start, arm a timer for `ping_interval`. On timer fire,
/// send a `Ping` to every other node and re-arm. On `Ping` received,
/// reply with `Pong`. On `Pong`, log it.
///
/// No randomness, no real time, no I/O — hence fully deterministic.
#[derive(Debug)]
pub struct PingNode {
    pub id: NodeId,
    pub ping_interval: Duration,
    pub next_timer_id: u64,
    /// Set to `true` once the initial timer is armed, to avoid doing it
    /// more than once if the node is somehow reconstructed.
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

    /// Emits the "start" actions: arm the initial ping timer.
    ///
    /// Called by the engine exactly once, when the node is added. This is
    /// not part of `handle` because "node just appeared" is not an event
    /// from the scheduler's point of view; it is a lifecycle hook.
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

    /// Handles a single event, returning the actions to perform.
    ///
    /// Note: this method does not mutate any node other than `self`, does
    /// not touch the network, and does not read the wall clock. It only
    /// maps `(now, event) -> Vec<Action>`.
    pub fn handle(
        &mut self,
        _now: VirtualTime,
        event_id: EventId,
        payload: &EventPayload,
    ) -> Vec<Action> {
        let mut actions = Vec::new();
        match &payload.kind {
            EventKind::TimerFired { node, timer_id } => {
                debug_assert_eq!(*node, self.id, "engine dispatched timer to wrong node");
                actions.push(Action::Log(crate::log::LogEventKind::TimerFired {
                    node: self.id,
                    timer_id: *timer_id,
                }));
                // Send Ping to every other node. On Stage 2 the peer list is
                // passed in via `broadcast_peers`. We keep `PingNode` simple
                // and let the engine provide the peer set as a field on the
                // action. See engine.rs `route_actions`.
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
                        actions.push(Action::Log(crate::log::LogEventKind::PingReceived {
                            node: self.id,
                            from: *from,
                        }));
                        actions.push(Action::SendMessage {
                            to: *from,
                            msg: Message::Pong,
                        });
                    }
                    Message::Pong => {
                        actions.push(Action::Log(crate::log::LogEventKind::PongReceived {
                            node: self.id,
                            from: *from,
                        }));
                    }
                }
            }
        }
        let _ = event_id; // preserved for future use in causal logging
        actions
    }

    fn alloc_timer_id(&mut self) -> TimerId {
        let id = TimerId(self.next_timer_id);
        self.next_timer_id += 1;
        id
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
        assert!(actions.iter().any(
            |a| matches!(a, Action::SendMessage { to, msg: Message::Pong } if *to == NodeId(2))
        ));
    }
}
