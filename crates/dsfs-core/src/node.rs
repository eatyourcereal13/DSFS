//! Virtual node identifiers, availability states, and `NodeImpl`.

use dsfs_raft::{PeerId, RaftNode, RaftOutput};

use crate::action::Action;
use crate::event::{EventId, EventKind, EventPayload};
use crate::log::LogEventKind;
use crate::message::Message;
use crate::raft_adapter::raft_event_to_log_event;
use crate::time::{Duration, VirtualTime};
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct NodeId(pub u32);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "n{}", self.0)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct TimerId(pub u64);

#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Serialize, Deserialize,
)]
pub enum Availability {
    #[default]
    Online,
    Offline,
    Recovering,
}

impl Availability {
    #[must_use]
    pub fn is_active(self) -> bool {
        matches!(self, Availability::Online | Availability::Recovering)
    }
}

#[derive(Debug)]
pub struct PingNode {
    pub id: NodeId,
    pub ping_interval: Duration,
    next_timer_id: u64,
    initialised: bool,
}

impl PingNode {
    #[must_use]
    pub fn new(id: NodeId, ping_interval: Duration) -> Self {
        Self {
            id,
            ping_interval,
            next_timer_id: 1,
            initialised: false,
        }
    }

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

    pub fn handle(
        &mut self,
        _now: VirtualTime,
        _event_id: EventId,
        payload: &EventPayload,
    ) -> Vec<Action> {
        let mut actions = Vec::new();
        match &payload.kind {
            EventKind::TimerFired { node, timer_id } => {
                debug_assert_eq!(*node, self.id);
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
                debug_assert_eq!(*to, self.id);
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
                    Message::Raft(_) => {
                        // PingNode ignores Raft messages.
                    }
                }
            }
            EventKind::ClientCommand { .. } => {
                // PingNode ignores client commands.
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

pub struct RaftAdapter {
    pub id: NodeId,
    pub inner: RaftNode,
}

impl std::fmt::Debug for RaftAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftAdapter")
            .field("id", &self.id)
            .field("role", &self.inner.role())
            .field("term", &self.inner.term())
            .finish()
    }
}

impl RaftAdapter {
    #[must_use]
    pub fn new(id: NodeId, inner: RaftNode) -> Self {
        Self { id, inner }
    }

    pub fn on_start(&mut self) -> Vec<Action> {
        let out = self.inner.on_start();
        self.output_to_actions(out)
    }

    pub fn handle(
        &mut self,
        _now: VirtualTime,
        _event_id: EventId,
        payload: &EventPayload,
    ) -> Vec<Action> {
        match &payload.kind {
            EventKind::TimerFired { node, timer_id } => {
                debug_assert_eq!(*node, self.id);
                let out = self.inner.on_timer(timer_id.0);
                self.output_to_actions(out)
            }
            EventKind::MessageDelivered { to, from, msg } => {
                debug_assert_eq!(*to, self.id);
                let Message::Raft(m) = msg else {
                    return Vec::new();
                };
                let out = self.inner.on_message(PeerId(from.0), m.clone());
                self.output_to_actions(out)
            }
            EventKind::ClientCommand { command, .. } => {
                let out = self.inner.on_client_command(command.clone());
                self.output_to_actions(out)
            }
        }
    }

    fn output_to_actions(&mut self, out: RaftOutput) -> Vec<Action> {
        let mut actions = Vec::with_capacity(out.messages.len() + 1 + out.events.len());
        for (peer, msg) in out.messages {
            actions.push(Action::SendMessage {
                to: NodeId(peer.0),
                msg: Message::Raft(msg),
            });
        }
        if let Some(arm) = out.set_timer {
            actions.push(Action::ScheduleTimer {
                delay: Duration::from_nanos(arm.after.as_nanos() as u64),
                timer_id: TimerId(arm.generation),
            });
        }
        for ev in out.events {
            actions.push(Action::Log(raft_event_to_log_event(self.id, &ev)));
        }
        actions
    }
}

/// Dispatch enum over node kinds.
///
/// `RaftAdapter` is boxed: it is ~10x larger than `PingNode` (Raft state is
/// big). Boxing keeps `NodeImpl` small regardless of variant, so moving and
/// storing `PingNode`s does not pay for Raft's footprint. See DD-018.
#[derive(Debug)]
pub enum NodeImpl {
    Ping(PingNode),
    Raft(Box<RaftAdapter>),
}

impl NodeImpl {
    #[must_use]
    pub fn id(&self) -> NodeId {
        match self {
            NodeImpl::Ping(n) => n.id,
            NodeImpl::Raft(n) => n.id,
        }
    }

    pub fn on_start(&mut self) -> Vec<Action> {
        match self {
            NodeImpl::Ping(n) => n.on_start(),
            NodeImpl::Raft(n) => n.on_start(),
        }
    }

    pub fn handle(
        &mut self,
        now: VirtualTime,
        event_id: EventId,
        payload: &EventPayload,
    ) -> Vec<Action> {
        match self {
            NodeImpl::Ping(n) => n.handle(now, event_id, payload),
            NodeImpl::Raft(n) => n.handle(now, event_id, payload),
        }
    }

    #[must_use]
    pub fn raft_leader_id(&self) -> Option<NodeId> {
        match self {
            NodeImpl::Raft(a) if a.inner.role() == dsfs_raft::Role::Leader => Some(a.id),
            _ => None,
        }
    }

    #[must_use]
    pub fn raft_applied(&self) -> Option<&[Vec<u8>]> {
        match self {
            NodeImpl::Raft(a) => Some(a.inner.applied()),
            _ => None,
        }
    }

    #[must_use]
    pub fn raft_commit_index(&self) -> Option<u64> {
        match self {
            NodeImpl::Raft(a) => Some(a.inner.commit_index()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::Duration;

    #[test]
    fn on_start_arms_timer_once() {
        let mut n = PingNode::new(NodeId(1), Duration::from_millis(100));
        let a1 = n.on_start();
        let a2 = n.on_start();
        assert_eq!(a1.len(), 1);
        assert!(matches!(a1[0], Action::ScheduleTimer { .. }));
        assert!(a2.is_empty());
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
