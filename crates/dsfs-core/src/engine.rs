//! Simulation engine.
//!
//! The engine owns the clock, the event queue, the nodes, the network
//! model, the log, and the global random streams. It is the only entity
//! that mutates global state. Nodes are pure state machines returning
//! `Action`s; the engine is the interpreter.
//!
//! Determinism contract: given the same initial configuration and the
//! same master seed, the engine must produce an identical sequence of
//! `LogRecord`s across runs. This is tested in `tests/integration/`.

use std::collections::BTreeMap;
use std::io;

use crate::action::Action;
use crate::event::{EventId, EventKind, EventPayload, Priority};
use crate::log::{EventLog, InMemoryLog, LogEventKind, LogRecord};
use crate::message::Message;
use crate::node::{NodeId, PingNode};
use crate::scheduler::EventQueue;
use crate::time::VirtualTime;

/// Network model used by the engine.
///
/// Stage 2 ships with [`NullNetwork`] only. Stage 3 will add a full model
/// with per-link latency, loss, duplication and partitions. The trait
/// exists now because there are already two concrete futures: null and
/// full. It is a one-method trait, object-safe, cheap to dispatch.
pub trait NetworkModel {
    /// Decides when (in virtual time) a message is delivered.
    ///
    /// Returns `None` if the message is dropped. The current time and the
    /// message endpoints are provided so that latency- or loss-aware
    /// implementations can make decisions.
    fn deliver_at(
        &mut self,
        now: VirtualTime,
        from: NodeId,
        to: NodeId,
        msg: &Message,
    ) -> Option<VirtualTime>;
}

/// Instantaneous, lossless network. Delivers messages at the current
/// virtual time, never drops.
///
/// Used on Stage 2 only to verify the engine loop. The name is deliberate:
/// it signals that the *absence* of a real network model is a Stage-2
/// stand-in, not a permanent design choice.
#[derive(Debug, Default)]
pub struct NullNetwork;

impl NetworkModel for NullNetwork {
    fn deliver_at(
        &mut self,
        now: VirtualTime,
        _from: NodeId,
        _to: NodeId,
        _msg: &Message,
    ) -> Option<VirtualTime> {
        Some(now)
    }
}

/// Engine configuration.
#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// Master seed for all random streams in the simulation.
    ///
    /// Stage 2 does not consume randomness, but the seed is threaded
    /// through so that later stages can create their `StreamRng`s from
    /// it without changing the engine's public surface.
    pub master_seed: u64,
    /// Hard cap on the number of events processed; prevents infinite loops
    /// in scenarios with periodic timers.
    pub max_events: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            master_seed: 0,
            max_events: 10_000,
        }
    }
}

/// The deterministic simulation engine.
pub struct SimulationEngine<L: EventLog, N: NetworkModel> {
    config: EngineConfig,
    clock: VirtualTime,
    queue: EventQueue<EventPayload>,
    nodes: BTreeMap<NodeId, PingNode>,
    network: N,
    log: L,
    next_event_id: u64,
    processed_events: u64,
}

impl<L: EventLog, N: NetworkModel> SimulationEngine<L, N> {
    /// Creates an engine with the given configuration, network model and log.
    #[must_use]
    pub fn new(config: EngineConfig, network: N, log: L) -> Self {
        Self {
            config,
            clock: VirtualTime::ZERO,
            queue: EventQueue::new(),
            nodes: BTreeMap::new(),
            network,
            log,
            next_event_id: 0,
            processed_events: 0,
        }
    }

    /// Returns the current virtual time.
    #[must_use]
    pub fn now(&self) -> VirtualTime {
        self.clock
    }

    /// Adds a ping node to the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if a node with the same id already exists, or if
    /// the node's `on_start` actions cannot be routed (e.g., the log
    /// rejects a record).
    pub fn add_ping_node(&mut self, mut node: PingNode) -> io::Result<()> {
        let id = node.id;
        if self.nodes.contains_key(&id) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("node {id} already exists"),
            ));
        }
        let start_actions = node.on_start();
        self.nodes.insert(id, node);
        self.route_actions(id, None, start_actions)
    }

    /// Consumes the engine and returns the log.
    #[must_use]
    pub fn into_log(self) -> L {
        self.log
    }

    /// Returns a reference to the log.
    #[must_use]
    pub fn log(&self) -> &L {
        &self.log
    }

    /// Number of events processed so far.
    #[must_use]
    pub fn processed_events(&self) -> u64 {
        self.processed_events
    }

    /// Runs the engine until the queue is empty or `max_events` is reached.
    ///
    /// # Errors
    ///
    /// Propagates I/O errors from the event log.
    pub fn run(&mut self) -> io::Result<RunOutcome> {
        while let Some(event) = self.queue.pop() {
            if self.processed_events >= self.config.max_events {
                return Ok(RunOutcome::HitEventLimit {
                    processed: self.processed_events,
                });
            }
            self.clock = event.key.time;
            let event_id = EventId(self.next_event_id);
            self.next_event_id += 1;
            self.processed_events += 1;

            let payload = event.payload;
            let caused_by = payload.caused_by;
            self.dispatch_to_node(payload, event_id, caused_by)?;
        }
        Ok(RunOutcome::Completed {
            processed: self.processed_events,
        })
    }

    /// Dispatches an event to its target node, routes the resulting actions,
    /// and logs the delivery when applicable.
    fn dispatch_to_node(
        &mut self,
        payload: EventPayload,
        event_id: EventId,
        caused_by: Option<EventId>,
    ) -> io::Result<()> {
        let node_id = match &payload.kind {
            EventKind::TimerFired { node, .. } => *node,
            EventKind::MessageDelivered { to, .. } => *to,
        };

        // Log delivery of a network message before the node reacts.
        if let EventKind::MessageDelivered { from, .. } = &payload.kind {
            self.log.record(LogRecord {
                event_id,
                time: self.clock,
                kind: LogEventKind::MessageDelivered {
                    from: *from,
                    to: node_id,
                },
                caused_by,
            })?;
        }

        // `VirtualTime` is `Copy`; take it by value before the mutable
        // borrow of `self.nodes` starts, so the borrow ends on this line.
        let now = self.clock;

        let actions = {
            let node = self.nodes.get_mut(&node_id).ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, format!("node {node_id} not found"))
            })?;
            node.handle(now, event_id, &payload)
        };

        self.route_actions(node_id, Some(event_id), actions)
    }

    /// Interprets actions returned by a node.
    fn route_actions(
        &mut self,
        source: NodeId,
        source_event: Option<EventId>,
        actions: Vec<Action>,
    ) -> io::Result<()> {
        let peer_ids: Vec<NodeId> = self
            .nodes
            .keys()
            .copied()
            .filter(|id| *id != source)
            .collect();

        for action in actions {
            match action {
                Action::Log(kind) => {
                    self.log.record(LogRecord {
                        event_id: source_event.unwrap_or(EventId(u64::MAX)),
                        time: self.clock,
                        kind,
                        caused_by: source_event,
                    })?;
                }
                Action::ScheduleTimer { delay, timer_id } => {
                    let fire_at = self.clock + delay;
                    self.queue.schedule(
                        fire_at,
                        Priority::Timer,
                        EventPayload::new(
                            EventKind::TimerFired {
                                node: source,
                                timer_id,
                            },
                            source_event,
                        ),
                    );
                }
                Action::SendMessage { to, msg } => {
                    self.enqueue_message(source, to, msg, source_event)?;
                }
                Action::BroadcastPing => {
                    for peer in &peer_ids {
                        self.enqueue_message(source, *peer, Message::Ping, source_event)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn enqueue_message(
        &mut self,
        from: NodeId,
        to: NodeId,
        msg: Message,
        caused_by: Option<EventId>,
    ) -> io::Result<()> {
        let deliver_at = self.network.deliver_at(self.clock, from, to, &msg);
        let Some(when) = deliver_at else {
            return Ok(()); // message dropped by the network model
        };
        self.log.record(LogRecord {
            event_id: caused_by.unwrap_or(EventId(u64::MAX)),
            time: self.clock,
            kind: LogEventKind::MessageSent { from, to },
            caused_by,
        })?;
        self.queue.schedule(
            when,
            Priority::Network,
            EventPayload::new(EventKind::MessageDelivered { to, from, msg }, caused_by),
        );
        Ok(())
    }
}

/// Convenience constructor with in-memory log and null network.
///
/// This is a free function rather than an associated function on
/// `SimulationEngine` because type inference cannot resolve the generic
/// parameters of `SimulationEngine` from an associated-function call
/// without turbofish. As a free function, the return type
/// `SimulationEngine<InMemoryLog, NullNetwork>` fully determines `L` and `N`.
#[must_use]
pub fn new_in_memory_engine(config: EngineConfig) -> SimulationEngine<InMemoryLog, NullNetwork> {
    SimulationEngine::new(config, NullNetwork, InMemoryLog::new())
}

/// Outcome of a `run()` call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunOutcome {
    /// The event queue drained naturally.
    Completed { processed: u64 },
    /// The `max_events` cap was hit before the queue drained.
    HitEventLimit { processed: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::Duration;

    fn make_engine() -> SimulationEngine<InMemoryLog, NullNetwork> {
        new_in_memory_engine(EngineConfig {
            master_seed: 42,
            max_events: 10_000,
        })
    }

    #[test]
    fn single_node_with_ping_timer_runs_bounded() {
        let cfg = EngineConfig {
            master_seed: 42,
            max_events: 5,
        };
        let mut e = SimulationEngine::new(cfg, NullNetwork, InMemoryLog::new());
        e.add_ping_node(PingNode::new(NodeId(1), Duration::from_millis(100)))
            .unwrap();
        let outcome = e.run().unwrap();
        assert_eq!(outcome, RunOutcome::HitEventLimit { processed: 5 });
    }

    #[test]
    fn two_nodes_ping_each_other() {
        let mut e = make_engine();
        e.add_ping_node(PingNode::new(NodeId(1), Duration::from_millis(100)))
            .unwrap();
        e.add_ping_node(PingNode::new(NodeId(2), Duration::from_millis(100)))
            .unwrap();
        let outcome = e.run().unwrap();
        assert!(matches!(outcome, RunOutcome::HitEventLimit { .. }));
        let log = e.into_log();
        assert!(log
            .records()
            .iter()
            .any(|r| matches!(r.kind, LogEventKind::PongReceived { .. })));
    }

    #[test]
    fn duplicate_node_id_rejected() {
        let mut e = make_engine();
        e.add_ping_node(PingNode::new(NodeId(1), Duration::from_millis(100)))
            .unwrap();
        let err = e
            .add_ping_node(PingNode::new(NodeId(1), Duration::from_millis(100)))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }
}
