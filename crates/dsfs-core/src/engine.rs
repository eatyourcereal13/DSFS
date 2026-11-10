//! Simulation engine.

use std::collections::BTreeMap;
use std::io;

use crate::action::Action;
use crate::event::{EventId, EventKind, EventPayload, Priority};
use crate::log::{EventLog, InMemoryLog, LogEventKind, LogRecord};
use crate::message::Message;
use crate::network::{NetworkModel, NullNetwork};
use crate::node::{Availability, NodeId, NodeImpl, PingNode};
use crate::scheduler::EventQueue;
use crate::time::{Duration, VirtualTime};

pub use crate::network::{Delivery, FullNetwork, LinkConfig};

/// Base slowdown per incoming event, in nanoseconds, when `slow_factor > 1`.
const SLOW_BASE_NANOS: u64 = 1_000_000;

/// Engine configuration.
#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// Master seed for all random streams in the simulation.
    pub master_seed: u64,
    /// Hard cap on the number of events processed.
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

#[derive(Debug)]
struct NodeEntry {
    node: NodeImpl,
    availability: Availability,
    slow_factor: f64,
}

/// The deterministic simulation engine.
pub struct SimulationEngine<L: EventLog, N: NetworkModel> {
    config: EngineConfig,
    clock: VirtualTime,
    queue: EventQueue<EventPayload>,
    nodes: BTreeMap<NodeId, NodeEntry>,
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

    /// Shared reference to the network model.
    #[must_use]
    pub fn network(&self) -> &N {
        &self.network
    }

    /// Mutable reference to the network model.
    pub fn network_mut(&mut self) -> &mut N {
        &mut self.network
    }

    /// Adds a ping node to the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if a node with the same id already exists.
    pub fn add_ping_node(&mut self, node: PingNode) -> io::Result<()> {
        self.add_node(NodeImpl::Ping(node))
    }

    /// Adds a node of any supported kind.
    ///
    /// # Errors
    ///
    /// Returns an error if a node with the same id already exists, or if
    /// the node's start actions cannot be routed.
    pub fn add_node(&mut self, mut node: NodeImpl) -> io::Result<()> {
        let id = node.id();
        if self.nodes.contains_key(&id) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("node {id} already exists"),
            ));
        }
        let start_actions = node.on_start();
        self.nodes.insert(
            id,
            NodeEntry {
                node,
                availability: Availability::Online,
                slow_factor: 1.0,
            },
        );
        self.route_actions(id, None, start_actions)
    }

    /// Sets the availability of a node.
    ///
    /// # Errors
    ///
    /// Returns an error if the node does not exist.
    pub fn set_availability(
        &mut self,
        node_id: NodeId,
        availability: Availability,
    ) -> io::Result<()> {
        let entry = self.nodes.get_mut(&node_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("node {node_id} not found"))
        })?;
        entry.availability = availability;
        self.log.record(LogRecord {
            event_id: EventId(u64::MAX),
            time: self.clock,
            kind: LogEventKind::NodeAvailabilityChanged {
                node: node_id,
                to: availability,
            },
            caused_by: None,
        })
    }

    /// Sets the slow factor of a node (`>= 1.0`).
    ///
    /// # Errors
    ///
    /// Returns an error if the node does not exist or `factor < 1.0`.
    pub fn set_slow_factor(&mut self, node_id: NodeId, factor: f64) -> io::Result<()> {
        if factor < 1.0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("slow_factor must be >= 1.0, got {factor}"),
            ));
        }
        let entry = self.nodes.get_mut(&node_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("node {node_id} not found"))
        })?;
        entry.slow_factor = factor;
        self.log.record(LogRecord {
            event_id: EventId(u64::MAX),
            time: self.clock,
            kind: LogEventKind::NodeSlowFactorChanged {
                node: node_id,
                slow_factor_x1000: (factor * 1000.0) as u32,
            },
            caused_by: None,
        })
    }

    /// Returns the availability of a node, if it exists.
    #[must_use]
    pub fn availability(&self, node_id: NodeId) -> Option<Availability> {
        self.nodes.get(&node_id).map(|e| e.availability)
    }

    /// Finds the current Raft leader among active nodes, if any.
    #[must_use]
    pub fn find_leader(&self) -> Option<NodeId> {
        for (id, entry) in &self.nodes {
            if !entry.availability.is_active() {
                continue;
            }
            if entry.node.raft_leader_id().is_some() {
                return Some(*id);
            }
        }
        None
    }

    /// Returns the commands applied by the given Raft node, if it is one.
    #[must_use]
    pub fn raft_applied(&self, node_id: NodeId) -> Option<&[Vec<u8>]> {
        self.nodes.get(&node_id)?.node.raft_applied()
    }

    /// Returns the commit index of the given Raft node, if it is one.
    #[must_use]
    pub fn raft_commit_index(&self, node_id: NodeId) -> Option<u64> {
        self.nodes.get(&node_id)?.node.raft_commit_index()
    }

    /// Submits a client command to the current leader.
    ///
    /// # Errors
    ///
    /// Returns `NotFound` if there is no active leader.
    pub fn submit_command(&mut self, command: Vec<u8>) -> io::Result<()> {
        let leader = self
            .find_leader()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no leader available"))?;
        self.queue.schedule(
            self.clock,
            Priority::Client,
            EventPayload::new(
                EventKind::ClientCommand {
                    to: leader,
                    command,
                },
                None,
            ),
        );
        Ok(())
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

    /// Runs the engine until the queue is empty or the remaining global
    /// budget from `EngineConfig::max_events` is exhausted.
    ///
    /// # Errors
    ///
    /// Propagates I/O errors from the event log.
    pub fn run(&mut self) -> io::Result<RunOutcome> {
        let budget = self.config.max_events.saturating_sub(self.processed_events);
        self.run_until(budget)
    }

    /// Runs the engine until the queue is empty or `budget` more events
    /// have been processed.
    ///
    /// Use this for interactive control:
    ///
    /// ```ignore
    /// e.run_until(10_000)?;    // let elections settle
    /// e.submit_command(...)?;
    /// e.run_until(5_000)?;     // let the command replicate and commit
    /// ```
    ///
    /// # Errors
    ///
    /// Propagates I/O errors from the event log.
    pub fn run_until(&mut self, budget: u64) -> io::Result<RunOutcome> {
        let target = self.processed_events.saturating_add(budget);
        while self.processed_events < target {
            let Some(event) = self.queue.pop() else {
                return Ok(RunOutcome::Completed {
                    processed: self.processed_events,
                });
            };
            self.clock = event.key.time;
            let event_id = EventId(self.next_event_id);
            self.next_event_id += 1;
            self.processed_events += 1;

            let payload = event.payload;
            let caused_by = payload.caused_by;
            self.dispatch_to_node(payload, event_id, caused_by)?;
        }
        Ok(RunOutcome::HitEventLimit {
            processed: self.processed_events,
        })
    }

    fn dispatch_to_node(
        &mut self,
        payload: EventPayload,
        event_id: EventId,
        caused_by: Option<EventId>,
    ) -> io::Result<()> {
        let node_id = match &payload.kind {
            EventKind::TimerFired { node, .. } => *node,
            EventKind::MessageDelivered { to, .. } => *to,
            EventKind::ClientCommand { to, .. } => *to,
        };

        let availability = self
            .nodes
            .get(&node_id)
            .map(|e| e.availability)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, format!("node {node_id} not found"))
            })?;

        if !availability.is_active() {
            return self.handle_offline_dispatch(node_id, event_id, caused_by, &payload);
        }

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

        let now = self.clock;

        let actions = {
            let entry = self.nodes.get_mut(&node_id).ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, format!("node {node_id} not found"))
            })?;
            entry.node.handle(now, event_id, &payload)
        };

        self.route_actions(node_id, Some(event_id), actions)
    }

    fn handle_offline_dispatch(
        &mut self,
        node_id: NodeId,
        event_id: EventId,
        caused_by: Option<EventId>,
        payload: &EventPayload,
    ) -> io::Result<()> {
        match &payload.kind {
            EventKind::MessageDelivered { from, .. } => {
                self.log.record(LogRecord {
                    event_id,
                    time: self.clock,
                    kind: LogEventKind::MessageDropped {
                        from: *from,
                        to: node_id,
                    },
                    caused_by,
                })?;
            }
            EventKind::TimerFired { .. } | EventKind::ClientCommand { .. } => {
                self.log.record(LogRecord {
                    event_id,
                    time: self.clock,
                    kind: LogEventKind::NodeIgnoredWhileOffline {
                        node: node_id,
                        event_id,
                    },
                    caused_by,
                })?;
            }
        }
        Ok(())
    }

    fn route_actions(
        &mut self,
        source: NodeId,
        source_event: Option<EventId>,
        actions: Vec<Action>,
    ) -> io::Result<()> {
        let peer_ids: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|(id, entry)| **id != source && entry.availability.is_active())
            .map(|(id, _)| *id)
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
        let deliveries = self.network.deliveries(self.clock, from, to, &msg);

        if deliveries.is_empty() {
            self.log.record(LogRecord {
                event_id: caused_by.unwrap_or(EventId(u64::MAX)),
                time: self.clock,
                kind: LogEventKind::MessageDropped { from, to },
                caused_by,
            })?;
            return Ok(());
        }

        let slow = self.nodes.get(&to).map(|e| e.slow_factor).unwrap_or(1.0);
        let extra_ns = if slow > 1.0 {
            ((slow - 1.0) * SLOW_BASE_NANOS as f64) as u64
        } else {
            0
        };

        for d in deliveries {
            self.log.record(LogRecord {
                event_id: caused_by.unwrap_or(EventId(u64::MAX)),
                time: self.clock,
                kind: if d.is_duplicate {
                    LogEventKind::MessageDuplicated { from, to }
                } else {
                    LogEventKind::MessageSent { from, to }
                },
                caused_by,
            })?;
            let deliver_at = if extra_ns > 0 {
                self.log.record(LogRecord {
                    event_id: caused_by.unwrap_or(EventId(u64::MAX)),
                    time: self.clock,
                    kind: LogEventKind::MessageDelayedBySlowNode { from, to },
                    caused_by,
                })?;
                d.at + Duration::from_nanos(extra_ns)
            } else {
                d.at
            };
            self.queue.schedule(
                deliver_at,
                Priority::Network,
                EventPayload::new(
                    EventKind::MessageDelivered {
                        to,
                        from,
                        msg: msg.clone(),
                    },
                    caused_by,
                ),
            );
        }
        Ok(())
    }
}

/// Convenience constructor with in-memory log and null network.
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
    use crate::node::PingNode;
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

    #[test]
    fn full_network_with_latency_delays_messages() {
        let cfg = EngineConfig {
            master_seed: 42,
            max_events: 100,
        };
        let link = LinkConfig::with_latency(Duration::from_millis(50));
        let net = FullNetwork::new(42, link);
        let mut e = SimulationEngine::new(cfg, net, InMemoryLog::new());
        e.add_ping_node(PingNode::new(NodeId(1), Duration::from_millis(10)))
            .unwrap();
        e.add_ping_node(PingNode::new(NodeId(2), Duration::from_millis(10)))
            .unwrap();
        e.run().unwrap();
        let log = e.into_log();
        let first_send = log
            .records()
            .iter()
            .find(|r| matches!(r.kind, LogEventKind::MessageSent { .. }))
            .expect("expected a sent message");
        let first_delivery = log
            .records()
            .iter()
            .find(|r| matches!(r.kind, LogEventKind::MessageDelivered { .. }))
            .expect("expected a delivered message");
        assert!(
            first_delivery.time.0 > first_send.time.0,
            "delivery at {:?} must be after send at {:?}",
            first_delivery.time,
            first_send.time
        );
    }

    #[test]
    fn offline_node_does_not_process_messages() {
        let mut e = make_engine();
        e.add_ping_node(PingNode::new(NodeId(1), Duration::from_millis(100)))
            .unwrap();
        e.add_ping_node(PingNode::new(NodeId(2), Duration::from_millis(100)))
            .unwrap();
        e.set_availability(NodeId(2), Availability::Offline)
            .unwrap();
        e.run().unwrap();
        let log = e.into_log();
        let pongs_to_2 = log
            .records()
            .iter()
            .filter(
                |r| matches!(r.kind, LogEventKind::PongReceived { node, .. } if node == NodeId(2)),
            )
            .count();
        assert_eq!(pongs_to_2, 0, "offline node must not receive pongs");
    }

    #[test]
    fn set_slow_factor_validates_input() {
        let mut e = make_engine();
        e.add_ping_node(PingNode::new(NodeId(1), Duration::from_millis(100)))
            .unwrap();
        let err = e.set_slow_factor(NodeId(1), 0.5).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn set_availability_rejects_missing_node() {
        let mut e = make_engine();
        let err = e
            .set_availability(NodeId(99), Availability::Offline)
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn submit_command_without_leader_errors() {
        let mut e = make_engine();
        e.add_ping_node(PingNode::new(NodeId(1), Duration::from_millis(100)))
            .unwrap();
        let err = e.submit_command(b"x".to_vec()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }
}
