//! Actions returned by nodes to the engine.
//!
//! The engine is the only entity that mutates global simulation state.
//! Nodes are pure-ish state machines: they receive events and return
//! actions; the engine interprets those actions against the network,
//! the scheduler, and the log.
//!
//! Stage 2 includes only what `PingNode` needs. New variants are added
//! when a second consumer (Raft in Stage 5, fault injection in Stage 7)
//! needs them — not preemptively.

use crate::log::LogEventKind;
use crate::message::Message;
use crate::node::{NodeId, TimerId};
use crate::time::Duration;

/// Something a node asks the engine to do.
#[derive(Clone, Debug)]
pub enum Action {
    /// Send `msg` to `to`.
    SendMessage { to: NodeId, msg: Message },
    /// Arm a timer for `delay`, with local id `timer_id`.
    ScheduleTimer { delay: Duration, timer_id: TimerId },
    /// Append a record to the log.
    Log(LogEventKind),
    /// Broadcast `Ping` to every other registered node.
    ///
    /// This variant exists because `PingNode` has no peer list of its own:
    /// peers are a cluster concern, known to the engine. A node asking the
    /// engine to broadcast is the cleanest way to keep the node ignorant
    /// of cluster membership at this stage. Raft nodes will *not* use this
    /// variant — they will send explicit `SendMessage` per peer, because
    /// Raft must know each recipient to track `next_index`/`match_index`.
    BroadcastPing,
}
