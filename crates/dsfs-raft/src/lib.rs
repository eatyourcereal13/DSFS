//! A pure Raft state machine.
//!
//! This crate knows nothing about the network, virtual time, node
//! identities, or the simulation engine. It exposes [`RaftNode`] as a
//! deterministic state machine: `on_start`, `on_timer`, `on_message` all
//! return [`RaftOutput`] describing what to send, which timer to set, and
//! which events to log.
//!
//! See `docs/raft.md` (in the parent repository) for the list of
//! limitations relative to the full Raft specification.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

mod state;

pub mod message;
pub mod node;
pub mod rng;
pub mod types;

pub use message::RaftMessage;
pub use node::{FollowerReason, RaftConfig, RaftEvent, RaftNode, RaftOutput, TimerArm};
pub use rng::RaftRng;
pub use types::{LogEntry, PeerId, Role, Term};
