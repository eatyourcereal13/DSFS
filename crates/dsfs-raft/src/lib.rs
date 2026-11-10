//! A pure Raft state machine.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

mod state;

pub mod message;
pub mod node;
pub mod rng;
pub mod types;

pub use message::RaftMessage;
pub use node::{
    FollowerReason, RaftConfig, RaftEvent, RaftNode, RaftOutput, RejectReason, TimerArm,
};
pub use rng::RaftRng;
pub use types::{LogEntry, PeerId, Role, Term};
