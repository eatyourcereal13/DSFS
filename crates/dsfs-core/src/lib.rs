//! Core primitives for the Distributed Systems Failure Simulator.
//!
//! Stage 2 adds the deterministic engine loop, `Action`-based node
//! interaction, in-memory event log, and a trivial network model used to
//! exercise the loop with `PingNode`s. Raft, real network, and fault
//! injection come in later stages.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

pub mod action;
pub mod engine;
pub mod event;
pub mod log;
pub mod message;
pub mod node;
pub mod rng;
pub mod scheduler;
pub mod time;
