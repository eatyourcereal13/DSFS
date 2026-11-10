//! Core primitives for the Distributed Systems Failure Simulator.
//!
//! - [`time`] — `VirtualTime`, `Duration`.
//! - [`event`] — event keys, priorities, payloads.
//! - [`scheduler`] — deterministic `EventQueue`.
//! - [`rng`] — stream-partitioned deterministic RNG.
//! - [`network`] — `NetworkModel`, `FullNetwork`, `LinkConfig`.
//! - [`node`] — `NodeId`, `PingNode`.
//! - [`message`] — `Message`.
//! - [`action`] — `Action` returned by nodes.
//! - [`log`] — `EventLog`, `LogRecord`.
//! - [`engine`] — `SimulationEngine`.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

pub mod action;
pub mod engine;
pub mod event;
pub mod log;
pub mod message;
pub mod network;
pub mod node;
pub mod rng;
pub mod scheduler;
pub mod time;
