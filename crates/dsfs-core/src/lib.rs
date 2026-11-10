//! Core primitives for the Distributed Systems Failure Simulator.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

pub mod action;
pub mod engine;
pub mod event;
pub mod fault_injector;
pub mod log;
pub mod message;
pub mod network;
pub mod node;
pub mod raft_adapter;
pub mod rng;
pub mod scenario;
pub mod scheduler;
pub mod time;
