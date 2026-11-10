//! Offline consistency checker for DSFS logs.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

pub mod invariants;
pub mod reader;
pub mod report;

pub use reader::{read_jsonl, ReadOutcome};
pub use report::{CheckReport, Stats, Violation};
