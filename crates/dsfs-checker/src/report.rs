//! Report types.

use dsfs_core::event::EventId;
use dsfs_core::time::VirtualTime;

/// One invariant violation.
#[derive(Clone, Debug)]
pub struct Violation {
    /// Name of the violated invariant.
    pub invariant: &'static str,
    /// The event that triggered detection.
    pub event_id: Option<EventId>,
    /// Time of the event.
    pub time: Option<VirtualTime>,
    /// Human-readable description.
    pub context: String,
    /// Events related to the violation.
    pub related_events: Vec<EventId>,
}

/// Overall statistics about the log.
#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub total_records: usize,
    pub messages_sent: usize,
    pub messages_dropped: usize,
    pub messages_duplicated: usize,
    pub leaders_elected: usize,
    pub applied_commands: usize,
}

/// Result of a checker run.
#[derive(Clone, Debug, Default)]
pub struct CheckReport {
    pub violations: Vec<Violation>,
    pub stats: Stats,
}

impl CheckReport {
    /// Returns `true` if no violations were found.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.violations.is_empty()
    }
}
