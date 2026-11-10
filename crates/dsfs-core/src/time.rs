//! Virtual time.
//!
//! All time in the simulation is expressed as [`VirtualTime`], a monotonic
//! count of nanoseconds since simulation start. The simulation must never
//! consult the real system clock (see `docs/design-decisions.md`, DD-001).

use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Add;

/// Virtual time in nanoseconds since the start of the simulation.
///
/// Using `u64` nanoseconds gives a range of about 584 years. `saturating_add`
/// must be used when adding durations; overflow indicates a bug in the
/// scenario, not a runtime condition.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Serialize, Deserialize,
)]
pub struct VirtualTime(pub u64);

impl VirtualTime {
    /// Zero, i.e. the start of the simulation.
    pub const ZERO: Self = Self(0);

    /// Constructs a [`VirtualTime`] from milliseconds.
    #[must_use]
    pub const fn from_millis(ms: u64) -> Self {
        Self(ms.saturating_mul(1_000_000))
    }

    /// Constructs a [`VirtualTime`] from microseconds.
    #[must_use]
    pub const fn from_micros(us: u64) -> Self {
        Self(us.saturating_mul(1_000))
    }

    /// Returns the number of whole milliseconds elapsed.
    #[must_use]
    pub const fn as_millis(self) -> u64 {
        self.0 / 1_000_000
    }

    /// Adds a duration, saturating at `u64::MAX` nanoseconds.
    #[must_use]
    pub fn saturating_add(self, d: Duration) -> Self {
        Self(self.0.saturating_add(d.0))
    }
}

impl Add<Duration> for VirtualTime {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        self.saturating_add(rhs)
    }
}

impl fmt::Display for VirtualTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 % 1_000_000 == 0 {
            write!(f, "{}ms", self.0 / 1_000_000)
        } else {
            write!(f, "{}ns", self.0)
        }
    }
}

/// A duration in virtual time, expressed in nanoseconds.
///
/// This mirrors `std::time::Duration` but lives in the virtual domain, so a
/// real duration can never be accidentally added to a virtual time.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Serialize, Deserialize,
)]
pub struct Duration(pub u64);

impl Duration {
    /// Zero duration.
    pub const ZERO: Self = Self(0);

    /// Constructs a duration from milliseconds.
    #[must_use]
    pub const fn from_millis(ms: u64) -> Self {
        Self(ms.saturating_mul(1_000_000))
    }

    /// Constructs a duration from microseconds.
    #[must_use]
    pub const fn from_micros(us: u64) -> Self {
        Self(us.saturating_mul(1_000))
    }

    /// Constructs a duration from nanoseconds.
    #[must_use]
    pub const fn from_nanos(ns: u64) -> Self {
        Self(ns)
    }

    /// Returns the value in nanoseconds.
    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 % 1_000_000 == 0 {
            write!(f, "{}ms", self.0 / 1_000_000)
        } else {
            write!(f, "{}ns", self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_is_default() {
        assert_eq!(VirtualTime::default(), VirtualTime::ZERO);
        assert_eq!(Duration::default(), Duration::ZERO);
    }

    #[test]
    fn from_millis_and_back() {
        let t = VirtualTime::from_millis(1234);
        assert_eq!(t.as_millis(), 1234);
    }

    #[test]
    fn saturating_add_does_not_panic() {
        let t = VirtualTime(u64::MAX - 5);
        let t2 = t + Duration::from_nanos(100);
        assert_eq!(t2, VirtualTime(u64::MAX));
    }

    #[test]
    fn display_ms() {
        assert_eq!(VirtualTime::from_millis(5).to_string(), "5ms");
        assert_eq!(VirtualTime(1500).to_string(), "1500ns");
    }

    #[test]
    fn ordering_is_numeric() {
        assert!(VirtualTime(1) < VirtualTime(2));
        assert!(Duration::from_millis(1) < Duration::from_millis(2));
    }
}
