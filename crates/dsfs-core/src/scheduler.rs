//! Deterministic event scheduler.
//!
//! [`EventQueue`] is a min-heap over [`EventKey`]. It assigns monotonically
//! increasing [`EventSeq`] values on insertion, so that events with equal
//! `(time, priority)` are processed in registration order. This is the only
//! determinism guarantee the scheduler provides; the semantics of
//! `(time, priority, seq)` are documented in DD-002.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::event::{EventKey, EventSeq, Priority};
use crate::time::VirtualTime;

/// A queued event: its sort key and an opaque payload.
///
/// Ordering is defined by `key` alone; `payload` is deliberately ignored.
/// This lets the scheduler order events without imposing `T: Ord`, and
/// matches DD-002: the only tiebreaker among equal `(time, priority)` is
/// `seq`, which is unique, so payload order never matters.
#[derive(Clone, Debug)]
pub struct QueuedEvent<T> {
    pub key: EventKey,
    pub payload: T,
}

impl<T> PartialEq for QueuedEvent<T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<T> Eq for QueuedEvent<T> {}

impl<T> PartialOrd for QueuedEvent<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for QueuedEvent<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

/// Min-heap of scheduled events.
///
/// Invariants maintained by the queue:
///
/// - `seq` values are strictly increasing across the lifetime of the queue.
/// - Popping returns events in ascending [`EventKey`] order.
/// - Ties on `(time, priority)` are broken by `seq` (registration order).
#[derive(Debug)]
pub struct EventQueue<T> {
    heap: BinaryHeap<Reverse<QueuedEvent<T>>>,
    next_seq: u64,
}

impl<T> Default for EventQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> EventQueue<T> {
    /// Creates an empty queue with `seq` starting at `0`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            next_seq: 0,
        }
    }

    /// Number of queued events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Returns `true` if no events are queued.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Next `seq` value that will be assigned. Useful for tests and logging.
    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Schedules `payload` at `time` with `priority`.
    ///
    /// Returns the assigned [`EventKey`] so that callers can log it or use
    /// it as a causal reference for future events.
    pub fn schedule(&mut self, time: VirtualTime, priority: Priority, payload: T) -> EventKey {
        let seq = EventSeq(self.next_seq);
        self.next_seq = self
            .next_seq
            .checked_add(1)
            .expect("event sequence counter overflow");
        let key = EventKey::new(time, priority, seq);
        self.heap.push(Reverse(QueuedEvent { key, payload }));
        key
    }

    /// Pops the earliest event, or returns `None` if the queue is empty.
    pub fn pop(&mut self) -> Option<QueuedEvent<T>> {
        self.heap.pop().map(|Reverse(e)| e)
    }

    /// Peeks at the earliest event without removing it.
    #[must_use]
    pub fn peek(&self) -> Option<&QueuedEvent<T>> {
        self.heap.peek().map(|Reverse(e)| e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_queue_returns_none() {
        let mut q: EventQueue<&'static str> = EventQueue::new();
        assert!(q.is_empty());
        assert_eq!(q.len(), 0);
        assert!(q.pop().is_none());
        assert!(q.peek().is_none());
    }

    #[test]
    fn pops_in_time_order() {
        let mut q = EventQueue::new();
        q.schedule(VirtualTime(30), Priority::Network, "c");
        q.schedule(VirtualTime(10), Priority::Network, "a");
        q.schedule(VirtualTime(20), Priority::Network, "b");

        assert_eq!(q.pop().unwrap().payload, "a");
        assert_eq!(q.pop().unwrap().payload, "b");
        assert_eq!(q.pop().unwrap().payload, "c");
        assert!(q.is_empty());
    }

    #[test]
    fn ties_broken_by_priority() {
        let mut q = EventQueue::new();
        q.schedule(VirtualTime(10), Priority::System, "system");
        q.schedule(VirtualTime(10), Priority::Client, "client");
        q.schedule(VirtualTime(10), Priority::Network, "network");
        q.schedule(VirtualTime(10), Priority::Timer, "timer");

        assert_eq!(q.pop().unwrap().payload, "timer");
        assert_eq!(q.pop().unwrap().payload, "network");
        assert_eq!(q.pop().unwrap().payload, "client");
        assert_eq!(q.pop().unwrap().payload, "system");
    }

    #[test]
    fn ties_broken_by_seq_in_registration_order() {
        let mut q = EventQueue::new();
        for i in 0..100 {
            q.schedule(VirtualTime(10), Priority::Network, i);
        }
        for i in 0..100 {
            assert_eq!(q.pop().unwrap().payload, i);
        }
    }

    #[test]
    fn seq_is_strictly_increasing() {
        let mut q = EventQueue::new();
        let k1 = q.schedule(VirtualTime(100), Priority::Network, ());
        let k2 = q.schedule(VirtualTime(1), Priority::Timer, ());
        let k3 = q.schedule(VirtualTime(50), Priority::Client, ());
        assert_eq!(k1.seq, EventSeq(0));
        assert_eq!(k2.seq, EventSeq(1));
        assert_eq!(k3.seq, EventSeq(2));
        assert_eq!(q.next_seq(), 3);
    }

    #[test]
    fn peek_does_not_remove() {
        let mut q = EventQueue::new();
        q.schedule(VirtualTime(10), Priority::Network, "a");
        assert_eq!(q.peek().unwrap().payload, "a");
        assert_eq!(q.len(), 1);
        assert_eq!(q.pop().unwrap().payload, "a");
        assert!(q.peek().is_none());
    }

    /// Determinism regression test: a fixed sequence of schedules must produce
    /// an identical pop order across runs.
    #[test]
    fn deterministic_pop_order_with_mixed_keys() {
        fn run() -> Vec<u32> {
            let mut q = EventQueue::new();
            let priorities = [
                Priority::System,
                Priority::Client,
                Priority::Network,
                Priority::Timer,
                Priority::Network,
                Priority::Timer,
                Priority::Client,
            ];
            for (i, p) in priorities.into_iter().enumerate() {
                q.schedule(VirtualTime(10), p, i as u32);
            }
            let mut out = Vec::new();
            while let Some(e) = q.pop() {
                out.push(e.payload);
            }
            out
        }

        let a = run();
        let b = run();
        let c = run();
        assert_eq!(a, b);
        assert_eq!(b, c);
        // Semantic expectation:
        // Timer:   idx 3, 5
        // Network: idx 2, 4
        // Client:  idx 1, 6
        // System:  idx 0
        assert_eq!(a, vec![3, 5, 2, 4, 1, 6, 0]);
    }
}
