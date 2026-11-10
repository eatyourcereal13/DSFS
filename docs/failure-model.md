# Failure model

DSFS models a specific, well-defined subset of distributed-systems
failures. This document is the contract between the user (scenario
author) and the simulator about what is and is not modelled.

## Model in one sentence

Asynchronous message passing between a fixed set of processes, with
unbounded but finite message delay, optional message loss, optional
duplication, optional partitions; processes can crash and restart;
processes have no shared memory and no shared clock.

## What is modelled

### Network

- **Per-direction latency.** `LinkConfig.base_latency` in both
  directions is configurable separately.
- **Jitter.** Uniform extra delay in `[0, jitter)`. If `jitter >=
  base_latency`, reordering emerges naturally, which is the only
  correct way to model reordering without breaking replay (DD-008).
- **Loss.** Per-direction `loss_probability` in `[0, 1]`.
- **Duplication.** Per-direction `duplicate_probability` in `[0, 1]`.
  Duplicates arrive at the same `VirtualTime` as the original
  (DD-006); Raft's idempotency handles this.
- **Partition.** Cutting all messages between two groups of nodes, in
  both directions. Health: `heal` restores.

### Nodes

- **Crash / restart.** An offline node processes no events (messages
  or timer fires). On restart, it becomes available again; Raft
  recovery is exercised on the next `AppendEntries`.
- **Slow.** `slow_factor > 1.0` multiplies incoming message delay by
  `1 + (slow_factor - 1) * 1ms`. This models a node that takes longer
  to process incoming messages. Not a CPU throttle; there is no CPU
  (DD-011).

### Time

- **Virtual.** `VirtualTime` in nanoseconds. No real time anywhere in
  the engine (DD-001).

### Fault injection

- **Scheduled** by `at_ms` in the scenario.
- **Gated** by `wait_for_leader`, which blocks until the engine sees
  any active leader.

## What is NOT modelled

- **Clock skew / drift.** Virtual time is global and monotonic. All
  nodes share one clock. This is a simplification: Raft tolerates
  clock skew but only insofar as timeouts are not synchronised.
  Our election timeouts are randomised, which is a proxy; the Raft
  safety rules do not depend on clock synchronisation anyway.
- **Disk failures.** A crashed node forgets its state on restart?
  No, actually — our `RaftNode` keeps its log and term in memory, and
  `set_availability(Online)` does not reset them. So our crash is
  "process paused" rather than "process killed". This is a
  simplification that lets us test Raft's recovery without modelling
  WALs.
- **Byte-level message corruption.** Messages are values; they don't
  corrupt.
- **Asymmetric partitions with delay between cut and restore.** A
  partition is instantaneous in virtual time; the engine does not
  model "link cut in transit".
- **Partial partition recovery.** `heal` restores all cross-links
  between two groups at once. No per-link `heal` from a scenario
  (though the underlying `FullNetwork::heal` API exists).
- **CPU / memory pressure.** Slow factor is a proxy, not a
  simulation of resource exhaustion.
- **Adversarial behaviour.** All nodes follow the Raft spec.
- **Byzantine faults.** Not modelled; Raft does not tolerate them.

## Assumptions the simulator makes

- The scenario is well-formed: validated by `Scenario::validate`.
- Cluster size is static for the lifetime of the simulation (Raft
  membership change is out of scope; `docs/raft.md`).
- All nodes share the same `RaftConfig`.
- The scenario author lists events in execution order;
  `FaultInjector` fires them in list order, not sorted by `at_ms`.
  (Validated to be at least internally consistent; not
  monotonic-checked, but events with earlier `at_ms` after later ones
  are the author's bug.)

## Properties guaranteed under this model

Raft safety (§5.4 of the paper), modulo the omitted features:

- **Election Safety.** At most one leader per term. Enforced by
  voting rules; verified by checker `single_leader_per_term`.
- **Leader Append-Only.** A leader never overwrites or deletes entries
  in its own log.
- **Log Matching.** If two logs contain an entry with the same
  `(index, term)`, then all preceding entries are identical.
- **Leader Completeness.** If a log entry is committed in a given
  term, then it is present in the logs of all leaders of higher
  terms.
- **State Machine Safety.** If a server has applied a log entry at
  index `i`, no other server will ever apply a different entry at
  the same index.

The checker (`docs/checker.md`) verifies the *observable* version of
these properties. It is not a proof.

## Failure injection as a testing strategy

The purpose of fault injection is not to convince yourself that the
system is correct; it is to *falsify* properties quickly. If a
randomized scenario produces a violation, you have a counterexample
in the log, replayable from the seed alone. This is the core value
of DSFS: failures are reproducible.