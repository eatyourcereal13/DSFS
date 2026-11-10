# Architecture

DSFS is a deterministic discrete-event simulator for distributed systems,
built around a hand-written Raft implementation and a fault-injection
engine. This document explains the layering, the determinism contract,
and the reasoning behind the crate split.

## Crates

    dsfs-raft       pure Raft state machine (no I/O, no time, no peers)
    dsfs-core       simulation engine, network, nodes, fault injection
    dsfs-checker    offline analysis of JSONL logs
    dsfs-cli        binary `dsfs`; thin wrapper around the above

Dependencies form a DAG:

    dsfs-raft   (no deps)
        ^
        |
    dsfs-core
        ^        ^
        |        |
    dsfs-checker |
        ^        |
        |        |
    dsfs-cli ----+

`dsfs-raft` does not depend on `dsfs-core`; the two are connected in
`dsfs-core::node::RaftAdapter`, which translates between the Raft
domain (`PeerId`, `RaftOutput`, `TimerArm`) and the simulator domain
(`NodeId`, `Action`, `EventPayload`, `TimerId`).

## The determinism contract

> Given the same scenario (or `EngineConfig` + seed + network
> configuration), running the simulator twice must produce the same
> sequence of `LogRecord`s.

This is the load-bearing guarantee. It is what makes:
- the `dsfs replay` command meaningful,
- property/chaos tests reproducible,
- checker results trustworthy,
- bugs trivially reproducible from a JSONL log.

How determinism is achieved:

1. **Virtual time only.** The engine never reads the real clock. All
   timestamps are `VirtualTime` (nanoseconds from simulation start).
   DD-001.

2. **Total order on events.** `EventQueue` orders by
   `(time, priority, seq)`. `seq` is a monotonic counter assigned on
   insertion, so events with equal `(time, priority)` are processed in
   their insertion order. DD-002.

3. **Stream-partitioned RNG.** The simulator has no single "global
   random number generator". Every random decision draws from a named
   stream (`StreamId`) derived independently from a master seed.
   Adding a new random decision in one module does not change the
   sequence seen by any other. DD-003.

4. **No implicit nondeterminism in nodes.** `Node::handle` is a pure
   function of `(now, event_id, &EventPayload)`; it does not read
   clock, filesystem, environment. Nodes return `Vec<Action>`; the
   engine applies them. DD-010.

5. **Network decisions are in the log.** Message loss, duplication,
   and latency jitter are decisions made by `NetworkModel` and
   reflected as `MessageDropped` / `MessageDuplicated` /
   `MessageSent` records with `caused_by` references. Replay does not
   need to re-roll the dice; it can compare against the recorded
   outcomes.

## Event loop

    loop:
      injector.try_fire(now, leader_exists)   // fault events due now
      event = queue.pop()
      now = event.key.time
      injector.try_fire(now, leader_exists)
      log.record(delivery if MessageDelivered)
      actions = node.handle(now, event_id, payload)
      for action in actions:
        route_message / schedule_timer / log

`run_until(budget)` processes at most `budget` events and returns
`Completed` or `HitEventLimit`. `run()` is a convenience wrapper using
the remaining global `EngineConfig::max_events` budget.

## Fault injection

`FaultInjector` holds the scenario's ordered fault events. It does
not mutate the engine directly; it returns `InjectionOutcome`, which
the engine applies. `wait_for_leader` blocks until the engine reports
a leader; this is what makes the "crash the leader" scenario work
without hard-coding an election timeout. DD-019, DD-020.

## Checker

`dsfs-checker` reads a JSONL log and runs a small set of invariants
over it. It is not part of the engine, does not see live state, and
makes no guarantees beyond what the log itself contains. DD-022
through DD-026, and `docs/checker.md`.

## Why this split

- **`dsfs-raft` free of simulator concepts** — so Raft can be unit-
  tested without an engine, and so the safety rules are visible in
  isolation. DD-014.

- **`dsfs-core` free of `dyn` dispatch in the hot path** — nodes are
  `enum NodeImpl { Ping(PingNode), Raft(Box<RaftAdapter>) }`, not
  `Box<dyn Node>`. `RaftAdapter` is boxed to keep `NodeImpl` small
  (~32 bytes). DD-010, DD-018.

- **`dsfs-checker` free of engine dependencies** — it consumes
  `Vec<LogRecord>`; it can be used by CLI, by tests, or by a future
  tool without re-running the simulation. DD-022.

- **`dsfs-cli` free of logic** — every command is a few lines calling
  into the libraries. Exit codes are documented. DD-027, DD-030.

## Non-goals

- Real network, real processes, real time. The point of the project is
  determinism, not realism.
- Production-grade Raft. See `docs/raft.md` for the explicit list of
  omitted features (snapshots, membership change, pre-vote).
- Multi-machine deployment. The whole simulator is one process.