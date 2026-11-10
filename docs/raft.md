# Raft implementation — scope and limitations

This document describes what our Raft implementation covers and — just as
importantly — what it deliberately does not. It is a **partial** implementation
of the Raft paper (Ongaro & Ousterhout, "In Search of an Understandable
Consensus Algorithm", USENIX ATC 2014), sufficient for the simulator's
goals but not a production-grade library.

## Implemented (v1)

### Stage 5 (leader election)

- Persistent state: `currentTerm`, `votedFor`.
- Volatile state: `role`, election timer, vote set.
- Randomized election timeout in `[base, 2*base)`.
- `RequestVote` RPC with the "log at least as up-to-date" check (§5.4.1).
- `RequestVoteReply`.
- `AppendEntries` as heartbeat (empty entries).
- `AppendEntriesReply`.
- Term handling per §5.1: higher-term messages cause step-down.
- At most one leader per term (verified by a property test in
  `tests/raft_election.rs::no_two_leaders_in_same_term`).
- Leader failure / single-node cluster / majority election.

### Stage 6 (planned)

- Log replication: `AppendEntries` with entries.
- Commit index and applied-to-state-machine.
- Conflict resolution: `prev_log_index/prev_log_term` mismatch → step back.
- `next_index[]` / `match_index[]` management.
- Recovery: rejoining follower synchronises its log.

## NOT implemented (and will not be in v1)

- **Snapshots / log compaction** (§7). The simulation horizon is bounded,
  so log growth is not a concern.
- **Cluster membership changes** (§6). The cluster is static for the
  lifetime of a simulation.
- **Pre-vote / check-quorum** (extensions, not in the original paper).
- **Persistence to disk.** `dsfs-raft` keeps persistent state in memory.
  The `dsfs-core` event log is the durable trail: a replay reconstructs
  any past state exactly.
- **Pipelining / batching.** One `AppendEntries` per heartbeat; one entry
  per client request. Simpler, and Raft's safety does not depend on it.
- **Client sessions / linearizability of client reads.** Stage 6 gives
  "committed writes are durable"; reads always go through the leader but
  we do not implement a read-index protocol.

## Relationship to the paper

- Section references: §5.1 (terms, step-down), §5.2 (leader election),
  §5.3 (log replication, Stage 6), §5.4 (safety), Figure 2 (rules).
- We follow the **safety** rules strictly. Where we deviate in
  performance-oriented rules (batching, pre-vote), the deviation cannot
  affect the invariants checked by the consistency checker (Stage 8).

## Non-goals in code layout

- `dsfs-raft` does **not** depend on `dsfs-core`; the bridge is
  `dsfs-core/src/raft_adapter.rs`. This lets the Raft state machine be
  tested in isolation, without a simulation engine.