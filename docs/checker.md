# Consistency Checker

The checker (`dsfs-checker`) analyzes a simulation log offline and reports
violations of Raft invariants.

## Guarantees and non-guarantees

The checker verifies the **observable behavior** in a log against a set of
invariants. It does **not** prove that the Raft implementation is
mathematically correct; it catches concrete classes of bugs that manifest
as rule violations in the log.

**What it catches well:**
- Two leaders elected in the same term (a fundamental safety violation).
- Different commands applied at the same index (breaks state-machine
  consistency).
- A log entry changing its term without an intervening truncate (breaks
  log matching).
- A commit event where fewer than a majority applied the entry.
- Node recovery losing applied indexes.

**What it does not catch:**
- Raft bugs that never manifest as the above violations in the recorded
  events (e.g., a subtle safety violation hidden behind the same
  content at the same index, or a lost AppendEntries that was
  re-sent).
- Bugs that depend on state the checker does not reconstruct (e.g., the
  internal `next_index` of a leader).
- A commit event where a majority *did* apply the entry but only much
  later than expected. We only check eventual majority, not timing.

**Partial logs.** If the JSONL file is truncated or missing lines, the
checker may under-report. It does not attempt to detect truncation. In
future versions, the meta line may include a `complete: bool` marker.

## Invariants

| Name | Description | Catches |
|---|---|---|
| `single_leader_per_term` | At most one `BecameLeader` per term | Split-brain elections |
| `no_lost_committed_entries` | Same `index` applied with same `(term, command)` on all nodes | Divergent apply |
| `log_matching` | A node's `(index, term)` never changes without an intervening truncate | Log divergence |
| `majority_commit_rule` | Every `CommitAdvanced to=N` has ≥ majority nodes applying `N` | Commit without quorum |
| `recovery_consistency` | Applied indexes never decrease across restart | Log loss on recovery |

## Usage

```rust
use dsfs_checker::read_jsonl;
use dsfs_checker::invariants::run;

let text = std::fs::read("results/latest.jsonl")?;
let out = read_jsonl(&text[..])?;
let report = run(&out.records);
if report.is_ok() {
    println!("all invariants satisfied");
} else {
    for v in &report.violations {
        eprintln!("[{}] t={:?} {}", v.invariant, v.time, v.context);
    }
}
```

## Limitations

See the doc-comments in crates/dsfs-checker/src/invariants.rs for
specific per-check caveats.
text


### `docs/design-decisions.md` — добавить DD-022..DD-026

---

## DD-022. Checker is a separate crate

`dsfs-checker` depends on `dsfs-core` (for `LogRecord`), but the reverse
is not true. It can be used by the future CLI (`dsfs validate`) and by
tests independently of the engine. JSONL reading uses `serde_json`,
which we did not want to pull into `dsfs-core` (only the writer needs
serialization, and `serde_json::to_writer` is already available through
`serde` feature).

## DD-023. JSONL: writer in `dsfs-core`, reader in `dsfs-checker`

`dsfs_core::log::JsonlLog<W>` implements `EventLog` and writes one JSON
object per line, prefixed by a meta line. The reader lives in
`dsfs-checker::reader` so that `dsfs-core` does not depend on
`serde_json`'s deserialization machinery beyond `LogRecord`.

## DD-024. Checker runs independent checks

Five checks (`single_leader_per_term`, `no_lost_committed_entries`,
`log_matching`, `majority_commit_rule`, `recovery_consistency`), each
maintaining its own per-check state. This keeps each check small and
testable; a single merged pass would conflate concerns and make new
checks harder to add.

## DD-025. `RaftEvent::EntryAppended` per entry

Follower logs entries individually (`RaftEntryAppended { index, term }`),
not as ranges, so the checker has a full `(node, index) -> term` map.
Range events would lose term information per entry.

## DD-026. JSONL schema has a version

The first line of a JSONL file is a meta object with `schema` and
`sim_version`. This allows future `dsfs validate` to warn if a log was
produced by an incompatible version. The schema version is separate
from `SIM_VERSION` (which is the crate version); they serve different
purposes.