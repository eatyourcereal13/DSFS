//! Network message types.
//!
//! Stage 2 uses `Ping`/`Pong` only. Stage 5 will add a `Raft(RaftMessage)`
//! variant; Stage 7 may add control messages for fault injection.

/// A message that can travel between virtual nodes.
///
/// `Message` is `Copy` because Stage 2 variants are trivial; later stages
/// that need heap payloads will drop `Copy` and add `Clone`. We do not
/// preemptively add `Serialize` here: serialization is a concern of the
/// log, not of in-memory transport.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// "Are you alive?" — answered with `Pong`.
    Ping,
    /// Reply to `Ping`.
    Pong,
}
