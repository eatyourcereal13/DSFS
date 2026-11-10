//! Network message types.

use dsfs_raft::RaftMessage;

/// A message that can travel between virtual nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// "Are you alive?" — answered with `Pong`.
    Ping,
    /// Reply to `Ping`.
    Pong,
    /// Raft protocol message.
    Raft(RaftMessage),
}
