//! The Raft state machine.

use std::collections::BTreeSet;
use std::time::Duration;

use crate::message::RaftMessage;
use crate::rng::RaftRng;
use crate::state::{new_candidate_state, new_leader_state, PeerState};
use crate::types::{LogEntry, PeerId, Role, Term};

/// Events emitted by the Raft state machine, for logging and for the
/// consistency checker. The engine translates these into log records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RaftEvent {
    BecameFollower {
        term: Term,
        reason: FollowerReason,
    },
    BecameCandidate {
        term: Term,
    },
    BecameLeader {
        term: Term,
    },
    VotedFor {
        candidate: PeerId,
        term: Term,
    },
    ReceivedVote {
        from: PeerId,
        term: Term,
        granted: bool,
    },
    TermBumped {
        from: Term,
        to: Term,
    },
}

/// Why a peer became a follower.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FollowerReason {
    /// Started as a follower.
    Initial,
    /// Saw a message with a higher term.
    HigherTerm,
    /// Saw a valid `AppendEntries` from a leader in the same term.
    SameTermLeader,
    /// A `RequestVote` from a candidate in the same term.
    SameTermCandidate,
    /// Election timed out; became follower again (internal transition,
    /// not usually emitted for a candidate that just became leader).
    Timeout,
}

/// Instruction to (re)arm a timer.
///
/// `generation` is a monotonically increasing id: the caller must pass it
/// back to [`RaftNode::on_timer`] when the timer fires. Stale timers (from
/// a previous generation) are ignored. See DD-015.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerArm {
    pub generation: u64,
    pub after: Duration,
}

/// Result of a Raft step. Describes what to do next.
#[derive(Clone, Debug, Default)]
pub struct RaftOutput {
    /// Messages to send to peers.
    pub messages: Vec<(PeerId, RaftMessage)>,
    /// If `Some`, replace any pending timer with a new one for the given
    /// duration. The caller must schedule the timer with `TimerId(generation)`.
    pub set_timer: Option<TimerArm>,
    /// Events for logging / checking.
    pub events: Vec<RaftEvent>,
}

impl RaftOutput {
    fn empty() -> Self {
        Self::default()
    }
}

/// Configuration for a Raft node.
#[derive(Clone, Debug)]
pub struct RaftConfig {
    /// Base election timeout. The actual timeout is
    /// `[base, 2*base)`, chosen randomly each time the timer is armed.
    pub election_timeout: Duration,
    /// Heartbeat interval. Must be less than `election_timeout`.
    pub heartbeat_interval: Duration,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout: Duration::from_millis(150),
            heartbeat_interval: Duration::from_millis(30),
        }
    }
}

/// The Raft state machine.
pub struct RaftNode {
    // Identity
    self_id: PeerId,
    peers: Vec<PeerId>,
    config: RaftConfig,

    // Persistent state (per Raft spec). On Stage 5, persistence is
    // in-memory only; the simulation log provides the durable trail.
    current_term: Term,
    voted_for: Option<PeerId>,

    // Volatile state
    state: PeerState,
    /// Incremented each time the timer is (re)armed. `on_timer` receives
    /// the generation of the timer that fired; if it doesn't match the
    /// current one, the expiry is stale and is ignored.
    timer_generation: u64,
    /// If the next `on_start` should arm the timer (initial start).
    started: bool,

    // RNG
    rng: Box<dyn RaftRng>,

    // Stage 6 placeholders
    pub(crate) log: Vec<LogEntry>,
    #[allow(dead_code)]
    pub(crate) commit_index: u64,
}

impl RaftNode {
    /// Creates a new Raft node.
    ///
    /// `peers` must contain all cluster members **including** `self_id`.
    #[must_use]
    pub fn new(
        self_id: PeerId,
        peers: Vec<PeerId>,
        config: RaftConfig,
        rng: Box<dyn RaftRng>,
    ) -> Self {
        debug_assert!(peers.contains(&self_id), "peers list must contain self_id");
        Self {
            self_id,
            peers,
            config,
            current_term: Term::ZERO,
            voted_for: None,
            state: PeerState::Follower,
            timer_generation: 0,
            started: false,
            rng,
            log: Vec::new(),
            commit_index: 0,
        }
    }

    /// Returns the current role.
    #[must_use]
    pub fn role(&self) -> Role {
        self.state.role()
    }

    /// Returns the current term.
    #[must_use]
    pub fn term(&self) -> Term {
        self.current_term
    }

    /// Returns the current leader, if known (only meaningful when the node
    /// is a follower and has received an `AppendEntries` in this term).
    #[must_use]
    pub fn voted_for(&self) -> Option<PeerId> {
        self.voted_for
    }

    /// Called exactly once, when the node starts. Arms the initial
    /// election timer.
    pub fn on_start(&mut self) -> RaftOutput {
        if self.started {
            return RaftOutput::empty();
        }
        self.started = true;
        let mut out = RaftOutput::empty();
        self.arm_timer(&mut out);
        out
    }

    /// Handles a timer expiration.
    ///
    /// `generation` is the generation id the engine associated with the
    /// `TimerFired` event. If it doesn't match `self.timer_generation`,
    /// the timer was superseded and is ignored.
    pub fn on_timer(&mut self, generation: u64) -> RaftOutput {
        if generation != self.timer_generation {
            return RaftOutput::empty();
        }
        let mut out = RaftOutput::empty();
        match self.state.role() {
            Role::Follower | Role::Candidate => {
                self.become_candidate(&mut out);
            }
            Role::Leader => {
                // Leader does not have election timeouts (heartbeats are
                // driven by a different timer on Stage 6; on Stage 5,
                // the leader simply re-arms a heartbeat timer).
                self.broadcast_heartbeat(&mut out);
                self.arm_timer(&mut out); // re-arm for next heartbeat
            }
        }
        out
    }

    /// Handles a message from `from`.
    pub fn on_message(&mut self, from: PeerId, msg: RaftMessage) -> RaftOutput {
        match msg {
            RaftMessage::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.on_request_vote(from, term, candidate_id, last_log_index, last_log_term),
            RaftMessage::RequestVoteReply { term, vote_granted } => {
                self.on_request_vote_reply(from, term, vote_granted)
            }
            RaftMessage::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.on_append_entries(
                from,
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            ),
            RaftMessage::AppendEntriesReply {
                term,
                success,
                match_index,
            } => self.on_append_entries_reply(from, term, success, match_index),
        }
    }

    // ---- Internals ----

    fn arm_timer(&mut self, out: &mut RaftOutput) {
        self.timer_generation = self.timer_generation.wrapping_add(1);
        let base = self.config.election_timeout.as_millis() as u64;
        let range = base.max(1);
        let extra = self.rng.gen_range_u64(0, range);
        out.set_timer = Some(TimerArm {
            generation: self.timer_generation,
            after: Duration::from_millis(base + extra),
        });
    }

    fn arm_heartbeat_timer(&mut self, out: &mut RaftOutput) {
        self.timer_generation = self.timer_generation.wrapping_add(1);
        out.set_timer = Some(TimerArm {
            generation: self.timer_generation,
            after: self.config.heartbeat_interval,
        });
    }

    fn bump_term(&mut self, new_term: Term, out: &mut RaftOutput) {
        if new_term > self.current_term {
            let from = self.current_term;
            self.current_term = new_term;
            self.voted_for = None;
            self.state = PeerState::Follower;
            out.events
                .push(RaftEvent::TermBumped { from, to: new_term });
        }
    }

    fn become_follower(&mut self, reason: FollowerReason, out: &mut RaftOutput) {
        self.state = PeerState::Follower;
        out.events.push(RaftEvent::BecameFollower {
            term: self.current_term,
            reason,
        });
    }

    fn become_candidate(&mut self, out: &mut RaftOutput) {
        self.current_term = self.current_term.next();
        self.voted_for = Some(self.self_id);
        let mut votes = BTreeSet::new();
        votes.insert(self.self_id);
        let term = self.current_term;
        self.state = new_candidate_state(votes);
        out.events.push(RaftEvent::BecameCandidate { term });

        // Arm the election timer for the new election.
        self.arm_timer(out);

        // Send RequestVote to all peers.
        for &peer in &self.peers {
            if peer == self.self_id {
                continue;
            }
            out.messages.push((
                peer,
                RaftMessage::RequestVote {
                    term,
                    candidate_id: self.self_id,
                    last_log_index: self.last_log_index(),
                    last_log_term: self.last_log_term(),
                },
            ));
        }
        // Check if we already have a majority (single-node cluster).
        self.maybe_become_leader(out);
    }

    fn maybe_become_leader(&mut self, out: &mut RaftOutput) {
        if let PeerState::Candidate { votes } = &self.state {
            let majority = self.peers.len() / 2 + 1;
            if votes.len() >= majority {
                let term = self.current_term;
                self.state = new_leader_state();
                out.events.push(RaftEvent::BecameLeader { term });
                self.broadcast_heartbeat(out);
                self.arm_heartbeat_timer(out);
            }
        }
    }

    fn broadcast_heartbeat(&mut self, out: &mut RaftOutput) {
        if self.state.role() != Role::Leader {
            return;
        }
        let term = self.current_term;
        for &peer in &self.peers {
            if peer == self.self_id {
                continue;
            }
            out.messages.push((
                peer,
                RaftMessage::AppendEntries {
                    term,
                    leader_id: self.self_id,
                    prev_log_index: self.last_log_index(),
                    prev_log_term: self.last_log_term(),
                    entries: Vec::new(),
                    leader_commit: self.commit_index,
                },
            ));
        }
    }

    fn last_log_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    fn last_log_term(&self) -> Term {
        self.log.last().map(|e| e.term).unwrap_or(Term::ZERO)
    }

    fn on_request_vote(
        &mut self,
        from: PeerId,
        term: Term,
        candidate_id: PeerId,
        last_log_index: u64,
        last_log_term: Term,
    ) -> RaftOutput {
        let mut out = RaftOutput::empty();

        // If message term > current term, bump and become follower.
        if term > self.current_term {
            self.bump_term(term, &mut out);
            self.become_follower(FollowerReason::HigherTerm, &mut out);
        }

        let mut vote_granted = false;
        if term == self.current_term {
            // Grant if: haven't voted, or already voted for this candidate,
            // AND candidate's log is at least as up-to-date as ours (§5.4.1).
            let can_vote = self.voted_for.is_none() || self.voted_for == Some(candidate_id);
            let log_ok = last_log_term > self.last_log_term()
                || (last_log_term == self.last_log_term()
                    && last_log_index >= self.last_log_index());
            if can_vote && log_ok {
                self.voted_for = Some(candidate_id);
                vote_granted = true;
                out.events.push(RaftEvent::VotedFor {
                    candidate: candidate_id,
                    term: self.current_term,
                });
                // Voting resets the election timer (a known leader is likely
                // to emerge; do not preempt it with our own candidacy).
                self.arm_timer(&mut out);
            }
        }

        out.events.push(RaftEvent::ReceivedVote {
            from,
            term: self.current_term,
            granted: vote_granted,
        });

        out.messages.push((
            from,
            RaftMessage::RequestVoteReply {
                term: self.current_term,
                vote_granted,
            },
        ));
        out
    }

    fn on_request_vote_reply(
        &mut self,
        from: PeerId,
        term: Term,
        vote_granted: bool,
    ) -> RaftOutput {
        let mut out = RaftOutput::empty();

        if term > self.current_term {
            self.bump_term(term, &mut out);
            self.become_follower(FollowerReason::HigherTerm, &mut out);
            return out;
        }

        // Only process replies in our current term if we are still a candidate.
        if term != self.current_term || self.state.role() != Role::Candidate {
            return out;
        }

        if vote_granted {
            if let PeerState::Candidate { votes } = &mut self.state {
                votes.insert(from);
            }
            self.maybe_become_leader(&mut out);
        }
        out
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append_entries(
        &mut self,
        from: PeerId,
        term: Term,
        leader_id: PeerId,
        _prev_log_index: u64,
        _prev_log_term: Term,
        _entries: Vec<LogEntry>,
        _leader_commit: u64,
    ) -> RaftOutput {
        let mut out = RaftOutput::empty();

        if term > self.current_term {
            self.bump_term(term, &mut out);
        }

        if term < self.current_term {
            // Stale leader; reply with our term so it can step down.
            out.messages.push((
                from,
                RaftMessage::AppendEntriesReply {
                    term: self.current_term,
                    success: false,
                    match_index: 0,
                },
            ));
            return out;
        }

        // Valid leader in our term. Reset election timer, become follower.
        if self.state.role() != Role::Follower {
            self.become_follower(FollowerReason::SameTermLeader, &mut out);
        }
        // Confirm the leader_id matches the sender (Raft requires this).
        debug_assert_eq!(from, leader_id, "AppendEntries leader_id must match sender");
        self.arm_timer(&mut out);

        out.messages.push((
            from,
            RaftMessage::AppendEntriesReply {
                term: self.current_term,
                success: true,
                match_index: self.last_log_index(),
            },
        ));
        out
    }

    fn on_append_entries_reply(
        &mut self,
        _from: PeerId,
        term: Term,
        _success: bool,
        _match_index: u64,
    ) -> RaftOutput {
        let mut out = RaftOutput::empty();

        if term > self.current_term {
            self.bump_term(term, &mut out);
            self.become_follower(FollowerReason::HigherTerm, &mut out);
        }
        // Stage 5 ignores replication replies; Stage 6 will use them.
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::fixed::CyclicRng;

    fn node_with_fixed_rng(id: u32, peers: Vec<u32>, rng_values: Vec<u64>) -> RaftNode {
        RaftNode::new(
            PeerId(id),
            peers.into_iter().map(PeerId).collect(),
            RaftConfig {
                election_timeout: Duration::from_millis(100),
                heartbeat_interval: Duration::from_millis(20),
            },
            Box::new(CyclicRng::new(rng_values)),
        )
    }

    #[test]
    fn starts_as_follower() {
        let n = node_with_fixed_rng(1, vec![1, 2, 3], vec![0]);
        assert_eq!(n.role(), Role::Follower);
        assert_eq!(n.term(), Term::ZERO);
        assert_eq!(n.voted_for(), None);
    }

    #[test]
    fn on_start_arms_timer() {
        let mut n = node_with_fixed_rng(1, vec![1, 2, 3], vec![10]);
        let out = n.on_start();
        assert!(out.set_timer.is_some());
        let arm = out.set_timer.unwrap();
        assert!(arm.after >= Duration::from_millis(100));
        assert!(arm.after < Duration::from_millis(200));
        assert!(out.messages.is_empty());
    }

    #[test]
    fn on_start_is_idempotent() {
        let mut n = node_with_fixed_rng(1, vec![1, 2, 3], vec![10]);
        let _ = n.on_start();
        let out2 = n.on_start();
        assert!(out2.set_timer.is_none());
        assert!(out2.messages.is_empty());
    }

    #[test]
    fn timeout_triggers_candidacy() {
        let mut n = node_with_fixed_rng(1, vec![1, 2, 3], vec![10, 10, 10, 10]);
        let _ = n.on_start();
        let out = n.on_timer(1);
        assert_eq!(n.role(), Role::Candidate);
        assert_eq!(n.term(), Term(1));
        assert_eq!(n.voted_for(), Some(PeerId(1)));
        assert!(out
            .events
            .iter()
            .any(|e| matches!(e, RaftEvent::BecameCandidate { .. })));
        let request_votes = out
            .messages
            .iter()
            .filter(|(_, m)| matches!(m, RaftMessage::RequestVote { .. }))
            .count();
        assert_eq!(request_votes, 2, "must request vote from 2 peers");
    }

    #[test]
    fn stale_timer_is_ignored() {
        let mut n = node_with_fixed_rng(1, vec![1, 2, 3], vec![10]);
        let _ = n.on_start(); // generation becomes 1
        let out = n.on_timer(0);
        assert_eq!(n.role(), Role::Follower);
        assert!(out.messages.is_empty());
    }

    #[test]
    fn majority_votes_make_leader() {
        let mut n = node_with_fixed_rng(1, vec![1, 2, 3], vec![10, 10, 10, 10]);
        let _ = n.on_start();
        let _ = n.on_timer(1);
        assert_eq!(n.role(), Role::Candidate);
        // Receive two votes from peers 2 and 3 — majority is 2.
        let _ = n.on_message(
            PeerId(2),
            RaftMessage::RequestVoteReply {
                term: Term(1),
                vote_granted: true,
            },
        );
        assert_eq!(n.role(), Role::Leader);
    }

    #[test]
    fn candidate_steps_down_on_higher_term() {
        let mut n = node_with_fixed_rng(1, vec![1, 2, 3], vec![10, 10, 10, 10]);
        let _ = n.on_start();
        let _ = n.on_timer(1);
        assert_eq!(n.role(), Role::Candidate);
        let _ = n.on_message(
            PeerId(2),
            RaftMessage::RequestVoteReply {
                term: Term(5),
                vote_granted: false,
            },
        );
        assert_eq!(n.role(), Role::Follower);
        assert_eq!(n.term(), Term(5));
    }

    #[test]
    fn request_vote_granted_when_not_voted() {
        let mut n = node_with_fixed_rng(2, vec![1, 2, 3], vec![10]);
        let out = n.on_message(
            PeerId(1),
            RaftMessage::RequestVote {
                term: Term(1),
                candidate_id: PeerId(1),
                last_log_index: 0,
                last_log_term: Term::ZERO,
            },
        );
        assert_eq!(n.term(), Term(1));
        assert_eq!(n.voted_for(), Some(PeerId(1)));
        let reply = out
            .messages
            .iter()
            .find(|(_, m)| matches!(m, RaftMessage::RequestVoteReply { .. }))
            .unwrap();
        match reply.1 {
            RaftMessage::RequestVoteReply { vote_granted, .. } => assert!(vote_granted),
            _ => unreachable!(),
        }
    }

    #[test]
    fn request_vote_denied_if_already_voted_for_other() {
        let mut n = node_with_fixed_rng(2, vec![1, 2, 3], vec![10]);
        let _ = n.on_message(
            PeerId(1),
            RaftMessage::RequestVote {
                term: Term(1),
                candidate_id: PeerId(1),
                last_log_index: 0,
                last_log_term: Term::ZERO,
            },
        );
        let out = n.on_message(
            PeerId(3),
            RaftMessage::RequestVote {
                term: Term(1),
                candidate_id: PeerId(3),
                last_log_index: 0,
                last_log_term: Term::ZERO,
            },
        );
        let reply = out
            .messages
            .iter()
            .find(|(_, m)| matches!(m, RaftMessage::RequestVoteReply { .. }))
            .unwrap();
        match reply.1 {
            RaftMessage::RequestVoteReply { vote_granted, .. } => assert!(!vote_granted),
            _ => unreachable!(),
        }
        assert_eq!(n.voted_for(), Some(PeerId(1)));
    }

    #[test]
    fn higher_term_append_entries_resets_to_follower() {
        let mut n = node_with_fixed_rng(1, vec![1, 2, 3], vec![10, 10, 10, 10]);
        let _ = n.on_start();
        let _ = n.on_timer(1); // become candidate
        assert_eq!(n.role(), Role::Candidate);
        let _ = n.on_message(
            PeerId(2),
            RaftMessage::AppendEntries {
                term: Term(5),
                leader_id: PeerId(2),
                prev_log_index: 0,
                prev_log_term: Term::ZERO,
                entries: vec![],
                leader_commit: 0,
            },
        );
        assert_eq!(n.role(), Role::Follower);
        assert_eq!(n.term(), Term(5));
    }
}
