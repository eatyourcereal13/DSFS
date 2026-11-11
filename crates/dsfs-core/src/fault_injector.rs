//! Runtime interpreter for scenario fault events.

use std::collections::VecDeque;

use crate::node::NodeId;
use crate::scenario::{encode_put, FaultAction, Scenario};
use crate::time::{Duration, VirtualTime};

/// Outcome of a fault event that the engine must apply.
#[derive(Debug, Clone)]
pub enum InjectionOutcome {
    /// The `wait_for_leader` event was satisfied; nothing to apply.
    WaitSatisfied,
    /// Take a node offline.
    Crash(NodeId),
    /// Bring a node back online.
    Restart(NodeId),
    /// Crash whichever node is currently the leader. The engine resolves
    /// the leader at apply time; if none exists, the outcome is a no-op.
    CrashLeader,
    /// Restart the node that was most recently crashed by `CrashLeader`.
    RestartCrashedLeader,
    /// Cut all cross-links between two groups.
    Partition {
        group_a: Vec<NodeId>,
        group_b: Vec<NodeId>,
    },
    /// Restore all cross-links between two groups.
    Heal {
        group_a: Vec<NodeId>,
        group_b: Vec<NodeId>,
    },
    /// Apply a slow factor to a node.
    SetSlow { node: NodeId, factor: f64 },
    /// Reset a slow factor.
    ClearSlow(NodeId),
    /// Submit a command to the current leader.
    Submit { command: Vec<u8> },
}

#[derive(Debug, Clone)]
struct Pending {
    at: Option<VirtualTime>,
    action: FaultAction,
    index: usize,
}

/// Interpreter over a scenario. See DD-020.
#[derive(Debug)]
pub struct FaultInjector {
    pending: VecDeque<Pending>,
    blocked: bool,
}

impl FaultInjector {
    #[must_use]
    pub fn from_scenario(scenario: &Scenario) -> Self {
        let pending: VecDeque<Pending> = scenario
            .events
            .iter()
            .enumerate()
            .map(|(i, ev)| Pending {
                at: ev.at_ms.map(VirtualTime::from_millis),
                action: ev.action.clone(),
                index: i,
            })
            .collect();
        let blocked = matches!(
            pending.front().map(|p| &p.action),
            Some(FaultAction::WaitForLeader {})
        );
        Self { pending, blocked }
    }

    #[must_use]
    pub fn is_blocked(&self) -> bool {
        self.blocked
    }

    #[must_use]
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    #[must_use]
    pub fn next_event_time(&self) -> Option<VirtualTime> {
        self.pending.front().and_then(|p| p.at)
    }

    pub fn try_fire(
        &mut self,
        now: VirtualTime,
        leader_exists: bool,
    ) -> Option<(usize, InjectionOutcome)> {
        let head = self.pending.front()?;
        match head.at {
            None => {
                self.blocked = true;
                if !leader_exists {
                    return None;
                }
                let p = self.pending.pop_front()?;
                self.blocked = matches!(
                    self.pending.front().map(|x| &x.action),
                    Some(FaultAction::WaitForLeader {})
                );
                Some((p.index, InjectionOutcome::WaitSatisfied))
            }
            Some(t) if now >= t => {
                let p = self.pending.pop_front()?;
                self.blocked = false;
                let outcome = outcome_from_action(&p.action)?;
                Some((p.index, outcome))
            }
            Some(_) => None,
        }
    }
}

fn outcome_from_action(action: &FaultAction) -> Option<InjectionOutcome> {
    match action {
        FaultAction::WaitForLeader {} => None,
        FaultAction::CrashNode { node } => Some(InjectionOutcome::Crash(NodeId(*node))),
        FaultAction::RestartNode { node } => Some(InjectionOutcome::Restart(NodeId(*node))),
        FaultAction::CrashLeader {} => Some(InjectionOutcome::CrashLeader),
        FaultAction::RestartCrashedLeader {} => Some(InjectionOutcome::RestartCrashedLeader),
        FaultAction::Partition { groups } => Some(InjectionOutcome::Partition {
            group_a: groups[0].iter().map(|n| NodeId(*n)).collect(),
            group_b: groups[1].iter().map(|n| NodeId(*n)).collect(),
        }),
        FaultAction::Heal { groups } => Some(InjectionOutcome::Heal {
            group_a: groups[0].iter().map(|n| NodeId(*n)).collect(),
            group_b: groups[1].iter().map(|n| NodeId(*n)).collect(),
        }),
        FaultAction::SetSlow { node, factor } => Some(InjectionOutcome::SetSlow {
            node: NodeId(*node),
            factor: *factor,
        }),
        FaultAction::ClearSlow { node } => Some(InjectionOutcome::ClearSlow(NodeId(*node))),
        FaultAction::SubmitWrite { key, value } => Some(InjectionOutcome::Submit {
            command: encode_put(key, value),
        }),
    }
}

/// Converts milliseconds to a virtual-time `Duration`.
#[must_use]
pub fn ms(ms: u64) -> Duration {
    Duration::from_millis(ms)
}