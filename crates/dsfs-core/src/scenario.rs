//! YAML scenario schema, validation, and the encoding of client commands.
//!
//! See DD-021 for the schema design.

use serde::Deserialize;
use thiserror::Error;

/// Top-level scenario.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    /// Schema version. Must be `1`.
    pub version: u32,
    /// Human-readable name (informational; used in logs).
    pub name: String,
    /// Master seed for reproducibility.
    pub seed: u64,
    /// Cluster configuration.
    pub cluster: ClusterConfig,
    /// Network configuration.
    pub network: NetworkConfig,
    /// Ordered list of fault events.
    pub events: Vec<FaultEvent>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub nodes: u32,
    pub raft: RaftConfigYaml,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftConfigYaml {
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    pub default: LinkConfigYaml,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LinkConfigYaml {
    pub base_latency_ms: u64,
    pub jitter_ms: u64,
    pub loss_probability: f64,
    pub duplicate_probability: f64,
}

/// One fault event in the scenario.
///
/// `at_ms` is omitted for `wait_for_leader`, which fires on a state
/// change, not on time. Exactly one of the action variants is set.
///
/// Deserialization is manual because `#[serde(flatten)]` does not compose
/// with externally-tagged enums that contain struct-like variants (the
/// action keyword is what selects the variant, and serde's flatten cannot
/// see through the tag).
#[derive(Debug, Clone)]
pub struct FaultEvent {
    pub at_ms: Option<u64>,
    pub action: FaultAction,
}

#[derive(Debug, Clone)]
pub enum FaultAction {
    /// Block until a leader exists; then continue.
    WaitForLeader {},
    /// Take a node offline.
    CrashNode { node: u32 },
    /// Bring a node back online.
    RestartNode { node: u32 },
    /// Crash the node that is currently the Raft leader. If no leader
    /// exists, the fault is deferred until a leader is elected (this is
    /// not the same as `wait_for_leader`, which blocks the whole list;
    /// `crash_leader` only defers its own action).
    CrashLeader {},
    /// Restart the node that was most recently crashed by `crash_leader`.
    RestartCrashedLeader {},
    /// Partition the cluster into two groups.
    Partition { groups: Vec<Vec<u32>> },
    /// Heal all cross-group links (inverse of `partition`).
    Heal { groups: Vec<Vec<u32>> },
    /// Apply a slow factor to a node.
    SetSlow { node: u32, factor: f64 },
    /// Reset a node's slow factor to 1.0.
    ClearSlow { node: u32 },
    /// Submit a `put` command to the leader.
    SubmitWrite { key: String, value: String },
}

impl<'de> Deserialize<'de> for FaultEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as DeError;

        #[derive(Deserialize)]
        struct RawEvent {
            at_ms: Option<u64>,
            wait_for_leader: Option<serde_yaml::Value>,
            crash_node: Option<serde_yaml::Value>,
            restart_node: Option<serde_yaml::Value>,
            crash_leader: Option<serde_yaml::Value>,
            restart_crashed_leader: Option<serde_yaml::Value>,
            partition: Option<serde_yaml::Value>,
            heal: Option<serde_yaml::Value>,
            set_slow: Option<serde_yaml::Value>,
            clear_slow: Option<serde_yaml::Value>,
            submit_write: Option<serde_yaml::Value>,
        }

        fn parse_value<T: serde::de::DeserializeOwned, E: DeError>(
            v: serde_yaml::Value,
            what: &str,
        ) -> Result<T, E> {
            serde_yaml::from_value(v).map_err(|e| E::custom(format!("{what}: {e}")))
        }

        fn parse_node<E: DeError>(v: serde_yaml::Value, what: &str) -> Result<u32, E> {
            match v {
                serde_yaml::Value::Number(n) => n
                    .as_u64()
                    .and_then(|x| u32::try_from(x).ok())
                    .ok_or_else(|| {
                        E::custom(format!("{what}: node must be a positive integer"))
                    }),
                other => {
                    #[derive(Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct NodeArg {
                        node: u32,
                    }
                    let a: NodeArg = serde_yaml::from_value(other)
                        .map_err(|e| E::custom(format!("{what}: {e}")))?;
                    Ok(a.node)
                }
            }
        }

        let raw = RawEvent::deserialize(deserializer)?;

        #[derive(Deserialize, Default)]
        #[serde(deny_unknown_fields)]
        struct Empty {}

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct GroupsArg {
            groups: Vec<Vec<u32>>,
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct SlowArg {
            node: u32,
            factor: f64,
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WriteArg {
            key: String,
            value: String,
        }

        let mut present = 0u32;
        if raw.wait_for_leader.is_some() {
            present += 1;
        }
        if raw.crash_node.is_some() {
            present += 1;
        }
        if raw.restart_node.is_some() {
            present += 1;
        }
        if raw.crash_leader.is_some() {
            present += 1;
        }
        if raw.restart_crashed_leader.is_some() {
            present += 1;
        }
        if raw.partition.is_some() {
            present += 1;
        }
        if raw.heal.is_some() {
            present += 1;
        }
        if raw.set_slow.is_some() {
            present += 1;
        }
        if raw.clear_slow.is_some() {
            present += 1;
        }
        if raw.submit_write.is_some() {
            present += 1;
        }

        if present != 1 {
            return Err(D::Error::custom(format!(
                "expected exactly one fault action, got {present}"
            )));
        }

        let action = if let Some(v) = raw.wait_for_leader {
            let _: Empty = parse_value(v, "wait_for_leader")?;
            FaultAction::WaitForLeader {}
        } else if let Some(v) = raw.crash_node {
            FaultAction::CrashNode {
                node: parse_node(v, "crash_node")?,
            }
        } else if let Some(v) = raw.restart_node {
            FaultAction::RestartNode {
                node: parse_node(v, "restart_node")?,
            }
        } else if let Some(v) = raw.crash_leader {
            let _: Empty = parse_value(v, "crash_leader")?;
            FaultAction::CrashLeader {}
        } else if let Some(v) = raw.restart_crashed_leader {
            let _: Empty = parse_value(v, "restart_crashed_leader")?;
            FaultAction::RestartCrashedLeader {}
        } else if let Some(v) = raw.partition {
            let a: GroupsArg = parse_value(v, "partition")?;
            FaultAction::Partition { groups: a.groups }
        } else if let Some(v) = raw.heal {
            let a: GroupsArg = parse_value(v, "heal")?;
            FaultAction::Heal { groups: a.groups }
        } else if let Some(v) = raw.set_slow {
            let a: SlowArg = parse_value(v, "set_slow")?;
            FaultAction::SetSlow {
                node: a.node,
                factor: a.factor,
            }
        } else if let Some(v) = raw.clear_slow {
            FaultAction::ClearSlow {
                node: parse_node(v, "clear_slow")?,
            }
        } else if let Some(v) = raw.submit_write {
            let a: WriteArg = parse_value(v, "submit_write")?;
            FaultAction::SubmitWrite {
                key: a.key,
                value: a.value,
            }
        } else {
            unreachable!("present == 1");
        };

        Ok(FaultEvent {
            at_ms: raw.at_ms,
            action,
        })
    }
}

/// Validation errors.
#[derive(Debug, Error)]
pub enum ScenarioError {
    #[error("unsupported scenario version: {0} (expected 1)")]
    UnsupportedVersion(u32),

    #[error("scenario must have at least one node")]
    NoNodes,

    #[error("cluster.nodes = {nodes} is too large; max is 1000")]
    TooManyNodes { nodes: u32 },

    #[error(
        "raft.heartbeat_interval_ms ({heartbeat}) must be < raft.election_timeout_ms ({election})"
    )]
    HeartbeatNotLessThanElection { heartbeat: u64, election: u64 },

    #[error("network.base_latency_ms ({latency}) must be < raft.election_timeout_ms ({election})")]
    LatencyNotLessThanElection { latency: u64, election: u64 },

    #[error("loss_probability {0} out of [0.0, 1.0]")]
    BadLossProbability(f64),

    #[error("duplicate_probability {0} out of [0.0, 1.0]")]
    BadDuplicateProbability(f64),

    #[error("event {index}: node id {node} out of range 1..={max}")]
    NodeOutOfRange { index: usize, node: u32, max: u32 },

    #[error("event {index}: missing at_ms (only wait_for_leader may omit it)")]
    MissingAtMs { index: usize },

    #[error("event {index}: wait_for_leader must not have at_ms")]
    WaitForLeaderHasTime { index: usize },

    #[error("event {index}: partition groups must be exactly 2 groups, got {got}")]
    PartitionGroupCount { index: usize, got: usize },

    #[error("event {index}: partition groups must not overlap")]
    PartitionGroupsOverlap { index: usize },

    #[error("event {index}: partition groups must cover all 1..={max} nodes exactly once")]
    PartitionGroupsCoverage { index: usize, max: u32 },

    #[error("event {index}: slow factor {factor} must be >= 1.0")]
    BadSlowFactor { index: usize, factor: f64 },

    #[error("scenario contains a trailing wait_for_leader that can never be followed")]
    TrailingWaitForLeader,

    #[error("yaml parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),
}

impl Scenario {
    /// Parses a scenario from YAML text and validates it.
    ///
    /// # Errors
    ///
    /// Returns a [`ScenarioError`] if the YAML is malformed or fails
    /// semantic validation.
    pub fn from_yaml(text: &str) -> Result<Self, ScenarioError> {
        let parsed: Scenario = serde_yaml::from_str(text)?;
        parsed.validate()?;
        Ok(parsed)
    }

    /// Validates the scenario. Called automatically by [`Scenario::from_yaml`].
    ///
    /// # Errors
    ///
    /// Returns a [`ScenarioError`] describing the first problem found.
    pub fn validate(&self) -> Result<(), ScenarioError> {
        if self.version != 1 {
            return Err(ScenarioError::UnsupportedVersion(self.version));
        }
        if self.cluster.nodes == 0 {
            return Err(ScenarioError::NoNodes);
        }
        if self.cluster.nodes > 1000 {
            return Err(ScenarioError::TooManyNodes {
                nodes: self.cluster.nodes,
            });
        }
        let n = self.cluster.nodes;
        let raft = &self.cluster.raft;
        if raft.heartbeat_interval_ms == 0
            || raft.heartbeat_interval_ms >= raft.election_timeout_ms
        {
            return Err(ScenarioError::HeartbeatNotLessThanElection {
                heartbeat: raft.heartbeat_interval_ms,
                election: raft.election_timeout_ms,
            });
        }
        let link = &self.network.default;
        if link.base_latency_ms >= raft.election_timeout_ms {
            return Err(ScenarioError::LatencyNotLessThanElection {
                latency: link.base_latency_ms,
                election: raft.election_timeout_ms,
            });
        }
        if !(0.0..=1.0).contains(&link.loss_probability) {
            return Err(ScenarioError::BadLossProbability(link.loss_probability));
        }
        if !(0.0..=1.0).contains(&link.duplicate_probability) {
            return Err(ScenarioError::BadDuplicateProbability(
                link.duplicate_probability,
            ));
        }

        for (i, ev) in self.events.iter().enumerate() {
            let is_wait = matches!(ev.action, FaultAction::WaitForLeader {});
            match (is_wait, ev.at_ms) {
                (true, Some(_)) => return Err(ScenarioError::WaitForLeaderHasTime { index: i }),
                (false, None) => return Err(ScenarioError::MissingAtMs { index: i }),
                _ => {}
            }

            match &ev.action {
                FaultAction::CrashNode { node }
                | FaultAction::RestartNode { node }
                | FaultAction::SetSlow { node, .. }
                | FaultAction::ClearSlow { node } => {
                    Self::check_node(i, *node, n)?;
                    if let FaultAction::SetSlow { factor, .. } = &ev.action {
                        if *factor < 1.0 {
                            return Err(ScenarioError::BadSlowFactor {
                                index: i,
                                factor: *factor,
                            });
                        }
                    }
                }
                FaultAction::CrashLeader {}
                | FaultAction::RestartCrashedLeader {}
                | FaultAction::Partition { .. }
                | FaultAction::Heal { .. }
                | FaultAction::SubmitWrite { .. }
                | FaultAction::WaitForLeader {} => {
                    if let FaultAction::Partition { groups } | FaultAction::Heal { groups } =
                        &ev.action
                    {
                        Self::check_groups(i, groups, n)?;
                    }
                }
            }
        }

        if matches!(
            self.events.last().map(|e| &e.action),
            Some(FaultAction::WaitForLeader {})
        ) {
            return Err(ScenarioError::TrailingWaitForLeader);
        }

        Ok(())
    }

    fn check_node(index: usize, node: u32, max: u32) -> Result<(), ScenarioError> {
        if node < 1 || node > max {
            return Err(ScenarioError::NodeOutOfRange { index, node, max });
        }
        Ok(())
    }

    fn check_groups(index: usize, groups: &[Vec<u32>], max: u32) -> Result<(), ScenarioError> {
        if groups.len() != 2 {
            return Err(ScenarioError::PartitionGroupCount {
                index,
                got: groups.len(),
            });
        }
        let mut seen = std::collections::BTreeSet::new();
        for g in groups {
            for &id in g {
                if id < 1 || id > max {
                    return Err(ScenarioError::NodeOutOfRange { index, node: id, max });
                }
                if !seen.insert(id) {
                    return Err(ScenarioError::PartitionGroupsOverlap { index });
                }
            }
        }
        if seen.len() != max as usize {
            return Err(ScenarioError::PartitionGroupsCoverage { index, max });
        }
        Ok(())
    }
}

/// Encodes a `put` command into the byte format accepted by the engine.
///
/// Format: `b"put:" || key.len:u32_le || key_bytes || value_bytes`.
#[must_use]
pub fn encode_put(key: &str, value: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + 4 + key.len() + value.len());
    out.extend_from_slice(b"put:");
    let klen = u32::try_from(key.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&klen.to_le_bytes());
    out.extend_from_slice(key.as_bytes());
    out.extend_from_slice(value.as_bytes());
    out
}

/// Decodes a `put` command. Returns `None` if the format is not a `put`.
#[must_use]
pub fn decode_put(bytes: &[u8]) -> Option<(String, String)> {
    if bytes.len() < 8 || &bytes[0..4] != b"put:" {
        return None;
    }
    let klen = u32::from_le_bytes(bytes[4..8].try_into().ok()?) as usize;
    if bytes.len() < 8 + klen {
        return None;
    }
    let key = std::str::from_utf8(&bytes[8..8 + klen]).ok()?.to_string();
    let value = std::str::from_utf8(&bytes[8 + klen..]).ok()?.to_string();
    Some((key, value))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
version: 1
name: minimal
seed: 42
cluster:
  nodes: 3
  raft:
    election_timeout_ms: 150
    heartbeat_interval_ms: 30
network:
  default:
    base_latency_ms: 5
    jitter_ms: 2
    loss_probability: 0.0
    duplicate_probability: 0.0
events:
  - at_ms: 100
    crash_node: 1
"#;

    #[test]
    fn parses_minimal() {
        let s = Scenario::from_yaml(MINIMAL).unwrap();
        assert_eq!(s.seed, 42);
        assert_eq!(s.cluster.nodes, 3);
        assert_eq!(s.events.len(), 1);
    }

    #[test]
    fn parses_crash_leader() {
        let yaml = MINIMAL.replace("crash_node: 1", "crash_leader: {}");
        let s = Scenario::from_yaml(&yaml).unwrap();
        assert!(matches!(s.events[0].action, FaultAction::CrashLeader {}));
    }

    #[test]
    fn rejects_bad_version() {
        let bad = MINIMAL.replace("version: 1", "version: 2");
        let err = Scenario::from_yaml(&bad).unwrap_err();
        assert!(matches!(err, ScenarioError::UnsupportedVersion(2)));
    }

    #[test]
    fn rejects_missing_at_ms() {
        let bad = MINIMAL.replace("  - at_ms: 100\n    crash_node: 1", "  - crash_node: 1");
        let err = Scenario::from_yaml(&bad).unwrap_err();
        assert!(matches!(err, ScenarioError::MissingAtMs { .. }));
    }

    #[test]
    fn rejects_wait_with_at_ms() {
        let bad = MINIMAL.replace(
            "  - at_ms: 100\n    crash_node: 1",
            "  - at_ms: 100\n    wait_for_leader: {}",
        );
        let err = Scenario::from_yaml(&bad).unwrap_err();
        assert!(matches!(err, ScenarioError::WaitForLeaderHasTime { .. }));
    }

    #[test]
    fn rejects_trailing_wait() {
        let bad = MINIMAL.replace(
            "  - at_ms: 100\n    crash_node: 1",
            "  - at_ms: 100\n    crash_node: 1\n  - wait_for_leader: {}",
        );
        let err = Scenario::from_yaml(&bad).unwrap_err();
        assert!(matches!(err, ScenarioError::TrailingWaitForLeader));
    }

    #[test]
    fn rejects_bad_partition_groups() {
        let bad = MINIMAL.replace(
            "  - at_ms: 100\n    crash_node: 1",
            "  - at_ms: 100\n    partition:\n      groups:\n        - [1, 2]\n        - [3]\n        - [4]",
        );
        let err = Scenario::from_yaml(&bad).unwrap_err();
        assert!(matches!(err, ScenarioError::PartitionGroupCount { .. }));
    }

    #[test]
    fn encodes_and_decodes_put() {
        let bytes = encode_put("balance", "500");
        let (k, v) = decode_put(&bytes).unwrap();
        assert_eq!(k, "balance");
        assert_eq!(v, "500");
    }
}