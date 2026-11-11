//! Tauri backend for the DSFS desktop app.

mod tour;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use serde::Serialize;
use tauri::State;

use dsfs_core::engine::{EngineConfig, FullNetwork, LinkConfig, SimulationEngine};
use dsfs_core::log::{EventLog, InMemoryLog, LogEventKind, LogRecord};
use dsfs_core::network::NetworkModel;
use dsfs_core::node::{Availability, NodeId, NodeImpl, RaftAdapter};
use dsfs_core::rng::{RaftStreamPurpose, StreamId, StreamRng};
use dsfs_core::scenario::{encode_put, FaultAction, Scenario};
use dsfs_core::time::Duration as VDuration;
use dsfs_raft::{PeerId, RaftConfig, RaftNode, Role};

use crate::tour::{TourAction, TourKind, TourState, WaitCondition};

struct AppState {
    inner: Arc<Mutex<EngineState>>,
}

struct EngineState {
    scenario: Option<Scenario>,
    engine: Option<SimulationEngine<InMemoryLog, FullNetwork>>,
    paused: bool,
    speed: f64,
    applied_fault_count: usize,
    last_snapshot: Option<SimSnapshot>,
    tour: TourState,
    last_crashed_leader: Option<NodeId>,
}

impl Default for EngineState {
    fn default() -> Self {
        Self {
            scenario: None,
            engine: None,
            paused: true,
            speed: 1.0,
            applied_fault_count: 0,
            last_snapshot: None,
            tour: TourState::inactive(),
            last_crashed_leader: None,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct SimSnapshot {
    pub scenario_name: String,
    pub seed: u64,
    pub running: bool,
    pub paused: bool,
    pub speed: f64,
    pub time_ms: u64,
    pub nodes: Vec<NodeView>,
    pub recent_events: Vec<EventView>,
    pub faults: Vec<FaultView>,
    pub stats: StatsView,
    pub tour: TourState,
}

#[derive(Clone, Debug, Serialize)]
pub struct NodeView {
    pub id: u32,
    pub role: String,
    pub availability: String,
    pub term: u64,
    pub log_len: u64,
    pub commit: u64,
    pub voted_for: Option<u32>,
    pub is_leader: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct EventView {
    pub event_id: u64,
    pub time_ms: u64,
    pub kind: String,
    pub kind_short: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct FaultView {
    pub index: usize,
    pub at_ms: Option<u64>,
    pub description: String,
    pub completed: bool,
}

#[derive(Clone, Debug, Serialize, Default)]
pub struct StatsView {
    pub messages_sent: usize,
    pub messages_dropped: usize,
    pub messages_duplicated: usize,
    pub leaders_elected: usize,
    pub applied: usize,
    pub events_processed: u64,
}

// ---- Scenario discovery ----

fn scenarios_dir() -> Option<PathBuf> {
    let mut candidates: Vec<PathBuf> = Vec::new();

    candidates.push(PathBuf::from("configs/scenarios"));

    {
        let manifest = env!("CARGO_MANIFEST_DIR");
        candidates.push(
            PathBuf::from(manifest)
                .join("..")
                .join("..")
                .join("..")
                .join("configs")
                .join("scenarios"),
        );
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join("configs").join("scenarios"));
        }
    }

    if let Ok(home) = std::env::var("HOME") {
        candidates.push(
            PathBuf::from(home)
                .join(".config")
                .join("dsfs")
                .join("scenarios"),
        );
    }

    for c in candidates {
        if c.is_dir() {
            return Some(std::fs::canonicalize(&c).unwrap_or(c));
        }
    }
    None
}

// ---- Tauri commands ----

#[tauri::command]
fn list_scenarios() -> Vec<String> {
    let Some(dir) = scenarios_dir() else {
        eprintln!("dsfs: scenarios directory not found in any candidate location");
        return Vec::new();
    };
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(&dir) {
        for entry in rd.flatten() {
            let p = entry.path();
            if p.extension().and_then(|e| e.to_str()) == Some("yaml") {
                if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
                    out.push(stem.to_string());
                }
            }
        }
    }
    out.sort();
    out
}

#[tauri::command]
fn scenarios_dir_info() -> String {
    match scenarios_dir() {
        Some(p) => format!("scenarios dir: {}", p.display()),
        None => {
            let cwd = std::env::current_dir()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|_| "<unknown>".into());
            let exe = std::env::current_exe()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|_| "<unknown>".into());
            format!(
                "scenarios not found\n  cwd={cwd}\n  exe={exe}\n  tried: cwd/configs/scenarios, CARGO_MANIFEST_DIR/../../../configs/scenarios, exe_dir/configs/scenarios, ~/.config/dsfs/scenarios"
            )
        }
    }
}

#[tauri::command]
fn load_scenario(name: String, state: State<'_, AppState>) -> Result<SimSnapshot, String> {
    let Some(dir) = scenarios_dir() else {
        return Err(
            "scenarios directory not found; call scenarios_dir_info for diagnostics".into(),
        );
    };
    let path = dir.join(format!("{name}.yaml"));
    let text = std::fs::read_to_string(&path)
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let scenario = Scenario::from_yaml(&text).map_err(|e| format!("scenario error: {e}"))?;

    let mut guard = state.inner.lock().unwrap();
    *guard = EngineState {
        scenario: Some(scenario.clone()),
        engine: None,
        paused: true,
        speed: 1.0,
        applied_fault_count: 0,
        last_snapshot: None,
        tour: TourState::inactive(),
        last_crashed_leader: None,
    };
    let engine = build_engine(
        &scenario,
        EngineConfig {
            master_seed: scenario.seed,
            max_events: u64::MAX,
        },
    )
    .map_err(|e| format!("engine error: {e}"))?;
    guard.engine = Some(engine);

    let snap = compute_snapshot(&guard);
    guard.last_snapshot = Some(snap.clone());
    Ok(snap)
}

#[tauri::command]
fn set_paused(paused: bool, state: State<'_, AppState>) -> Result<(), String> {
    let mut g = state.inner.lock().unwrap();
    g.paused = paused;
    Ok(())
}

#[tauri::command]
fn set_speed(speed: f64, state: State<'_, AppState>) -> Result<(), String> {
    let mut g = state.inner.lock().unwrap();
    g.speed = speed.clamp(0.25, 8.0);
    Ok(())
}

#[tauri::command]
fn reset(state: State<'_, AppState>) -> Result<SimSnapshot, String> {
    let mut g = state.inner.lock().unwrap();
    let Some(scenario) = g.scenario.clone() else {
        return Err("no scenario loaded".into());
    };
    let engine = build_engine(
        &scenario,
        EngineConfig {
            master_seed: scenario.seed,
            max_events: u64::MAX,
        },
    )
    .map_err(|e| format!("engine error: {e}"))?;
    g.engine = Some(engine);
    g.paused = true;
    g.applied_fault_count = 0;
    g.tour = TourState::inactive();
    g.last_crashed_leader = None;
    let snap = compute_snapshot(&g);
    g.last_snapshot = Some(snap.clone());
    Ok(snap)
}

#[tauri::command]
fn get_state(state: State<'_, AppState>) -> Result<SimSnapshot, String> {
    let mut g = state.inner.lock().unwrap();

    let tour_waiting_for_condition = g.tour.active && !g.tour.finished && !g.tour.step_ready;
    let should_run = (!g.paused && !g.tour.active) || tour_waiting_for_condition;
    if should_run {
        if g.tour.active {
            let target_ms = (100.0 / g.speed).max(1.0) as u64;
            let n_nodes = g
                .scenario
                .as_ref()
                .map(|s| s.cluster.nodes)
                .unwrap_or(0);
            let cond = g.tour.current_wait_condition().cloned();
            if let Some(engine) = g.engine.as_mut() {
                let deadline = engine.now().as_millis() + target_ms;
                let mut safety = 10_000;
                while engine.now().as_millis() < deadline && safety > 0 {
                    let before_processed = engine.processed_events();
                    let _ = engine.run_until(1);
                    if engine.processed_events() == before_processed {
                        break;
                    }
                    safety -= 1;
                    if let Some(cond) = &cond {
                        if evaluate_wait_condition_engine(cond, engine, n_nodes) {
                            break;
                        }
                    }
                }
            }
        } else {
            let steps = (50.0 * g.speed) as u64;
            if let Some(engine) = g.engine.as_mut() {
                let _ = engine.run_until(steps);
            }
        }
    }

    if g.tour.active && !g.tour.finished {
        if let Some(cond) = g.tour.current_wait_condition().cloned() {
            let ready = evaluate_wait_condition(&cond, &g);
            g.tour.step_ready = ready;
        }
    }

    let snap = compute_snapshot(&g);
    g.last_snapshot = Some(snap.clone());
    Ok(snap)
}

#[tauri::command]
fn start_tour(tour_name: String, state: State<'_, AppState>) -> Result<SimSnapshot, String> {
    let kind = match tour_name.as_str() {
        "leader_failure" => TourKind::LeaderFailure,
        "network_partition" => TourKind::NetworkPartition,
        "node_recovery" => TourKind::NodeRecovery,
        other => return Err(format!("unknown tour: {other}")),
    };
    let scenario_name = kind.scenario_name();

    let Some(dir) = scenarios_dir() else {
        return Err("scenarios dir not found".into());
    };
    let path = dir.join(format!("{scenario_name}.yaml"));
    let text = std::fs::read_to_string(&path)
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let scenario = Scenario::from_yaml(&text).map_err(|e| format!("scenario error: {e}"))?;

    let mut g = state.inner.lock().unwrap();
    let engine = build_engine_no_faults(
        &scenario,
        EngineConfig {
            master_seed: scenario.seed,
            max_events: u64::MAX,
        },
    )
    .map_err(|e| format!("engine error: {e}"))?;
    g.scenario = Some(scenario);
    g.engine = Some(engine);
    g.paused = true;
    g.last_crashed_leader = None;
    g.tour = TourState::inactive();
    g.tour.start(kind);

    let snap = compute_snapshot(&g);
    g.last_snapshot = Some(snap.clone());
    Ok(snap)
}

#[tauri::command]
fn advance_tour(state: State<'_, AppState>) -> Result<SimSnapshot, String> {
    let mut g = state.inner.lock().unwrap();

    if g.tour.finished {
        let kind = g.tour.kind;
        if let (Some(kind), Some(scenario)) = (kind, g.scenario.clone()) {
            let engine = build_engine_no_faults(
                &scenario,
                EngineConfig {
                    master_seed: scenario.seed,
                    max_events: u64::MAX,
                },
            )
            .map_err(|e| format!("engine error: {e}"))?;
            g.engine = Some(engine);
            g.last_crashed_leader = None;
            g.paused = true;
            g.tour = TourState::inactive();
            g.tour.start(kind);
        }
        let snap = compute_snapshot(&g);
        g.last_snapshot = Some(snap.clone());
        return Ok(snap);
    }

    if !g.tour.active {
        return Err("tour not active".into());
    }

    g.tour.advance();

    if let Some(action) = g.tour.current_action().cloned() {
        let step_index = g.tour.current_step;
        match action {
            TourAction::None => {}
            TourAction::StartEngine => {
                g.paused = false;
            }
            TourAction::CrashLeader => {
                let crashed_leader = if let Some(engine) = g.engine.as_mut() {
                    let leader = engine.find_leader();
                    if let Some(leader) = leader {
                        let _ = engine.set_availability(leader, Availability::Offline);
                        let now = engine.now();
                        let _ = engine.log_mut().record(LogRecord {
                            event_id: dsfs_core::event::EventId(u64::MAX),
                            time: now,
                            kind: LogEventKind::FaultApplied {
                                index: step_index,
                                description: "crash leader".to_string(),
                            },
                            caused_by: None,
                        });
                        Some(leader)
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(leader) = crashed_leader {
                    g.last_crashed_leader = Some(leader);
                }
            }
            TourAction::CrashNode(id) => {
                if let Some(engine) = g.engine.as_mut() {
                    let _ = engine.set_availability(NodeId(id), Availability::Offline);
                    let now = engine.now();
                    let _ = engine.log_mut().record(LogRecord {
                        event_id: dsfs_core::event::EventId(u64::MAX),
                        time: now,
                        kind: LogEventKind::FaultApplied {
                            index: step_index,
                            description: format!("crash node {id}"),
                        },
                        caused_by: None,
                    });
                }
            }
            TourAction::RestartNode(id) => {
                if let Some(engine) = g.engine.as_mut() {
                    let _ = engine.set_availability(NodeId(id), Availability::Online);
                    let now = engine.now();
                    let _ = engine.log_mut().record(LogRecord {
                        event_id: dsfs_core::event::EventId(u64::MAX),
                        time: now,
                        kind: LogEventKind::FaultApplied {
                            index: step_index,
                            description: format!("restart node {id}"),
                        },
                        caused_by: None,
                    });
                }
            }
            TourAction::Partition { group_a, group_b } => {
                if let Some(engine) = g.engine.as_mut() {
                    let a: Vec<NodeId> = group_a.iter().map(|&x| NodeId(x)).collect();
                    let b: Vec<NodeId> = group_b.iter().map(|&x| NodeId(x)).collect();
                    engine.network_mut().partition_groups(&a, &b);
                    let now = engine.now();
                    let _ = engine.log_mut().record(LogRecord {
                        event_id: dsfs_core::event::EventId(u64::MAX),
                        time: now,
                        kind: LogEventKind::FaultApplied {
                            index: step_index,
                            description: "partition".to_string(),
                        },
                        caused_by: None,
                    });
                }
            }
            TourAction::Heal { group_a, group_b } => {
                if let Some(engine) = g.engine.as_mut() {
                    let a: Vec<NodeId> = group_a.iter().map(|&x| NodeId(x)).collect();
                    let b: Vec<NodeId> = group_b.iter().map(|&x| NodeId(x)).collect();
                    engine.network_mut().heal_groups(&a, &b);
                    let now = engine.now();
                    let _ = engine.log_mut().record(LogRecord {
                        event_id: dsfs_core::event::EventId(u64::MAX),
                        time: now,
                        kind: LogEventKind::FaultApplied {
                            index: step_index,
                            description: "heal".to_string(),
                        },
                        caused_by: None,
                    });
                }
            }
            TourAction::SubmitWrite { key, value } => {
                let cmd = encode_put(key, value);
                if let Some(engine) = g.engine.as_mut() {
                    let _ = engine.submit_command(cmd);
                }
            }
        }
    }

    if g.tour.active && !g.tour.finished {
        let cond = g.tour.current_wait_condition().cloned();
        if let Some(cond) = cond {
            let n_nodes = g
                .scenario
                .as_ref()
                .map(|s| s.cluster.nodes)
                .unwrap_or(0);
            let mut ready = g
                .engine
                .as_ref()
                .map_or(true, |e| evaluate_wait_condition_engine(&cond, e, n_nodes));
            if !ready {
                if let Some(engine) = g.engine.as_mut() {
                    let deadline = engine.now().as_millis() + 400;
                    let mut safety = 2000;
                    while engine.now().as_millis() < deadline && safety > 0 {
                        let before_processed = engine.processed_events();
                        let _ = engine.run_until(1);
                        if engine.processed_events() == before_processed {
                            break;
                        }
                        safety -= 1;
                        if evaluate_wait_condition_engine(&cond, engine, n_nodes) {
                            ready = true;
                            break;
                        }
                    }
                }
            }
            g.tour.step_ready = ready;
        }
    }

    let snap = compute_snapshot(&g);
    g.last_snapshot = Some(snap.clone());
    Ok(snap)
}

#[tauri::command]
fn stop_tour(state: State<'_, AppState>) -> Result<SimSnapshot, String> {
    let mut g = state.inner.lock().unwrap();
    g.tour = TourState::inactive();
    g.paused = true;
    let snap = compute_snapshot(&g);
    g.last_snapshot = Some(snap.clone());
    Ok(snap)
}

// ---- Wait condition evaluation ----

fn evaluate_wait_condition_engine(
    cond: &WaitCondition,
    engine: &SimulationEngine<InMemoryLog, FullNetwork>,
    n_nodes: u32,
) -> bool {
    match cond {
        WaitCondition::Immediate => true,
        WaitCondition::LeaderExists => (1..=n_nodes).any(|i| {
            let id = NodeId(i);
            engine.node_role(id) == Some(Role::Leader)
                && engine.availability(id).map_or(false, |a| a.is_active())
        }),
        WaitCondition::CandidateExists => (1..=n_nodes)
            .any(|i| engine.node_role(NodeId(i)) == Some(Role::Candidate)),
        WaitCondition::NewLeaderAfterCrash => {
            let records = engine.log().records();
            let mut crashed = false;
            let mut new_leader_after = false;
            for r in records {
                match &r.kind {
                    LogEventKind::FaultApplied { description, .. } => {
                        if description.contains("crash") {
                            crashed = true;
                        }
                    }
                    LogEventKind::RaftBecameLeader { .. } if crashed => {
                        new_leader_after = true;
                    }
                    _ => {}
                }
            }
            new_leader_after
        }
        WaitCondition::FaultApplied(substr) => engine.log().records().iter().any(|r| {
            matches!(
                &r.kind,
                LogEventKind::FaultApplied { description, .. } if description.contains(substr)
            )
        }),
        WaitCondition::AppliedCommand(substr) => {
            let needle = substr.as_bytes();
            engine.log().records().iter().any(|r| match &r.kind {
                LogEventKind::RaftApplied { command, .. } => {
                    command.windows(needle.len()).any(|w| w == needle)
                }
                _ => false,
            })
        }
        WaitCondition::NodeOffline(id) => engine
            .availability(NodeId(*id))
            .map_or(false, |a| !a.is_active()),
        WaitCondition::NodeOnline(id) => engine
            .availability(NodeId(*id))
            .map_or(false, |a| a.is_active()),
        WaitCondition::NodeAppliedCommand(id, substr) => {
            let needle = substr.as_bytes();
            engine.raft_applied(NodeId(*id)).map_or(false, |cmds| {
                cmds.iter()
                    .any(|cmd| cmd.windows(needle.len()).any(|w| w == needle))
            })
        }
        WaitCondition::TimeReached(ms) => engine.now().as_millis() >= *ms,
    }
}

fn evaluate_wait_condition(cond: &WaitCondition, g: &EngineState) -> bool {
    let Some(engine) = g.engine.as_ref() else {
        return matches!(cond, WaitCondition::Immediate);
    };
    let n = g
        .scenario
        .as_ref()
        .map(|s| s.cluster.nodes)
        .unwrap_or(0);
    evaluate_wait_condition_engine(cond, engine, n)
}

// ---- Engine build ----

fn build_engine(
    scenario: &Scenario,
    cfg: EngineConfig,
) -> std::io::Result<SimulationEngine<InMemoryLog, FullNetwork>> {
    let engine = build_engine_no_faults(scenario, cfg)?;
    Ok(engine.with_scenario(scenario))
}

fn build_engine_no_faults(
    scenario: &Scenario,
    cfg: EngineConfig,
) -> std::io::Result<SimulationEngine<InMemoryLog, FullNetwork>> {
    let mut link = LinkConfig::perfect();
    link.base_latency = VDuration::from_millis(scenario.network.default.base_latency_ms);
    link.jitter = VDuration::from_millis(scenario.network.default.jitter_ms);
    link.loss_probability = scenario.network.default.loss_probability;
    link.duplicate_probability = scenario.network.default.duplicate_probability;
    let net = FullNetwork::new(scenario.seed, link);

    let mut engine = SimulationEngine::new(cfg, net, InMemoryLog::new());

    let n = scenario.cluster.nodes;
    let peers: Vec<PeerId> = (1..=n).map(PeerId).collect();
    let raft_cfg = RaftConfig {
        election_timeout: StdDuration::from_millis(scenario.cluster.raft.election_timeout_ms),
        heartbeat_interval: StdDuration::from_millis(scenario.cluster.raft.heartbeat_interval_ms),
    };
    for &p in &peers {
        let rng = StreamRng::new(
            scenario.seed,
            StreamId::raft(p.0, RaftStreamPurpose::ElectionTimeout),
        );
        let raft = RaftNode::new(p, peers.clone(), raft_cfg.clone(), Box::new(rng));
        let adapter = RaftAdapter::new(NodeId(p.0), raft);
        engine.add_node(NodeImpl::Raft(Box::new(adapter)))?;
    }
    Ok(engine)
}

// ---- Snapshot ----

fn compute_snapshot(g: &EngineState) -> SimSnapshot {
    let scenario = g.scenario.clone();
    let scenario_name = scenario
        .as_ref()
        .map(|s| s.name.clone())
        .unwrap_or_default();
    let seed = scenario.as_ref().map(|s| s.seed).unwrap_or(0);

    let mut snap = SimSnapshot {
        scenario_name,
        seed,
        running: g.engine.is_some(),
        paused: g.paused,
        speed: g.speed,
        time_ms: 0,
        nodes: Vec::new(),
        recent_events: Vec::new(),
        faults: Vec::new(),
        stats: StatsView::default(),
        tour: g.tour.clone(),
    };

    let Some(engine) = g.engine.as_ref() else {
        return snap;
    };

    snap.time_ms = engine.now().as_millis();

    let n = scenario.as_ref().map(|s| s.cluster.nodes).unwrap_or(0);
    for id in 1..=n {
        let node_id = NodeId(id);
        let role = engine.node_role(node_id).unwrap_or(Role::Follower);
        let availability = engine.availability(node_id).unwrap_or(Availability::Online);
        let term = engine.raft_term(node_id).unwrap_or(0);
        let log_len = engine.raft_log_len(node_id).unwrap_or(0);
        let commit = engine.raft_commit_index(node_id).unwrap_or(0);
        let voted_for = engine.raft_voted_for(node_id).flatten();
        snap.nodes.push(NodeView {
            id,
            role: role_str(role),
            availability: avail_str(availability),
            term,
            log_len,
            commit,
            voted_for,
            is_leader: matches!(role, Role::Leader),
        });
    }

    let records = engine.log().records();
    let start = records.len().saturating_sub(50);
    for r in &records[start..] {
        let (kind, short) = describe_record(r);
        snap.recent_events.push(EventView {
            event_id: r.event_id.0,
            time_ms: r.time.as_millis(),
            kind,
            kind_short: short,
        });
    }

    if let Some(sc) = scenario.as_ref() {
        let applied_indices: std::collections::BTreeSet<usize> = records
            .iter()
            .filter_map(|r| match &r.kind {
                LogEventKind::FaultApplied { index, .. } => Some(*index),
                _ => None,
            })
            .collect();

        for (i, ev) in sc.events.iter().enumerate() {
            let desc = describe_fault(&ev.action);
            snap.faults.push(FaultView {
                index: i,
                at_ms: ev.at_ms,
                description: desc,
                completed: applied_indices.contains(&i),
            });
        }
    }

    let mut stats = StatsView::default();
    for r in records {
        match &r.kind {
            LogEventKind::MessageSent { .. } => stats.messages_sent += 1,
            LogEventKind::MessageDropped { .. } => stats.messages_dropped += 1,
            LogEventKind::MessageDuplicated { .. } => stats.messages_duplicated += 1,
            LogEventKind::RaftBecameLeader { .. } => stats.leaders_elected += 1,
            LogEventKind::RaftApplied { .. } => stats.applied += 1,
            _ => {}
        }
    }
    stats.events_processed = engine.processed_events();
    snap.stats = stats;

    snap
}

fn role_str(r: Role) -> String {
    match r {
        Role::Follower => "Follower",
        Role::Candidate => "Candidate",
        Role::Leader => "Leader",
    }
    .to_string()
}

fn avail_str(a: Availability) -> String {
    match a {
        Availability::Online => "Online",
        Availability::Offline => "Offline",
        Availability::Recovering => "Recovering",
    }
    .to_string()
}

fn describe_fault(a: &FaultAction) -> String {
    match a {
        FaultAction::WaitForLeader {} => "wait_for_leader".to_string(),
        FaultAction::CrashNode { node } => format!("crash node {node}"),
        FaultAction::RestartNode { node } => format!("restart node {node}"),
        FaultAction::CrashLeader {} => "crash leader".to_string(),
        FaultAction::RestartCrashedLeader {} => "restart crashed leader".to_string(),
        FaultAction::Partition { groups } => format!("partition {groups:?}"),
        FaultAction::Heal { groups } => format!("heal {groups:?}"),
        FaultAction::SetSlow { node, factor } => format!("slow node {node} = {factor}"),
        FaultAction::ClearSlow { node } => format!("clear slow {node}"),
        FaultAction::SubmitWrite { key, value } => format!("submit {key}={value}"),
    }
}

fn describe_record(r: &LogRecord) -> (String, String) {
    use LogEventKind::*;
    match &r.kind {
        MessageSent { from, to } => (format!("message {from} -> {to}"), "msg ->".to_string()),
        MessageDelivered { from, to } => {
            (format!("delivered {from} -> {to}"), "delivered".to_string())
        }
        MessageDropped { from, to } => (format!("dropped {from} -> {to}"), "dropped".to_string()),
        RaftBecameLeader { node, term } => (
            format!("node {node} became leader (term {term})"),
            "leader".to_string(),
        ),
        RaftBecameCandidate { node, term } => (
            format!("node {node} became candidate (term {term})"),
            "candidate".to_string(),
        ),
        RaftBecameFollower { node, term, reason } => (
            format!("node {node} became follower (term {term}, {reason})"),
            "follower".to_string(),
        ),
        RaftApplied { node, index, command } => (
            format!(
                "node {node} applied index {index} ({} bytes)",
                command.len()
            ),
            "applied".to_string(),
        ),
        RaftCommitAdvanced { node, from, to } => (
            format!("node {node} committed up to {to} (was {from})"),
            "commit".to_string(),
        ),
        RaftVotedFor { node, candidate, term } => (
            format!("node {node} voted for {candidate} (term {term})"),
            "vote".to_string(),
        ),
        RaftTermBumped { node, from, to } => (
            format!("node {node} term {from} -> {to}"),
            "term".to_string(),
        ),
        FaultApplied { index, description } => (
            format!("fault {index}: {description}"),
            "fault".to_string(),
        ),
        _ => (format!("{:?}", r.kind), "-".to_string()),
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let state = AppState {
        inner: Arc::new(Mutex::new(EngineState::default())),
    };

    tauri::Builder::default()
        .manage(state)
        .invoke_handler(tauri::generate_handler![
            list_scenarios,
            scenarios_dir_info,
            load_scenario,
            set_paused,
            set_speed,
            reset,
            get_state,
            start_tour,
            advance_tour,
            stop_tour,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}