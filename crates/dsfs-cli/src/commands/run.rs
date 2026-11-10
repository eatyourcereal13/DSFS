//! `dsfs run`: execute a scenario.

use std::fs::{self, File};
use std::io::{self, BufWriter};
use std::path::Path;
use std::process::ExitCode;
use std::time::Duration as StdDuration;

use dsfs_core::engine::{EngineConfig, FullNetwork, LinkConfig, SimulationEngine};
use dsfs_core::log::{JsonlLog, LogMeta, LOG_SCHEMA_VERSION, SIM_VERSION};
use dsfs_core::node::{NodeId, NodeImpl, RaftAdapter};
use dsfs_core::rng::{RaftStreamPurpose, StreamId, StreamRng};
use dsfs_core::scenario::Scenario;
use dsfs_core::time::Duration as VDuration;
use dsfs_raft::{PeerId, RaftConfig, RaftNode};

use crate::cli::RunArgs;

/// Default budget for a run, in events processed.
const DEFAULT_MAX_EVENTS: u64 = 100_000;

pub fn run(args: RunArgs) -> io::Result<ExitCode> {
    let text = fs::read_to_string(&args.scenario)?;
    let scenario = match Scenario::from_yaml(&text) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("scenario error in '{}': {e}", args.scenario.display());
            return Ok(ExitCode::from(1));
        }
    };

    if args.no_log {
        let engine = build_engine::<NoLog>(&scenario, NoLog)?;
        let outcome = run_engine(engine, &scenario)?;
        println!("{}", outcome_message(&outcome));
        return Ok(ExitCode::SUCCESS);
    }

    // Prepare output path.
    if let Some(parent) = args.out.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let file = File::create(&args.out)?;
    let writer = BufWriter::new(file);
    let meta = LogMeta {
        schema: LOG_SCHEMA_VERSION,
        sim_version: SIM_VERSION.to_string(),
        scenario: scenario.name.clone(),
        seed: scenario.seed,
    };
    let log = JsonlLog::new(writer, meta)?;
    let engine = build_engine(&scenario, log)?;
    let outcome = run_engine(engine, &scenario)?;
    println!("{} -> {}", outcome_message(&outcome), args.out.display());
    Ok(ExitCode::SUCCESS)
}

fn outcome_message(outcome: &dsfs_core::engine::RunOutcome) -> String {
    match outcome {
        dsfs_core::engine::RunOutcome::Completed { processed } => {
            format!("simulation completed: {processed} events")
        }
        dsfs_core::engine::RunOutcome::HitEventLimit { processed } => {
            format!("simulation hit event limit: {processed} events")
        }
    }
}

fn run_engine<L>(
    mut engine: SimulationEngine<L, FullNetwork>,
    _scenario: &Scenario,
) -> io::Result<dsfs_core::engine::RunOutcome>
where
    L: dsfs_core::log::EventLog,
{
    engine.run_until(DEFAULT_MAX_EVENTS)
}

/// No-op event log for `--no-log` runs.
#[derive(Debug, Default)]
pub struct NoLog;

impl dsfs_core::log::EventLog for NoLog {
    fn record(&mut self, _record: dsfs_core::log::LogRecord) -> io::Result<()> {
        Ok(())
    }
    fn len(&self) -> usize {
        0
    }
}

fn build_engine<L: dsfs_core::log::EventLog>(
    scenario: &Scenario,
    log: L,
) -> io::Result<SimulationEngine<L, FullNetwork>> {
    let mut link = LinkConfig::perfect();
    link.base_latency = VDuration::from_millis(scenario.network.default.base_latency_ms);
    link.jitter = VDuration::from_millis(scenario.network.default.jitter_ms);
    link.loss_probability = scenario.network.default.loss_probability;
    link.duplicate_probability = scenario.network.default.duplicate_probability;
    let net = FullNetwork::new(scenario.seed, link);

    let mut engine = SimulationEngine::new(
        EngineConfig {
            master_seed: scenario.seed,
            max_events: u64::MAX,
        },
        net,
        log,
    );

    let n = scenario.cluster.nodes;
    let peers: Vec<PeerId> = (1..=n).map(PeerId).collect();
    let cfg = RaftConfig {
        election_timeout: StdDuration::from_millis(scenario.cluster.raft.election_timeout_ms),
        heartbeat_interval: StdDuration::from_millis(scenario.cluster.raft.heartbeat_interval_ms),
    };
    for &p in &peers {
        let rng = StreamRng::new(
            scenario.seed,
            StreamId::raft(p.0, RaftStreamPurpose::ElectionTimeout),
        );
        let raft = RaftNode::new(p, peers.clone(), cfg.clone(), Box::new(rng));
        let adapter = RaftAdapter::new(NodeId(p.0), raft);
        engine.add_node(NodeImpl::Raft(Box::new(adapter)))?;
    }

    Ok(engine.with_scenario(scenario))
}

/// Ensures the returned `SimulationEngine` is usable when `Path` is unused.
#[allow(dead_code)]
fn _unused(_p: &Path) {}
