//! `dsfs replay`: re-run a scenario from a JSONL log and compare.

use std::fs::{self, File};
use std::io::{self, BufReader};
use std::process::ExitCode;
use std::time::Duration as StdDuration;

use dsfs_checker::read_jsonl;
use dsfs_core::engine::{EngineConfig, FullNetwork, LinkConfig, SimulationEngine};
use dsfs_core::log::{EventLog, InMemoryLog, LogRecord};
use dsfs_core::node::{NodeId, NodeImpl, RaftAdapter};
use dsfs_core::rng::{RaftStreamPurpose, StreamId, StreamRng};
use dsfs_core::scenario::Scenario;
use dsfs_core::time::Duration as VDuration;
use dsfs_raft::{PeerId, RaftConfig, RaftNode};

use crate::cli::ReplayArgs;

/// Default budget for replay; enough to re-simulate what was originally run.
const DEFAULT_MAX_EVENTS: u64 = 100_000;

pub fn run(args: ReplayArgs) -> io::Result<ExitCode> {
    let file = File::open(&args.log)?;
    let reader = BufReader::new(file);
    let out = read_jsonl(reader)?;

    let Some(meta) = out.meta else {
        eprintln!("error: no meta line in '{}'", args.log.display());
        return Ok(ExitCode::from(1));
    };

    // Locate the scenario file.
    let scenario_path = format!("configs/scenarios/{}.yaml", meta.scenario);
    let scenario_text = fs::read_to_string(&scenario_path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("cannot read scenario '{scenario_path}': {e}"),
        )
    })?;
    let scenario = match Scenario::from_yaml(&scenario_text) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("scenario error: {e}");
            return Ok(ExitCode::from(1));
        }
    };
    if scenario.seed != meta.seed {
        eprintln!(
            "warning: log meta seed {} != scenario seed {}; replay may diverge",
            meta.seed, scenario.seed
        );
    }

    // Re-run in memory.
    let mut engine = build_engine(&scenario)?;
    let _ = engine.run_until(DEFAULT_MAX_EVENTS)?;
    let log = engine.into_log();
    let replayed = log.into_records();

    // Compare sequences of (event_id, kind debug).
    let original: Vec<(u64, String)> = out
        .records
        .iter()
        .map(|r: &LogRecord| (r.event_id.0, format!("{:?}", r.kind)))
        .collect();
    let rerun: Vec<(u64, String)> = replayed
        .iter()
        .map(|r| (r.event_id.0, format!("{:?}", r.kind)))
        .collect();

    if original == rerun {
        println!("replay OK: {} events match", original.len());
        Ok(ExitCode::SUCCESS)
    } else {
        let n = original.len().min(rerun.len());
        let mut first_diff = None;
        for i in 0..n {
            if original[i] != rerun[i] {
                first_diff = Some(i);
                break;
            }
        }
        eprintln!(
            "replay FAILED: original {} events, replay {} events",
            original.len(),
            rerun.len()
        );
        if let Some(i) = first_diff {
            eprintln!("  first divergence at index {i}:");
            eprintln!("    original: {:?}", original[i]);
            eprintln!("    replayed: {:?}", rerun[i]);
        }
        Ok(ExitCode::from(1))
    }
}

fn build_engine(scenario: &Scenario) -> io::Result<SimulationEngine<InMemoryLog, FullNetwork>> {
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
        InMemoryLog::new(),
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

// Silences unused warnings on the trait import in some configurations.
fn _use_event_log_trait<L: EventLog>(_l: &L) {}
