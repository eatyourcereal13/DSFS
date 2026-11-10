//! `dsfs inspect`: summary of a JSONL log.

use std::fs::File;
use std::io::{self, BufReader};
use std::process::ExitCode;

use dsfs_checker::read_jsonl;

use crate::cli::InspectArgs;

pub fn run(args: InspectArgs) -> io::Result<ExitCode> {
    let file = File::open(&args.log)?;
    let reader = BufReader::new(file);
    let out = read_jsonl(reader)?;

    println!("log: {}", args.log.display());
    if let Some(m) = &out.meta {
        println!("  schema:       {}", m.schema);
        println!("  sim_version:  {}", m.sim_version);
        println!("  scenario:     {}", m.scenario);
        println!("  seed:         {}", m.seed);
    } else {
        println!("  (no meta line)");
    }
    println!("  records:      {}", out.records.len());

    // Small stats.
    let mut sent = 0usize;
    let mut leaders = 0usize;
    let mut commits = 0usize;
    let mut applied = 0usize;
    use dsfs_core::log::LogEventKind;
    for r in &out.records {
        match &r.kind {
            LogEventKind::MessageSent { .. } => sent += 1,
            LogEventKind::RaftBecameLeader { .. } => leaders += 1,
            LogEventKind::RaftCommitAdvanced { .. } => commits += 1,
            LogEventKind::RaftApplied { .. } => applied += 1,
            _ => {}
        }
    }
    println!("  messages sent: {sent}");
    println!("  leaders:       {leaders}");
    println!("  commits:       {commits}");
    println!("  applied:       {applied}");

    Ok(ExitCode::SUCCESS)
}
