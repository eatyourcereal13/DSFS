//! `dsfs init`: create a starter scenario.

use std::fs;
use std::io;
use std::process::ExitCode;

use crate::cli::InitArgs;

const EXAMPLE_SCENARIO: &str = r#"version: 1
name: example
seed: 42
cluster:
  nodes: 5
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
  - wait_for_leader: {}
  - at_ms: 1000
    submit_write:
      key: hello
      value: world
"#;

pub fn run(args: InitArgs) -> io::Result<ExitCode> {
    if args.dir.exists() && !args.force {
        eprintln!(
            "error: directory '{}' already exists (use --force to overwrite files)",
            args.dir.display()
        );
        return Ok(ExitCode::from(1));
    }
    fs::create_dir_all(&args.dir)?;
    let example = args.dir.join("example.yaml");
    if example.exists() && !args.force {
        eprintln!(
            "error: '{}' already exists (use --force)",
            example.display()
        );
        return Ok(ExitCode::from(1));
    }
    fs::write(&example, EXAMPLE_SCENARIO)?;
    println!("created {}", example.display());
    Ok(ExitCode::SUCCESS)
}
