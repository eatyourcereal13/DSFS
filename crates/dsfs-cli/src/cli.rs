//! Clap definitions.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "dsfs",
    version,
    about = "Distributed Systems Failure Simulator"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Create configs/scenarios/ with an example scenario.
    Init(InitArgs),
    /// Run a scenario and save its JSONL log.
    Run(RunArgs),
    /// Show a summary of a JSONL log.
    Inspect(InspectArgs),
    /// Run the consistency checker on a JSONL log.
    Validate(ValidateArgs),
    /// Re-run the scenario from a JSONL log and compare event sequences.
    Replay(ReplayArgs),
}

#[derive(Args, Debug)]
pub struct InitArgs {
    /// Directory to create (default: configs/scenarios).
    #[arg(default_value = "configs/scenarios")]
    pub dir: PathBuf,
    /// Overwrite existing files.
    #[arg(long)]
    pub force: bool,
}

#[derive(Args, Debug)]
pub struct RunArgs {
    /// Scenario file to run.
    pub scenario: PathBuf,
    /// Where to write the JSONL log (default: results/latest.jsonl).
    #[arg(long, default_value = "results/latest.jsonl")]
    pub out: PathBuf,
    /// Do not write any log to disk.
    #[arg(long)]
    pub no_log: bool,
}

#[derive(Args, Debug)]
pub struct InspectArgs {
    /// JSONL log file.
    pub log: PathBuf,
}

#[derive(Args, Debug)]
pub struct ValidateArgs {
    /// JSONL log file.
    pub log: PathBuf,
}

#[derive(Args, Debug)]
pub struct ReplayArgs {
    /// JSONL log file whose scenario metadata will be replayed.
    pub log: PathBuf,
}
