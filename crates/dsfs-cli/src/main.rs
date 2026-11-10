//! Command-line interface for DSFS.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

mod cli;
mod commands;

use std::process::ExitCode;

use clap::Parser;

use crate::cli::{Cli, Commands};

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Commands::Init(args) => commands::init::run(args),
        Commands::Run(args) => commands::run::run(args),
        Commands::Inspect(args) => commands::inspect::run(args),
        Commands::Validate(args) => commands::validate::run(args),
        Commands::Replay(args) => commands::replay::run(args),
    };
    match result {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(2)
        }
    }
}
