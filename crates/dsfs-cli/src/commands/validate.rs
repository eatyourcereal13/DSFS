//! `dsfs validate`: run the checker.

use std::fs::File;
use std::io::{self, BufReader};
use std::process::ExitCode;

use dsfs_checker::invariants::run as run_checker;
use dsfs_checker::read_jsonl;

use crate::cli::ValidateArgs;

pub fn run(args: ValidateArgs) -> io::Result<ExitCode> {
    let file = File::open(&args.log)?;
    let reader = BufReader::new(file);
    let out = read_jsonl(reader)?;

    if let Some(m) = &out.meta {
        println!(
            "validating {} ({} records, scenario={}, seed={})",
            args.log.display(),
            out.records.len(),
            m.scenario,
            m.seed
        );
    } else {
        println!(
            "validating {} ({} records, no meta)",
            args.log.display(),
            out.records.len()
        );
    }

    let report = run_checker(&out.records);
    if report.is_ok() {
        println!(
            "OK: no violations (sent={} drop={} dup={} leaders={} applied={})",
            report.stats.messages_sent,
            report.stats.messages_dropped,
            report.stats.messages_duplicated,
            report.stats.leaders_elected,
            report.stats.applied_commands
        );
        Ok(ExitCode::SUCCESS)
    } else {
        eprintln!("FAIL: {} violation(s)", report.violations.len());
        for v in &report.violations {
            eprintln!(
                "  [{}] t={:?} {} (events: {:?})",
                v.invariant, v.time, v.context, v.related_events
            );
        }
        Ok(ExitCode::from(1))
    }
}
