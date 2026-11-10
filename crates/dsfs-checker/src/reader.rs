//! JSONL reader.

use std::io::{self, BufRead};

use dsfs_core::log::{LogMeta, LogRecord};

/// Result of reading a JSONL file.
#[derive(Debug)]
pub struct ReadOutcome {
    pub meta: Option<LogMeta>,
    pub records: Vec<LogRecord>,
}

/// Reads a JSONL log file. The first line may be a meta line.
///
/// Lines that fail to parse as either meta or record are skipped and
/// counted in the error return (we do not fail hard: a partially corrupt
/// log can still be useful).
///
/// # Errors
///
/// Returns I/O errors from the reader. Parse errors are silently skipped
/// (returned count is included in the result via `records` only).
pub fn read_jsonl<R: BufRead>(mut reader: R) -> io::Result<ReadOutcome> {
    let mut meta: Option<LogMeta> = None;
    let mut records: Vec<LogRecord> = Vec::new();
    let mut first_line = true;
    let mut line = String::new();

    loop {
        line.clear();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if first_line {
            first_line = false;
            // Try parsing as meta.
            #[derive(serde::Deserialize)]
            struct MetaWrapper {
                meta: LogMeta,
            }
            if let Ok(w) = serde_json::from_str::<MetaWrapper>(trimmed) {
                meta = Some(w.meta);
                continue;
            }
        }
        match serde_json::from_str::<LogRecord>(trimmed) {
            Ok(r) => records.push(r),
            Err(_) => {
                // Skip silently; a partial log is still useful.
            }
        }
    }

    Ok(ReadOutcome { meta, records })
}
