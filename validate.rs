//! In-process semantic validator — replaces the former Python sidecar
//! (`validator/validator.py`). Ported field-for-field: same error codes
//! (`COLUMN_COUNT`, `ENCODING`, `POS_NOT_INTEGER`), same canonicalization
//! shape, same per-chunk / summary report contract as `crate::schema`
//! already defines — so nothing downstream of a [`ChunkReport`] or
//! [`SummaryReport`] needs to change.
//!
//! One difference from the sidecar: there is no `NO_SESSION` error code.
//! That existed only because a CHUNK frame could arrive over IPC before a
//! SESSION frame. In-process, `Validator::new` always has the session's
//! `expected_columns` before the first chunk is processed, so that failure
//! mode cannot occur.

use std::collections::BTreeMap;
use std::time::Instant;

use crate::schema::{CanonicalRecord, ChunkReport, InfoField, SummaryReport, ValidationError};

/// In-process semantic validator. Owns the running session state that used
/// to live in the Python sidecar's `Validator` class: the expected column
/// count, running line/record ordinals, and aggregate error/timing stats
/// for the final summary.
pub struct Validator {
    session_id: String,
    expected_columns: u64,

    chunk_index: u64,
    line_no: u64,
    record_ordinal: u64,
    total_lines: u64,
    total_bytes: u64,
    errors_total: u64,
    error_codes: BTreeMap<String, u64>,
    chunk_ms_total: u128,
    chunk_ms_peak: u128,
    started: Instant,
}

impl Validator {
    pub fn new(session_id: String, expected_columns: u64) -> Self {
        Validator {
            session_id,
            expected_columns,
            chunk_index: 0,
            line_no: 0,
            record_ordinal: 0,
            total_lines: 0,
            total_bytes: 0,
            errors_total: 0,
            error_codes: BTreeMap::new(),
            chunk_ms_total: 0,
            chunk_ms_peak: 0,
            started: Instant::now(),
        }
    }

    fn bump(
        &mut self,
        code: &str,
        message: &str,
        line: Option<u64>,
        expected: Option<u64>,
        found: Option<u64>,
    ) -> ValidationError {
        self.errors_total += 1;
        *self.error_codes.entry(code.to_string()).or_insert(0) += 1;
        ValidationError {
            code: code.to_string(),
            message: message.to_string(),
            line,
            expected,
            found,
        }
    }

    /// Mirrors `Validator.canonicalize` in validator.py. `fields` is the
    /// tab-split line; the caller has already confirmed the column count
    /// and UTF-8 validity of the whole line, and that fields[1] parses as a
    /// positive integer.
    fn canonicalize(&mut self, fields: &[&[u8]]) -> CanonicalRecord {
        let rec = CanonicalRecord {
            index: self.record_ordinal,
            chrom: String::from_utf8_lossy(fields[0]).into_owned(),
            pos: std::str::from_utf8(fields[1])
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            id: if fields[2] != b"." {
                String::from_utf8_lossy(fields[2]).into_owned()
            } else {
                String::new()
            },
            reference: if fields[3] != b"." {
                String::from_utf8_lossy(fields[3]).into_owned()
            } else {
                String::new()
            },
            alt: fields[4]
                .split(|&b| b == b',')
                .filter(|a| *a != b".")
                .map(|a| String::from_utf8_lossy(a).into_owned())
                .collect(),
            // Kept raw, same as the sidecar — it never turned "." into None
            // itself; that's left to whatever consumes CanonicalRecord next.
            qual: Some(String::from_utf8_lossy(fields[5]).into_owned()),
            filter: fields[6]
                .split(|&b| b == b';')
                .filter(|f| *f != b"." && !f.is_empty())
                .map(|f| String::from_utf8_lossy(f).into_owned())
                .collect(),
            info: parse_info(fields[7]),
            format: if fields.len() > 8 {
                fields[8]
                    .split(|&b| b == b':')
                    .map(|f| String::from_utf8_lossy(f).into_owned())
                    .collect()
            } else {
                Vec::new()
            },
            samples: if fields.len() > 8 {
                fields[9..]
                    .iter()
                    .map(|col| {
                        col.split(|&b| b == b':')
                            .map(|v| String::from_utf8_lossy(v).into_owned())
                            .collect()
                    })
                    .collect()
            } else {
                Vec::new()
            },
        };
        self.record_ordinal += 1;
        rec
    }

    /// Validate one chunk of raw bytes in place, mirroring `process_chunk`
    /// in validator.py. Returns the same [`ChunkReport`] shape the sidecar
    /// used to send back over IPC — synchronously, with no serialization.
    pub fn process_chunk(&mut self, body: &[u8]) -> ChunkReport {
        self.chunk_index += 1;
        self.total_bytes += body.len() as u64;

        let mut errors = Vec::new();
        let mut records = Vec::new();
        let mut lines_in_chunk: u64 = 0;

        for raw in body.split(|&b| b == b'\n') {
            if raw.is_empty() {
                continue;
            }
            lines_in_chunk += 1;
            self.line_no += 1;
            self.total_lines += 1;

            let line = if raw.last() == Some(&b'\r') {
                &raw[..raw.len() - 1]
            } else {
                raw
            };
            if line.starts_with(b"#") {
                continue;
            }

            let fields: Vec<&[u8]> = line.split(|&b| b == b'\t').collect();
            if fields.len() as u64 != self.expected_columns {
                errors.push(self.bump(
                    "COLUMN_COUNT",
                    "column count differs from #CHROM header",
                    Some(self.line_no),
                    Some(self.expected_columns),
                    Some(fields.len() as u64),
                ));
                continue;
            }

            if std::str::from_utf8(line).is_err() {
                errors.push(self.bump(
                    "ENCODING",
                    "line is not valid UTF-8",
                    Some(self.line_no),
                    None,
                    None,
                ));
                continue;
            }

            let pos_ok = std::str::from_utf8(fields[1])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .map(|v| v > 0)
                .unwrap_or(false);
            if !pos_ok {
                errors.push(self.bump(
                    "POS_NOT_INTEGER",
                    "POS is not a positive integer",
                    Some(self.line_no),
                    None,
                    None,
                ));
                continue;
            }

            records.push(self.canonicalize(&fields));
        }

        ChunkReport {
            session_id: self.session_id.clone(),
            chunk_index: self.chunk_index,
            bytes: body.len() as u64,
            lines: lines_in_chunk,
            errors,
            records,
        }
    }

    /// Record one chunk's processing time for the final summary's timing
    /// stats. Call once per chunk from the streaming loop, after
    /// `process_chunk`.
    pub fn record_chunk_ms(&mut self, elapsed_ms: u128) {
        self.chunk_ms_total += elapsed_ms;
        self.chunk_ms_peak = self.chunk_ms_peak.max(elapsed_ms);
    }

    /// Build the final summary, mirroring `send_summary` in validator.py.
    pub fn summary(&self) -> SummaryReport {
        let mean = if self.chunk_index > 0 {
            (self.chunk_ms_total as f64 / self.chunk_index as f64) as u64
        } else {
            0
        };
        SummaryReport {
            session_id: self.session_id.clone(),
            total_chunks: self.chunk_index,
            total_lines: self.total_lines,
            total_bytes: self.total_bytes,
            errors: self.errors_total,
            error_codes: self.error_codes.clone(),
            duration_ms: self.started.elapsed().as_millis() as u64,
            peak_chunk_ms: self.chunk_ms_peak as u64,
            mean_chunk_ms: mean,
        }
    }
}

/// `DP=100;FLAG;AF=0.2` -> `[{key: DP, value: Some(100)}, {key: FLAG, value: None}, ...]`
fn parse_info(raw: &[u8]) -> Vec<InfoField> {
    let mut out = Vec::new();
    if raw.is_empty() || raw == b"." {
        return out;
    }
    for item in raw.split(|&b| b == b';') {
        if item.is_empty() {
            continue;
        }
        if let Some(eq) = item.iter().position(|&b| b == b'=') {
            out.push(InfoField {
                key: String::from_utf8_lossy(&item[..eq]).into_owned(),
                value: Some(String::from_utf8_lossy(&item[eq + 1..]).into_owned()),
            });
        } else {
            out.push(InfoField {
                key: String::from_utf8_lossy(item).into_owned(),
                value: None,
            });
        }
    }
    out
}