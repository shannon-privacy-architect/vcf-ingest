//! Streaming loop: read the VCF line-by-line into one reusable buffer, run
//! each chunk through the in-process Rust validator, and record lightweight
//! per-chunk instrumentation (task: chunk-level metrics).
//!
//! Zeroization contract (audit): exactly ONE heap buffer is used for raw
//! bytes; it is erased **after** validation and **before** reuse; no clones
//! of the raw line are ever created beyond what the validator reads in
//! place (the resulting report holds only parsed/canonicalized data — raw
//! bytes are never retained).

use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::time::Instant;

use crate::error::{Error, Result};
use crate::erase::erase_buffer;
use crate::precheck::{LINE_CAP, PrecheckMeta};
use crate::schema::{ChunkReport, SummaryReport};
use crate::validate::Validator;

/// Per-run streaming statistics (instrumentation surface).
#[derive(Debug, Default, Clone)]
pub struct StreamStats {
    pub chunks: u64,
    pub bytes: u64,
    pub lines: u64,
    pub total_chunk_ms: u128,
    pub peak_chunk_ms: u128,
    pub min_chunk_ms: u128,
    /// Kept for report-shape compatibility with existing callers. Always 0
    /// now that validation is in-process — there is no IPC round-trip to
    /// wait on anymore.
    pub wait_ms: u128,
    pub records_canonicalized: u64,
    pub errors: u64,
}

impl StreamStats {
    fn record_chunk(&mut self, elapsed_ms: u128, wait_ms: u128) {
        self.chunks += 1;
        self.total_chunk_ms += elapsed_ms;
        self.peak_chunk_ms = self.peak_chunk_ms.max(elapsed_ms);
        if self.chunks == 1 || elapsed_ms < self.min_chunk_ms {
            self.min_chunk_ms = elapsed_ms;
        }
        self.wait_ms += wait_ms;
    }

    pub fn mean_chunk_ms(&self) -> f64 {
        if self.chunks == 0 {
            0.0
        } else {
            self.total_chunk_ms as f64 / self.chunks as f64
        }
    }
}

/// Stream `path` through the in-process validator, collecting one
/// [ChunkReport] per frame.
/// `canon_sink` (if provided) receives the canonicalized records emitted by
/// the validator for every chunk (task 8).
pub fn stream_vcf(
    path: &str,
    meta: &PrecheckMeta,
    validator: &mut Validator,
    mut canon_sink: Option<&mut dyn Write>,
    stats: &mut StreamStats,
    mut on_report: impl FnMut(&ChunkReport),
) -> Result<SummaryReport> {
    log::info!("streaming {} (expected {} columns)", path, meta.expected_columns);
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    // ONE reusable chunk buffer for the whole run (zeroization audit).
    let mut chunk: Vec<u8> = Vec::with_capacity(64 * 1024);

    loop {
        chunk.clear();
        let n = match reader.read_until(b'\n', &mut chunk) {
            Ok(n) => n,
            Err(e) => {
                erase_buffer(&mut chunk);
                return Err(Error::from(e));
            }
        };
        if n == 0 {
            erase_buffer(&mut chunk);
            break; // EOF
        }
        if chunk.len() > LINE_CAP {
            // Hard line cap (PRD §9): refuse silently-growing lines.
            let err = Error::InputTooBig {
                bytes: chunk.len(),
                limit: LINE_CAP,
            };
            erase_buffer(&mut chunk);
            return Err(err);
        }

        let t0 = Instant::now();
        let report = validator.process_chunk(&chunk);
        // Raw bytes must not survive past validation: erase now that the
        // validator is done reading this chunk (no dispatch step anymore).
        erase_buffer(&mut chunk);
        let elapsed_ms = t0.elapsed().as_millis();
        validator.record_chunk_ms(elapsed_ms);

        stats.lines += report.lines;
        stats.bytes += report.bytes;
        stats.errors += report.errors.len() as u64;
        stats.records_canonicalized += report.records.len() as u64;
        stats.record_chunk(elapsed_ms, 0);

        if let Some(sink) = canon_sink.as_deref_mut() {
            for rec in &report.records {
                let json = serde_json::to_string(rec)?;
                sink.write_all(json.as_bytes())?;
                sink.write_all(b"\n")?;
            }
        }

        on_report(&report);

        // Graceful Ctrl+C: stop at the next chunk boundary (exit 130).
        if crate::INTERRUPTED.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(Error::Interrupted);
        }
    }

    // EOF: build the final summary directly (no IPC round-trip).
    let summary = validator.summary();

    // Full erase of the reusable chunk buffer before returning (it is
    // already empty after the per-iteration erases; this is the belt).
    erase_buffer(&mut chunk);

    log::info!(
        "stream complete: chunks={} lines={} bytes={} peak_chunk_ms={} mean_chunk_ms={:.2}",
        stats.chunks,
        stats.lines,
        stats.bytes,
        stats.peak_chunk_ms,
        stats.mean_chunk_ms()
    );

    Ok(summary)
}

/// Convenience: run the whole stream against a canonical sink writer and
/// return the raw records in JSON form so callers can hand them to the Noir
/// witness-prep stage (task 9) without re-parsing frames.
#[allow(dead_code)] // task 9 entry point; not yet invoked by main()
pub fn stream_canonical(
    path: &str,
    meta: &PrecheckMeta,
    validator: &mut Validator,
    stats: &mut StreamStats,
) -> Result<Vec<crate::schema::CanonicalRecord>> {
    let mut sink = SinkVec::default();
    stream_vcf(path, meta, validator, Some(&mut sink), stats, |_| {})?;
    Ok(sink.records)
}

#[derive(Default)]
struct SinkVec {
    records: Vec<crate::schema::CanonicalRecord>,
}

impl Write for SinkVec {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        for line in buf.split(|&b| b == b'\n') {
            if line.is_empty() {
                continue;
            }
            if let Ok(rec) = serde_json::from_slice::<crate::schema::CanonicalRecord>(line) {
                self.records.push(rec);
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}