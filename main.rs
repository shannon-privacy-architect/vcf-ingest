//! `vcf-ingest` — CLI entry point.
//!
//! Pipeline: bounded zeroized pre-check (Rust) → framed streaming to the
//! Python semantic sidecar (`validator/validator.py`) → per-chunk reports
//! with instrumentation → optional canonicalization to an NDJSON sink
//! (task 8) ready for the Noir witness-prep stage (task 9).
//!
//! Exit codes follow PRD §5: 0 ok · 2 input/pre-check/canonical failure ·
//! 3 cannot spawn sidecar · 4 validator/protocol failure · 5 usage ·
//! 130 interrupted (SIGINT).

mod error;
mod erase;
mod precheck;
mod schema;
mod stream;
mod validate;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use error::{Error, Result};
use schema::SessionInfo;

/// Raised by the Ctrl+C handler; the streaming loop checks it at chunk
/// boundaries and returns [`Error::Interrupted`] → exit code 130.
pub(crate) static INTERRUPTED: AtomicBool = AtomicBool::new(false);

/// CLI options (thin, hand-rolled parser — no clap, per implementation-spec).
struct Cli {
    file: PathBuf,
    canonical_out: Option<PathBuf>,
    json: bool,
}

const USAGE: &str = "\
vcf-ingest — offline, privacy-preserving VCF ingestion
(Rust streaming core + Python semantic sidecar over framed IPC)

Usage:
  vcf-ingest <input.vcf> [options]

Options:
  -c, --canonical <path>   also write canonicalized records (NDJSON) to <path>
  -j, --json               emit a machine-readable JSON report on stdout
  -h, --help               show this help and exit
  -V, --version            print version and exit

Exit codes:
  0 success · 2 input/pre-check/canonical file failure · 3 cannot spawn the
  sidecar · 4 validator/protocol failure · 5 usage · 130 interrupted
";

fn parse_args() -> Result<Cli> {
    let mut file: Option<PathBuf> = None;
    let mut canonical_out: Option<PathBuf> = None;
    let mut json = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            "-V" | "--version" => {
                println!("vcf-ingest {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            "-j" | "--json" => json = true,
            "-c" | "--canonical" => {
                let p = args
                    .next()
                    .ok_or_else(|| Error::Usage("--canonical requires a path".into()))?;
                canonical_out = Some(PathBuf::from(p));
            }
            s if s.starts_with('-') => {
                return Err(Error::Usage(format!("unknown flag: {s}")));
            }
            s => {
                if file.is_some() {
                    return Err(Error::Usage("only one input file is allowed".into()));
                }
                file = Some(PathBuf::from(s));
            }
        }
    }

    let file = file.ok_or_else(|| Error::Usage("missing input file (see --help)".into()))?;
    Ok(Cli { file, canonical_out, json })
}

/// Basename sanitized to `[A-Za-z0-9._-]` for the session frame.
fn sanitize_basename(path: &Path) -> String {
    path.file_name()
        .map(|s| {
            s.to_string_lossy()
                .chars()
                .filter(|&c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
                .collect::<String>()
        })
        .filter(|s: &String| !s.is_empty())
        .unwrap_or_else(|| "input.vcf".to_string())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn run(cli: &Cli) -> Result<(schema::SummaryReport, stream::StreamStats)> {
    let path = cli.file.to_string_lossy().to_string();

    // Stage 1: structural pre-check (bounded, zeroized).
    let meta = precheck::precheck_vcf(&path)?;
    log::info!(
        "pre-check ok: {} (expected {} columns, FORMAT column: {}, {} samples, {} declared contigs, {} bytes on disk)",
        meta.fileformat,
        meta.expected_columns,
        meta.has_format_column,
        meta.sample_names.len(),
        meta.contigs.len(),
        human_bytes(meta.data_bytes)
    );


    // Stage 2: open the validation session (audit record) and construct
    // the in-process validator. No child process anymore — this is just
    // metadata for traceability.
    let session = SessionInfo {
        session_id: uuid::Uuid::new_v4().to_string(),
        protocol: schema::PROTOCOL_VERSION,
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
        file: sanitize_basename(&cli.file),
        expected_columns: meta.expected_columns as u64,
        chunk_capacity: 64 * 1024,
        max_line: precheck::LINE_CAP as u64,
        sent_at_unix: unix_now(),
    };
    log::info!(
        "session {} opened: file={} protocol={} tool={} expected_columns={}",
        session.session_id,
        session.file,
        session.protocol,
        session.tool_version,
        session.expected_columns
    );
    let mut validator = validate::Validator::new(session.session_id.clone(), session.expected_columns);

    // Stage 3: stream chunks; canonicalize to a file if requested.
    let mut stats = stream::StreamStats::default();
    let summary = match &cli.canonical_out {
        Some(out) => {
            let file = std::fs::File::create(out)?;
            let mut sink = std::io::BufWriter::new(file);
            stream::stream_vcf(
                &path,
                &meta,
                &mut validator,
                Some(&mut sink),
                &mut stats,
                |_| {},
            )?
        }
        None => {
            stream::stream_vcf(&path, &meta, &mut validator, None, &mut stats, |_| {})?
        }
    };

   

    Ok((summary, stats))
}

fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

fn print_report(cli: &Cli, summary: &schema::SummaryReport, stats: &stream::StreamStats) {
    if cli.json {
        let out = serde_json::json!({
            "ok": true,
            "session_id": summary.session_id,
            "total_chunks": stats.chunks,
            "total_lines": stats.lines,
            "total_bytes": stats.bytes,
            "errors": stats.errors,
            "error_codes": summary.error_codes,
            "duration_ms": summary.duration_ms,
            "peak_chunk_ms": stats.peak_chunk_ms,
            "mean_chunk_ms": stats.mean_chunk_ms(),
            "records_canonicalized": stats.records_canonicalized,
        });
        println!("{out}");
        return;
    }

    println!(
        "✓ validated {} — {} lines, {}, {} chunks",
        cli.file.display(),
        stats.lines,
        human_bytes(stats.bytes),
        stats.chunks
    );
    if stats.errors > 0 {
        let codes: Vec<String> = summary
            .error_codes
            .iter()
            .map(|(k, v)| format!("{k}×{v}"))
            .collect();
        println!("  validation errors: {} ({})", stats.errors, codes.join(", "));
    } else {
        println!("  validation errors: none");
    }
    println!(
        "  mean chunk {:.2} ms · peak {} ms · wait {} ms · {} ms total",
        stats.mean_chunk_ms(),
        stats.peak_chunk_ms,
        stats.wait_ms,
        summary.duration_ms
    );
    if let Some(out) = &cli.canonical_out {
        println!(
            "  canonicalized {} records → {}",
            stats.records_canonicalized,
            out.display()
        );
    }
}

fn main() {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info"),
    )
    .init();

    // Ctrl+C raises the interrupt flag; stream.rs checks it at chunk
    // boundaries and surfaces `Error::Interrupted` (exit 130).
    if let Err(e) = ctrlc::set_handler(|| INTERRUPTED.store(true, Ordering::SeqCst)) {
        log::warn!("could not install Ctrl+C handler: {e}");
    }

    let cli = match parse_args() {
        Ok(cli) => cli,
        Err(err) => {
            eprintln!("{err}\n\n{USAGE}");
            std::process::exit(err.exit_code());
        }
    };

    match run(&cli) {
        Ok((summary, stats)) => {
            print_report(&cli, &summary, &stats);
            std::process::exit(0);
        }
        Err(err) => {
            eprintln!("vcf-ingest: {err}");
            if cli.json {
                let out = serde_json::json!({
                    "ok": false,
                    "code": format!("{:?}", err.code()),
                    "message": err.to_string(),
                });
                println!("{out}");
            }
            std::process::exit(err.exit_code());
        }
    }
}