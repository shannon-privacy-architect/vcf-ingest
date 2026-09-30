//! Structural pre-check of the VCF file before streaming begins.
//!
//! Reads a **bounded head** (64 KiB) into a zeroized scratch buffer and
//! validates:
//! 1. file exists and is non-empty;
//! 2. `##fileformat=VCFv4` with an accepted minor version (`VCFv4`, `.0`–`.3`);
//! 3. a `#CHROM` column line exists; its column count is the expected width;
//! 4. the first data rows are tab-delimited with a matching column count and
//!    a positive-integer `POS`;
//! 5. chromosome names match the declared `##contig` IDs (if any) or the
//!    standard convention: `chr`-optional `1..22`, `X`, `Y`, `M`, `MT`.
//!
//! The scratch buffer is erased on every exit path — several early returns
//! (I/O failure, invalid UTF-8, seek failure) erase independently, and the
//! main scan path erases once at a single boundary below, before any error
//! from the scan itself is surfaced.
//!
//! Truncation guard: `HEAD_SIZE` is a byte cutoff, not a line cutoff, so for
//! any file larger than the head sample, the last fragment of the scan may
//! be a line cut off mid-field by the 64 KiB boundary rather than a real
//! row. That fragment is detected and dropped before validation runs on it
//! (see `file_larger_than_head` below) — otherwise a well-formed file could
//! fail pre-check purely because of where the boundary happened to land.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use crate::error::Result;
use crate::erase::erase_buffer;

/// Head sample budget: only this many bytes enter the pre-check.
pub const HEAD_SIZE: u64 = 64 * 1024;
/// Hard cap for a single VCF line (see PRD §9).
pub const LINE_CAP: usize = 8 * 1024 * 1024;
/// Fixed `#CHROM` columns: CHROM POS ID REF ALT QUAL FILTER INFO.
pub const FIXED_COLUMNS: usize = 8;

/// Everything the streaming phase needs to know up front.
#[derive(Debug, Clone)]
pub struct PrecheckMeta {
    pub fileformat: String,
    pub expected_columns: usize,
    pub has_format_column: bool,
    pub sample_names: Vec<String>,
    pub contigs: BTreeSet<String>,
    pub data_bytes: u64,
}

#[derive(Debug)]
pub enum PrecheckError {
    Io(std::io::Error),
    EmptyFile,
    GzUnsupported,
    MissingFileformat,
    UnsupportedVersion(String),
    MissingChromLine,
    InvalidColumnWidth { line: u64, expected: usize, found: usize },
    UndeclaredContig { chrom: String, line: u64 },
    BadPosition { value: String, line: u64 },
}

impl std::fmt::Display for PrecheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrecheckError::Io(e) => write!(f, "i/o error during pre-check: {e}"),
            PrecheckError::EmptyFile => write!(f, "file is empty"),
            PrecheckError::GzUnsupported => {
                write!(f, ".gz input rejected: decompression is out of scope (V1)")
            }
            PrecheckError::MissingFileformat => {
                write!(f, "missing required ##fileformat=VCFv4 header line")
            }
            PrecheckError::UnsupportedVersion(v) => {
                write!(f, "unsupported ##fileformat version: {v}")
            }
            PrecheckError::MissingChromLine => {
                write!(f, "missing mandatory #CHROM column line")
            }
            PrecheckError::InvalidColumnWidth { line, expected, found } => write!(
                f,
                "line {line}: expected {expected} tab-separated columns, found {found}"
            ),
            PrecheckError::UndeclaredContig { chrom, line } => write!(
                f,
                "line {line}: chromosome \"{chrom}\" is not in the declared contigs or standard set"
            ),
            PrecheckError::BadPosition { value, line } => {
                write!(f, "line {line}: POS \"{value}\" is not a positive integer")
            }
        }
    }
}
impl std::error::Error for PrecheckError {}

/// Parse `##fileformat=VCFv4(.N)` strictly: `VCFv4` or `VCFv4.0`–`VCFv4.3`.
fn accepted_version(line: &str) -> Option<String> {
    let line = line.trim_end_matches(['\n', '\r']);
    let rest = line.strip_prefix("##fileformat=")?;
    if rest == "VCFv4" {
        return Some(rest.to_string());
    }
    let minor = rest.strip_prefix("VCFv4.")?;
    match minor.parse::<u32>() {
        Ok(v) if v <= 3 => Some(rest.to_string()),
        _ => None,
    }
}

/// Extract the `ID=` from a `##contig=<ID=...,...>` meta line.
fn contig_id(line: &str) -> Option<String> {
    let body = line.strip_prefix("##contig=<")?;
    let first = body.split(',').next()?.trim();
    let id = first.strip_prefix("ID=")?;
    Some(id.trim_end_matches('>').to_string())
}

/// Is `chrom` acceptable? Either it is explicitly declared in the header's
/// `##contig` records, or it matches the conventional set.
fn is_valid_chrom(raw: &str, declared: &BTreeSet<String>) -> bool {
    if declared.contains(raw) {
        return true;
    }
    let bare = raw.strip_prefix("chr").unwrap_or(raw);
    matches!(
        bare,
        "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "10"
            | "11" | "12" | "13" | "14" | "15" | "16" | "17" | "18" | "19"
            | "20" | "21" | "22" | "X" | "Y" | "M" | "MT"
    )
}

pub fn precheck_vcf(path: &str) -> Result<PrecheckMeta> {
    if path.ends_with(".gz") {
        return Err(PrecheckError::GzUnsupported.into());
    }

    let mut file = File::open(path).map_err(PrecheckError::Io)?;
    let data_bytes = file.metadata().map_err(PrecheckError::Io)?.len();
    if data_bytes == 0 {
        return Err(PrecheckError::EmptyFile.into());
    }

    // Bounded head read into a zeroized scratch buffer. Head lines are
    // parsed as borrows of `scratch` (no raw copies), and the buffer is
    // erased at the single boundary below — before any error is produced.
    let mut scratch: Vec<u8> = Vec::with_capacity(HEAD_SIZE as usize);
    if let Err(e) = file.by_ref().take(HEAD_SIZE).read_to_end(&mut scratch) {
        erase_buffer(&mut scratch);
        return Err(PrecheckError::Io(e).into());
    }
    let head = match std::str::from_utf8(&scratch) {
        Ok(h) => h,
        Err(_) => {
            let e = PrecheckError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "head sample is not valid UTF-8",
            ));
            erase_buffer(&mut scratch);
            return Err(e.into());
        }
    };

    // Rewind so the streaming pass reads the whole file from offset 0.
    if let Err(e) = file.seek(SeekFrom::Start(0)) {
        erase_buffer(&mut scratch);
        return Err(PrecheckError::Io(e).into());
    }

    // ---- left-to-right scan of the head ----
    // `HEAD_SIZE` is a byte cutoff, not a line cutoff. If the file is bigger
    // than the head sample, the last fragment from `split_inclusive` may be
    // a line truncated mid-field by that boundary rather than a real row —
    // it must be dropped, not validated as data.
    let file_larger_than_head = data_bytes > HEAD_SIZE;
    let fragments: Vec<&str> = head.split_inclusive('\n').collect();
    let last_idx = fragments.len().saturating_sub(1);

    let mut fileformat: Option<String> = None;
    let mut contigs: BTreeSet<String> = BTreeSet::new();
    let mut expected_columns: Option<usize> = None;
    let mut sample_names: Vec<String> = Vec::new();
    let mut has_format_column = false;
    let mut seen_data_row = false;
    let mut line_no: u64 = 0;
    let mut err: Option<PrecheckError> = None;

    for (i, raw) in fragments.iter().enumerate() {
        // Drop a truncated final fragment before it's ever counted as a
        // line or checked against any of the rules below.
        if i == last_idx && file_larger_than_head && !raw.ends_with('\n') {
            break;
        }
        line_no += 1;
        let l = raw.trim_end_matches(['\n', '\r']);

        if l.starts_with("##") {
            if let Some(ver) = accepted_version(l) {
                if fileformat.is_none() {
                    fileformat = Some(ver);
                }
            } else if l.starts_with("##fileformat=") && fileformat.is_none() {
                let version = l.strip_prefix("##fileformat=").unwrap_or(l).to_string();
                err = Some(PrecheckError::UnsupportedVersion(version));
                break;
            }
            if let Some(id) = contig_id(l) {
                contigs.insert(id);
            }
            continue;
        }

        if l.starts_with("#CHROM") {
            let cols: Vec<&str> = l.split('\t').collect();
            if cols.len() < FIXED_COLUMNS {
                err = Some(PrecheckError::InvalidColumnWidth {
                    line: line_no,
                    expected: FIXED_COLUMNS,
                    found: cols.len(),
                });
                break;
            }
            expected_columns = Some(cols.len());
            has_format_column = cols.iter().any(|&c| c == "FORMAT");
            if has_format_column {
                sample_names = cols[FIXED_COLUMNS + 1..]
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
            }
            continue;
        }

        if l.is_empty() {
            continue;
        }

        // First data row: enforce the contract from the header.
        if !seen_data_row {
            if fileformat.is_none() {
                err = Some(PrecheckError::MissingFileformat);
                break;
            }
            if expected_columns.is_none() {
                err = Some(PrecheckError::MissingChromLine);
                break;
            }
            seen_data_row = true;
        }

        let cols: Vec<&str> = l.split('\t').collect();
        if cols.len() != expected_columns.unwrap() {
            err = Some(PrecheckError::InvalidColumnWidth {
                line: line_no,
                expected: expected_columns.unwrap(),
                found: cols.len(),
            });
            break;
        }

        let chrom = cols[0];
        if !is_valid_chrom(chrom, &contigs) {
            err = Some(PrecheckError::UndeclaredContig {
                chrom: chrom.to_string(),
                line: line_no,
            });
            break;
        }

        let pos = cols[1];
        if pos.parse::<u64>().map(|p| p == 0).unwrap_or(true) {
            err = Some(PrecheckError::BadPosition {
                value: pos.to_string(),
                line: line_no,
            });
            break;
        }
    }

    // Single boundary: raw bytes are gone before any error is surfaced.
    erase_buffer(&mut scratch);

    let fileformat = match err {
        Some(e) => return Err(e.into()),
        None => fileformat.ok_or(PrecheckError::MissingFileformat)?,
    };
    // A VCF with only meta/#CHROM lines and zero data rows is still valid.
    let expected_columns = expected_columns.unwrap_or(FIXED_COLUMNS);

    Ok(PrecheckMeta {
        fileformat,
        expected_columns,
        has_format_column,
        sample_names,
        contigs,
        data_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_tiny_valid() {
        let meta = precheck_vcf("fixtures/tiny-valid.vcf").unwrap();
        assert_eq!(meta.expected_columns, 8);
        assert!(!meta.has_format_column);
        assert!(meta.sample_names.is_empty());
    }

    #[test]
    fn rejects_bad_fileformat() {
        assert!(precheck_vcf("fixtures/bad-fileformat.vcf").is_err());
    }

    #[test]
    fn rejects_missing_header() {
        assert!(precheck_vcf("fixtures/missing-header.vcf").is_err());
    }

    #[test]
    fn rejects_bad_columns() {
        assert!(precheck_vcf("fixtures/bad-cols.vcf").is_err());
    }

    #[test]
    fn rejects_empty() {
        assert!(precheck_vcf("fixtures/empty.vcf").is_err());
    }
}