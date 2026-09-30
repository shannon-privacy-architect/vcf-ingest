//! Structured IPC schema shared between the Rust core and the Python
//! sidecar. Every frame crossing the pipe is one of these types, encoded as
//! a single JSON object (serde here, `json` module on the Python side).
//!
//! Field names are snake_case and pinned: changing a field name is a
//! breaking contract change and must bump [`PROTOCOL_VERSION`].

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Bump on any breaking change to the frame schema or wire layout.
pub const PROTOCOL_VERSION: u32 = 1;

/// Per-frame kind byte (first byte of every payload).
pub mod kind {
    /// Rust -> Python: initial session/handshake frame.
    pub const SESSION: u8 = 0x01;
    /// Rust -> Python: one streamed chunk (raw VCF bytes).
    pub const CHUNK: u8 = 0x02;
    /// Rust -> Python: end-of-stream (EOF framing, no payload).
    pub const FINAL: u8 = 0x03;
    /// Python -> Rust: per-chunk validation report.
    pub const REPORT: u8 = 0x81;
    /// Python -> Rust: final summary after stdin closed.
    pub const SUMMARY: u8 = 0x82;
    /// Python -> Rust: fatal protocol refusal (then the child exits).
    #[allow(dead_code)] // sent by the Python sidecar; Rust only ever receives it
    pub const NAK: u8 = 0x83;
}

/// Message sent once at stream start so every later frame is attributable
/// to a session (task: session metadata on all IPC).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionInfo {
    pub session_id: String,          // v4 UUID
    pub protocol: u32,               // PROTOCOL_VERSION
    pub tool_version: String,
    pub file: String,                // basename only, sanitized
    pub expected_columns: u64,       // from the #CHROM line (pre-check result)
    pub chunk_capacity: u64,         // Rust chunk buffer capacity
    pub max_line: u64,               // hard line cap in bytes
    pub sent_at_unix: u64,           // unix epoch seconds (UTC)
}

/// One validation finding. `message` is a template with **no raw content**.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ValidationError {
    pub code: String,            // e.g. "COLUMN_COUNT", "CFLAG", "CHROM_NOMENCLATURE"
    pub message: String,         // metadata-only, human readable
    pub line: Option<u64>,       // 1-based VCF line number
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub found: Option<u64>,
}

/// A canonicalized variant record (task 8). Deterministic field order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CanonicalRecord {
    pub index: u64,              // ordinal of this record in the file
    pub chrom: String,
    pub pos: u64,
    pub id: String,
    pub reference: String,
    pub alt: Vec<String>,
    pub qual: Option<String>,    // kept raw (".", "12.3"); semantic parsers may
    pub filter: Vec<String>,
    pub info: Vec<InfoField>,    // INFO k=v pairs, insertion order
    pub format: Vec<String>,     // FORMAT fields, or empty
    pub samples: Vec<Vec<String>>, // per-sample field values, row-major
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InfoField {
    pub key: String,
    pub value: Option<String>,
}

/// Python -> Rust, one per chunk.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ChunkReport {
    pub session_id: String,
    pub chunk_index: u64,
    pub bytes: u64,
    pub lines: u64,
    pub errors: Vec<ValidationError>,
    pub records: Vec<CanonicalRecord>,
}

/// Python -> Rust, once after stdin EOF.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct SummaryReport {
    pub session_id: String,
    pub total_chunks: u64,
    pub total_lines: u64,
    pub total_bytes: u64,
    pub errors: u64,
    pub error_codes: BTreeMap<String, u64>,
    pub duration_ms: u64,
    pub peak_chunk_ms: u64,
    pub mean_chunk_ms: u64,
}