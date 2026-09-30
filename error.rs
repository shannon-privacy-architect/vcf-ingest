//! Typed, expressive error surface for every ingestion stage.
//!
//! Every failure is modelled as an [`Error`] that:
//! - carries a stable, machine-readable [`ErrorCode`] (used in `--json` mode),
//! - maps to a documented process exit code (PRD §5),
//! - renders a human `Display` that is **metadata-only** — it never
//!   interpolates raw VCF content.

use serde::Serialize;
use std::fmt;

/// Stable machine-readable error codes. These are part of the IPC/report
/// contract: Python relies on them, dashboards key on them. Do not renumber.
/// Some codes (Serde, Canonical) are reserved for stages wired later; they
/// are part of the documented contract even when not yet constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[allow(dead_code)]
pub enum ErrorCode {
    /// CLI misuse (bad arguments, unknown flag). Exit 5.
    Usage,
    /// I/O failure reading the input file. Exit 2.
    Io,
    /// Structural pre-check failure (bad format, missing header…). Exit 2.
    Precheck,
    /// Could not spawn or talk to the Python sidecar. Exit 3.
    Spawn,
    /// The sidecar crashed mid-stream or died early. Exit 4.
    ValidatorCrash,
    /// The sidecar did not answer within the deadline. Exit 4.
    ValidatorTimeout,
    /// The pipe produced bytes that violate the frame contract. Exit 4.
    Protocol,
    /// JSON (de)serialization failure on a control frame. Exit 4.
    Serde,
    /// Canonicalization / circuit-prep stage failed. Exit 2.
    Canonical,
    /// Hard memory cap exceeded (single line > cap). Exit 2.
    InputTooBig,
    /// Interrupted by SIGINT. Exit 130.
    Interrupted,
}

impl ErrorCode {
    pub fn exit_code(self) -> i32 {
        match self {
            ErrorCode::Usage => 5,
            ErrorCode::Io | ErrorCode::Precheck | ErrorCode::InputTooBig | ErrorCode::Canonical => 2,
            ErrorCode::Spawn => 3,
            ErrorCode::ValidatorCrash | ErrorCode::ValidatorTimeout | ErrorCode::Protocol | ErrorCode::Serde => 4,
            ErrorCode::Interrupted => 130,
        }
    }
}

/// The typed error the whole CLI funnels through.
/// `Canonical` is reserved for the circuit-prep stage (task 9).
#[derive(Debug)]
#[allow(dead_code)]
pub enum Error {
    Usage(String),
    Io { context: &'static str, source: std::io::Error },
    Precheck(crate::precheck::PrecheckError),
    Spawn { interpreter: String, source: std::io::Error },
    ValidatorCrashed { exit: Option<i32> },
    ValidatorTimeout { secs: u64 },
    Protocol(String),
    Serde(serde_json::Error),
    Canonical(String),
    InputTooBig { bytes: usize, limit: usize },
    Interrupted,
}

impl Error {
    /// Documented exit code for this error (PRD §5).
    pub fn exit_code(&self) -> i32 {
        match self {
            Error::Usage(_) => ErrorCode::Usage.exit_code(),
            Error::Io { .. } => ErrorCode::Io.exit_code(),
            Error::Precheck(_) => ErrorCode::Precheck.exit_code(),
            Error::Spawn { .. } => ErrorCode::Spawn.exit_code(),
            Error::ValidatorCrashed { .. } => ErrorCode::ValidatorCrash.exit_code(),
            Error::ValidatorTimeout { .. } => ErrorCode::ValidatorTimeout.exit_code(),
            Error::Protocol(_) | Error::Serde(_) => ErrorCode::Protocol.exit_code(),
            Error::Canonical(_) => ErrorCode::Canonical.exit_code(),
            Error::InputTooBig { .. } => ErrorCode::InputTooBig.exit_code(),
            Error::Interrupted => ErrorCode::Interrupted.exit_code(),
        }
    }

    pub fn code(&self) -> ErrorCode {
        match self {
            Error::Usage(_) => ErrorCode::Usage,
            Error::Io { .. } => ErrorCode::Io,
            Error::Precheck(_) => ErrorCode::Precheck,
            Error::Spawn { .. } => ErrorCode::Spawn,
            Error::ValidatorCrashed { .. } => ErrorCode::ValidatorCrash,
            Error::ValidatorTimeout { .. } => ErrorCode::ValidatorTimeout,
            Error::Protocol(_) | Error::Serde(_) => ErrorCode::Protocol,
            Error::Canonical(_) => ErrorCode::Canonical,
            Error::InputTooBig { .. } => ErrorCode::InputTooBig,
            Error::Interrupted => ErrorCode::Interrupted,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Usage(m) => write!(f, "usage error: {m}"),
            Error::Io { context, source } => write!(f, "i/o failure while {context}: {source}"),
            Error::Precheck(e) => write!(f, "{e}"),
            Error::Spawn { interpreter, source } => {
                write!(f, "could not start Python sidecar via `{interpreter}`: {source}")
            }
            Error::ValidatorCrashed { exit } => match exit {
                Some(c) => write!(f, "validator crashed with exit code {c}"),
                None => write!(f, "validator crashed (no exit code captured)"),
            },
            Error::ValidatorTimeout { secs } => write!(f, "validator did not respond within {secs}s"),
            Error::Protocol(m) => write!(f, "ipc protocol violation: {m}"),
            Error::Serde(e) => write!(f, "structured frame could not be parsed: {e}"),
            Error::Canonical(m) => write!(f, "canonicalization failed: {m}"),
            Error::InputTooBig { bytes, limit } => {
                write!(f, "line exceeds the {limit} byte cap ({bytes} bytes read) — see PRD Open Questions")
            }
            Error::Interrupted => write!(f, "interrupted"),
        }
    }
}
impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io { context: "file stream", source: e }
    }
}
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Serde(e)
    }
}
impl From<crate::precheck::PrecheckError> for Error {
    fn from(e: crate::precheck::PrecheckError) -> Self {
        Error::Precheck(e)
    }
}

/// A `Result` alias for the whole crate.
pub type Result<T> = std::result::Result<T, Error>;