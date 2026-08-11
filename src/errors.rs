//! Typed error model for Checkpoint Fabric.
//!
//! Ordinary runtime failures are reported through [`FabricError`] and never via panics.
//! Error categories follow the classification required by the 1.0.0 specification.

use std::fmt;

/// Categorized runtime error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FabricError {
    InvalidLifecycleTransition {
        from: String,
        to: String,
    },
    StaleCoordinatorEpoch {
        expected: u64,
        got: u64,
    },
    StaleWorkloadEpoch {
        expected: u64,
        got: u64,
    },
    StaleCaptureAttempt(String),
    StaleRestoreAttempt(String),
    ReservationConflict(String),
    QuiescenceFailure(String),
    CaptureProviderFailure(String),
    IncompleteCheckpoint(String),
    FrontierInconsistency(String),
    IntegrityFailure(String),
    CompatibilityFailure(String),
    MissingDependency(String),
    UnsupportedBackend(String),
    UnsupportedComponent(String),
    CorruptedCheckpoint(String),
    PersistenceError(String),
    TransportError(String),
    ProtocolError(String),
    StorageError(String),
    RestoreFailure(String),
    MigrationFailure(String),
    FencingFailure(String),
    LineageViolation(String),
    PolicyViolation(String),
    CheckpointNotFound(String),
    WorkloadNotFound(String),
    GenerationMismatch(String),
    Timeout(String),
    CleanupFailure(String),
    InvalidArgument(String),
    Internal(String),
    Io(String),
    Json(String),
    Sqlite(String),
    /// Deliberate injected failure used by failure-injection tests.
    FailPoint(String),
}

pub type FabricResult<T> = Result<T, FabricError>;

impl fmt::Display for FabricError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLifecycleTransition { from, to } => {
                write!(f, "invalid lifecycle transition: {from} -> {to}")
            }
            Self::StaleCoordinatorEpoch { expected, got } => write!(
                f,
                "stale coordinator epoch: coordinator expects {expected}, actor holds {got}"
            ),
            Self::StaleWorkloadEpoch { expected, got } => {
                write!(f, "stale workload epoch: expected {expected}, got {got}")
            }
            Self::StaleCaptureAttempt(s) => write!(f, "stale capture attempt: {s}"),
            Self::StaleRestoreAttempt(s) => write!(f, "stale restore attempt: {s}"),
            Self::ReservationConflict(s) => write!(f, "reservation conflict: {s}"),
            Self::QuiescenceFailure(s) => write!(f, "quiescence failure: {s}"),
            Self::CaptureProviderFailure(s) => write!(f, "capture provider failure: {s}"),
            Self::IncompleteCheckpoint(s) => write!(f, "incomplete checkpoint: {s}"),
            Self::FrontierInconsistency(s) => write!(f, "execution frontier inconsistency: {s}"),
            Self::IntegrityFailure(s) => write!(f, "integrity failure: {s}"),
            Self::CompatibilityFailure(s) => write!(f, "compatibility failure: {s}"),
            Self::MissingDependency(s) => write!(f, "missing dependency: {s}"),
            Self::UnsupportedBackend(s) => write!(f, "unsupported backend: {s}"),
            Self::UnsupportedComponent(s) => write!(f, "unsupported component: {s}"),
            Self::CorruptedCheckpoint(s) => write!(f, "corrupted checkpoint: {s}"),
            Self::PersistenceError(s) => write!(f, "persistence error: {s}"),
            Self::TransportError(s) => write!(f, "transport error: {s}"),
            Self::ProtocolError(s) => write!(f, "protocol error: {s}"),
            Self::StorageError(s) => write!(f, "storage error: {s}"),
            Self::RestoreFailure(s) => write!(f, "restore failure: {s}"),
            Self::MigrationFailure(s) => write!(f, "migration failure: {s}"),
            Self::FencingFailure(s) => write!(f, "fencing failure: {s}"),
            Self::LineageViolation(s) => write!(f, "lineage violation: {s}"),
            Self::PolicyViolation(s) => write!(f, "policy violation: {s}"),
            Self::CheckpointNotFound(s) => write!(f, "checkpoint not found: {s}"),
            Self::WorkloadNotFound(s) => write!(f, "workload not found: {s}"),
            Self::GenerationMismatch(s) => write!(f, "generation mismatch: {s}"),
            Self::Timeout(s) => write!(f, "timeout: {s}"),
            Self::CleanupFailure(s) => write!(f, "cleanup failure: {s}"),
            Self::InvalidArgument(s) => write!(f, "invalid argument: {s}"),
            Self::Internal(s) => write!(f, "internal error: {s}"),
            Self::Io(s) => write!(f, "io error: {s}"),
            Self::Json(s) => write!(f, "serialization error: {s}"),
            Self::Sqlite(s) => write!(f, "sqlite error: {s}"),
            Self::FailPoint(s) => write!(f, "injected failpoint: {s}"),
        }
    }
}

impl std::error::Error for FabricError {}

impl From<std::io::Error> for FabricError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

impl From<serde_json::Error> for FabricError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e.to_string())
    }
}

impl From<rusqlite::Error> for FabricError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Sqlite(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_roundtrip() {
        let e = FabricError::InvalidLifecycleTransition {
            from: "AVAILABLE".into(),
            to: "CAPTURING".into(),
        };
        assert!(e.to_string().contains("AVAILABLE"));
        assert!(e.to_string().contains("CAPTURING"));
    }
}
