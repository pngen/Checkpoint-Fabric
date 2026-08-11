//! External side-effect reasoning.
//!
//! Checkpoint Fabric reasons about non-idempotent external side effects so that
//! restore never blindly re-executes already-committed actions.

use serde::{Deserialize, Serialize};

use crate::errors::{FabricError, FabricResult};

/// Classification of an external side effect at capture time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SideEffectStatus {
    Uncommitted,
    Committed,
    Replayable,
    NonReplayable,
    Unknown,
}

impl SideEffectStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Uncommitted => "UNCOMMITTED",
            Self::Committed => "COMMITTED",
            Self::Replayable => "REPLAYABLE",
            Self::NonReplayable => "NON_REPLAYABLE",
            Self::Unknown => "UNKNOWN",
        }
    }
}

/// One tracked external side effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SideEffectEntry {
    pub side_effect_id: String,
    pub kind: String,
    pub status: SideEffectStatus,
    pub idempotency_key: Option<String>,
    pub committed_ms: Option<u64>,
}

/// The side-effect manifest of a workload frontier.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SideEffectManifest {
    pub entries: Vec<SideEffectEntry>,
    /// Number of entries at the durable boundary (all before this index are COMMITTED).
    pub frontier_boundary: u64,
}

impl SideEffectManifest {
    /// Validate that the manifest does not prevent the requested resumability class.
    ///
    /// A checkpoint with unresolved UNKNOWN side-effect state must not claim exact
    /// resumability, and an uncommitted NON_REPLAYABLE side effect blocks exact
    /// resumption entirely (it cannot be safely redone or omitted).
    pub fn validate_for_exact(&self) -> FabricResult<()> {
        for e in &self.entries {
            match e.status {
                SideEffectStatus::Unknown => {
                    return Err(FabricError::FrontierInconsistency(format!(
                        "side effect {} has UNKNOWN state; exact resumability cannot be claimed",
                        e.side_effect_id
                    )));
                }
                SideEffectStatus::NonReplayable => {
                    return Err(FabricError::FrontierInconsistency(format!(
                        "side effect {} is NON_REPLAYABLE and not committed; capture must not \
                         cross an unresolved non-idempotent side-effect boundary",
                        e.side_effect_id
                    )));
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn classify(&self, side_effect_id: &str) -> Option<SideEffectStatus> {
        self.entries
            .iter()
            .find(|e| e.side_effect_id == side_effect_id)
            .map(|e| e.status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, status: SideEffectStatus) -> SideEffectEntry {
        SideEffectEntry {
            side_effect_id: id.into(),
            kind: "database_commit".into(),
            status,
            idempotency_key: Some(format!("op-{id}")),
            committed_ms: None,
        }
    }

    #[test]
    fn committed_non_replayable_is_safe() {
        let m = SideEffectManifest {
            entries: vec![entry("a", SideEffectStatus::Committed)],
            frontier_boundary: 1,
        };
        assert!(m.validate_for_exact().is_ok());
    }

    #[test]
    fn unknown_blocks_exact() {
        let m = SideEffectManifest {
            entries: vec![entry("a", SideEffectStatus::Unknown)],
            frontier_boundary: 0,
        };
        let err = m.validate_for_exact().unwrap_err();
        assert!(matches!(err, FabricError::FrontierInconsistency(_)));
    }

    #[test]
    fn uncommitted_non_replayable_blocks_exact() {
        let m = SideEffectManifest {
            entries: vec![entry("a", SideEffectStatus::NonReplayable)],
            frontier_boundary: 0,
        };
        assert!(m.validate_for_exact().is_err());
    }

    #[test]
    fn replayable_is_fine() {
        let m = SideEffectManifest {
            entries: vec![entry("a", SideEffectStatus::Replayable)],
            frontier_boundary: 0,
        };
        assert!(m.validate_for_exact().is_ok());
    }
}
