//! Checkpoint-based migration.
//!
//! Flow: capture source -> seal checkpoint -> validate target compatibility ->
//! restore target -> validate target -> transfer workload authority (fence source,
//! grant target) -> stop/retire source.
//!
//! Authority transfer is explicit. Fencing tokens and epochs prevent both source
//! and target from believing they are the sole active continuation.

use serde::{Deserialize, Serialize};

use crate::errors::{FabricError, FabricResult};
use crate::id::Id;

/// Steps of a migration, recorded in the recovery journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationStep {
    Planned,
    Captured,
    SourceFenced,
    TargetRestored,
    AuthorityTransferred,
    SourceStopped,
}

impl MigrationStep {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Captured => "captured",
            Self::SourceFenced => "source_fenced",
            Self::TargetRestored => "target_restored",
            Self::AuthorityTransferred => "authority_transferred",
            Self::SourceStopped => "source_stopped",
        }
    }

    pub fn parse_str(s: &str) -> Self {
        match s {
            "planned" => Self::Planned,
            "captured" => Self::Captured,
            "source_fenced" => Self::SourceFenced,
            "target_restored" => Self::TargetRestored,
            "authority_transferred" => Self::AuthorityTransferred,
            _ => Self::SourceStopped,
        }
    }
}

/// A migration plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationPlan {
    pub migration_id: String,
    pub workload_id: Id,
    pub checkpoint_id: Id,
    pub source_node: String,
    pub target_node: String,
    pub authority_token: String,
    pub fence_epoch: u64,
}

/// Validate a migration request's basics.
pub fn validate_migration_request(
    workload_id: &Id,
    checkpoint_id: &Id,
    source_node: &str,
    target_node: &str,
) -> FabricResult<()> {
    if source_node == target_node {
        return Err(FabricError::MigrationFailure(
            "migration source and target are the same node".into(),
        ));
    }
    if workload_id == checkpoint_id {
        return Err(FabricError::InvalidArgument(
            "workload and checkpoint ids must differ".into(),
        ));
    }
    if source_node.is_empty() || target_node.is_empty() {
        return Err(FabricError::InvalidArgument(
            "node names must not be empty".into(),
        ));
    }
    Ok(())
}

/// Fencing token generation (128-bit hex).
pub fn new_fence_token() -> String {
    Id::random().to_hex()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_node_rejected() {
        let wid = Id::random();
        let cid = Id::random();
        assert!(validate_migration_request(&wid, &cid, "n1", "n1").is_err());
        assert!(validate_migration_request(&wid, &cid, "n1", "n2").is_ok());
        assert!(validate_migration_request(&wid, &cid, "", "n2").is_err());
    }

    #[test]
    fn step_roundtrip() {
        for s in [
            MigrationStep::Planned,
            MigrationStep::Captured,
            MigrationStep::SourceFenced,
            MigrationStep::TargetRestored,
            MigrationStep::AuthorityTransferred,
            MigrationStep::SourceStopped,
        ] {
            assert_eq!(MigrationStep::parse_str(s.as_str()), s);
        }
    }

    #[test]
    fn tokens_unique() {
        assert_ne!(new_fence_token(), new_fence_token());
    }
}
