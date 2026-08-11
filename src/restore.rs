//! Restore orchestration: planning, validation, commit, and cleanup.

use serde::{Deserialize, Serialize};

use crate::checkpoint::{CheckpointObject, ResumabilityClass};
use crate::errors::{FabricError, FabricResult};
use crate::lifecycle::LifecycleState;
use crate::policy::PolicySet;
use crate::workload::Workload;

/// Options for a restore request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RestoreOptions {
    /// Resume execution after restore (as opposed to restore-and-hold).
    pub resume: bool,
    /// Whether this restore is a rollback (bumps the workload generation).
    pub rollback: bool,
    /// Whether this restore is part of a migration.
    pub migration: bool,
    /// Fencing token presented by the target continuation.
    pub target_fence_token: Option<String>,
    /// Resolved Context Fabric object references (see [`crate::integrations`]).
    pub resolved_context_refs: Vec<crate::integrations::ContextRefResolution>,
}

impl Default for RestoreOptions {
    fn default() -> Self {
        Self {
            resume: true,
            rollback: false,
            migration: false,
            target_fence_token: None,
            resolved_context_refs: Vec::new(),
        }
    }
}

/// Validate a restore request against the checkpoint and policy.
pub fn validate_restore_request(
    checkpoint: &CheckpointObject,
    policy: &PolicySet,
    target_node: &str,
) -> FabricResult<()> {
    if !checkpoint.lifecycle.is_restorable() {
        return Err(FabricError::CorruptedCheckpoint(format!(
            "checkpoint {} is not restorable in lifecycle state {}",
            checkpoint.checkpoint_id,
            checkpoint.lifecycle.as_str()
        )));
    }
    if checkpoint.lifecycle == LifecycleState::Retired {
        // handled by is_restorable returning false; explicit check for clarity
        return Err(FabricError::PolicyViolation(
            "retired checkpoint requires archive-restore policy".into(),
        ));
    }
    if checkpoint.integrity_state == crate::checkpoint::IntegrityState::Corrupt {
        return Err(FabricError::CorruptedCheckpoint(format!(
            "checkpoint {} is marked corrupt; restore refused",
            checkpoint.checkpoint_id
        )));
    }
    if checkpoint.replica_count < policy.min_valid_replicas {
        return Err(FabricError::CorruptedCheckpoint(format!(
            "checkpoint {} has {} replicas but policy requires {}",
            checkpoint.checkpoint_id, checkpoint.replica_count, policy.min_valid_replicas
        )));
    }
    if checkpoint.durable_locations.is_empty() {
        return Err(FabricError::CorruptedCheckpoint(format!(
            "checkpoint {} has no durable locations",
            checkpoint.checkpoint_id
        )));
    }
    if checkpoint
        .durable_locations
        .iter()
        .any(|l| l.node == target_node && !l.verified)
    {
        // The replica on the target node is unverified; verification will run
        // during restore regardless, so this is informational.
        let _ = target_node;
    }
    Ok(())
}

/// Validate that all required Context Fabric references are resolved.
pub fn validate_context_refs(
    checkpoint: &CheckpointObject,
    resolved: &[crate::integrations::ContextRefResolution],
) -> FabricResult<()> {
    for r in resolved {
        if r.required {
            let found = checkpoint
                .dependencies
                .iter()
                .any(|d| d.kind == "context" && d.identity == r.object_id.to_hex());
            if !found {
                return Err(FabricError::MissingDependency(format!(
                    "required context object {} is not referenced by this checkpoint",
                    r.object_id
                )));
            }
        }
    }
    Ok(())
}

/// Derive the honest post-restore resumability class.
pub fn restored_resumability(
    captured: ResumabilityClass,
    compat_verdict: crate::compatibility::CompatVerdict,
    resumed: bool,
) -> ResumabilityClass {
    use crate::compatibility::CompatVerdict as V;
    let base = match compat_verdict {
        V::Compatible => captured,
        V::CompatibleWithTranslation => match captured {
            ResumabilityClass::Exact => ResumabilityClass::Equivalent,
            other => other,
        },
        V::CompatibleDegraded => match captured {
            ResumabilityClass::Exact | ResumabilityClass::Equivalent => ResumabilityClass::Degraded,
            other => other,
        },
        V::Incompatible => ResumabilityClass::NonResumable,
    };
    if !resumed && base > ResumabilityClass::RestartFromCheckpoint {
        ResumabilityClass::RestartFromCheckpoint
    } else {
        base
    }
}

/// Validate target authority claim during restore commit.
pub fn validate_target_claim(
    workload: &Workload,
    target_node: &str,
    migration: bool,
) -> FabricResult<()> {
    if workload.single_active {
        if let Some(active) = &workload.active_node {
            if active != target_node && active != "MIGRATING" && !migration {
                return Err(FabricError::FencingFailure(format!(
                    "restore target {target_node} is not the active node ({active}); \
                     single-active workloads require migration or an explicit fence"
                )));
            }
        }
    }
    Ok(())
}

/// Whether a restore is eligible given checkpoint protection.
pub fn restore_eligible(checkpoint: &CheckpointObject, policy: &PolicySet) -> FabricResult<()> {
    if checkpoint.lifecycle == LifecycleState::Retired && !policy.archive_restore_permitted {
        return Err(FabricError::PolicyViolation(
            "archived checkpoint restore requires archive_restore_permitted policy".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{
        DurableLocation, IntegrityState, ProtectionState, RetirementEligibility,
    };
    use crate::compatibility::sample_checkpoint;
    use crate::compatibility::CompatVerdict;

    fn ckpt() -> CheckpointObject {
        let mut c = sample_checkpoint();
        c.durable_locations = vec![DurableLocation {
            node: "n1".into(),
            path: "/x".into(),
            verified: true,
        }];
        c.replica_count = 1;
        c
    }

    #[test]
    fn restore_validation_cases() {
        let p = PolicySet::default();
        let c = ckpt();
        assert!(validate_restore_request(&c, &p, "n2").is_ok());

        let mut retired = c.clone();
        retired.lifecycle = LifecycleState::Retired;
        assert!(validate_restore_request(&retired, &p, "n2").is_err());

        let mut corrupt = c.clone();
        corrupt.integrity_state = IntegrityState::Corrupt;
        assert!(validate_restore_request(&corrupt, &p, "n2").is_err());

        let mut failed = c.clone();
        failed.lifecycle = LifecycleState::Failed;
        assert!(validate_restore_request(&failed, &p, "n2").is_err());

        let mut no_replicas = c.clone();
        no_replicas.durable_locations.clear();
        no_replicas.replica_count = 0;
        assert!(validate_restore_request(&no_replicas, &p, "n2").is_err());

        let mut not_enough = c.clone();
        not_enough.replica_count = 1;
        let p2 = PolicySet {
            min_valid_replicas: 2,
            ..PolicySet::default()
        };
        assert!(validate_restore_request(&not_enough, &p2, "n2").is_err());
    }

    #[test]
    fn restored_resumability_never_overstated() {
        assert_eq!(
            restored_resumability(ResumabilityClass::Exact, CompatVerdict::Compatible, true),
            ResumabilityClass::Exact
        );
        assert_eq!(
            restored_resumability(
                ResumabilityClass::Exact,
                CompatVerdict::CompatibleWithTranslation,
                true
            ),
            ResumabilityClass::Equivalent
        );
        assert_eq!(
            restored_resumability(
                ResumabilityClass::Exact,
                CompatVerdict::CompatibleDegraded,
                true
            ),
            ResumabilityClass::Degraded
        );
        assert_eq!(
            restored_resumability(ResumabilityClass::Exact, CompatVerdict::Incompatible, true),
            ResumabilityClass::NonResumable
        );
        assert_eq!(
            restored_resumability(ResumabilityClass::Exact, CompatVerdict::Compatible, false),
            ResumabilityClass::RestartFromCheckpoint
        );
    }

    #[test]
    fn target_claim_fencing() {
        let mut w = crate::workload::Workload {
            workload_id: crate::id::Id::random(),
            workload_generation: 0,
            owner: "t".into(),
            class: "c".into(),
            created_ms: 0,
            execution_epoch: 0,
            active_node: Some("n1".into()),
            backend_class: "cpu".into(),
            checkpoint_generation: 0,
            parent_workload: None,
            fork_generation: 0,
            policy_version: 1,
            metadata: serde_json::json!({}),
            state_schema_version: 1,
            runtime: crate::compatibility::RuntimeCompatibilityDescriptor::local_default(),
            resumability_class: ResumabilityClass::Equivalent,
            protection: crate::workload::ProtectionSpec::default(),
            single_active: true,
            fence_token: Some("t".into()),
            fence_epoch: 0,
        };
        assert!(validate_target_claim(&w, "n2", false).is_err());
        assert!(validate_target_claim(&w, "n2", true).is_ok());
        w.single_active = false;
        assert!(validate_target_claim(&w, "n2", false).is_ok());
    }

    #[test]
    fn archive_restore_policy() {
        let mut c = ckpt();
        c.lifecycle = LifecycleState::Retired;
        let mut p = PolicySet::default();
        assert!(restore_eligible(&c, &p).is_err());
        p.archive_restore_permitted = true;
        assert!(restore_eligible(&c, &p).is_ok());
    }

    #[test]
    fn protection_roundtrip() {
        let j = serde_json::to_string(&ProtectionState::Pinned).unwrap();
        let back: ProtectionState = serde_json::from_str(&j).unwrap();
        assert_eq!(back, ProtectionState::Pinned);
        let j2 = serde_json::to_string(&RetirementEligibility::Protected).unwrap();
        let _: RetirementEligibility = serde_json::from_str(&j2).unwrap();
    }
}
