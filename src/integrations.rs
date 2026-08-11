//! Optional integration interfaces for the other fabrics in the sequence:
//! FlashTier, Context Fabric, Compute Fabric, and Reclaim Fabric.
//!
//! Checkpoint Fabric must not hard-link any of these projects. These adapters
//! are pure descriptors and pure functions; no other fabric is required to run
//! core Checkpoint Fabric.

use serde::{Deserialize, Serialize};

use crate::checkpoint::{CheckpointObject, ProtectionState, RetirementEligibility};
use crate::id::Id;

// ---------------- Reclaim Fabric interface ----------------

/// View of a checkpoint exposed for external reclamation decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReclaimView {
    pub checkpoint_id: Id,
    pub workload_id: Id,
    pub checkpoint_generation: u64,
    pub lifecycle: String,
    pub value: u64,
    pub protection: ProtectionState,
    pub reconstructibility: String,
    pub lineage_parents: Vec<Id>,
    pub superseded_by: Option<Id>,
    pub min_generations_retained: u32,
    pub retirement_eligibility: RetirementEligibility,
    pub total_physical_bytes: u64,
    pub replicas: u32,
}

/// Build the Reclaim Fabric view for a checkpoint.
pub fn reclaim_view(checkpoint: &CheckpointObject, min_generations_retained: u32) -> ReclaimView {
    ReclaimView {
        checkpoint_id: checkpoint.checkpoint_id,
        workload_id: checkpoint.workload_id,
        checkpoint_generation: checkpoint.checkpoint_generation,
        lifecycle: checkpoint.lifecycle.as_str().to_string(),
        value: checkpoint
            .restore_count
            .saturating_add(checkpoint.replica_count as u64),
        protection: checkpoint.protection,
        reconstructibility: if checkpoint.is_restorable() {
            "restorable".into()
        } else {
            "not_restorable".into()
        },
        lineage_parents: checkpoint.lineage_parents.clone(),
        superseded_by: checkpoint.superseded_by,
        min_generations_retained,
        retirement_eligibility: checkpoint.retirement_eligibility,
        total_physical_bytes: checkpoint.total_physical_bytes,
        replicas: checkpoint.replica_count,
    }
}

// ---------------- Context Fabric interface ----------------

/// How a checkpoint references a Context Fabric object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ContextRefSemantics {
    /// Embed the state directly in the checkpoint.
    Embed,
    /// Reference the object by stable identity.
    Reference,
    /// Restore requires a replica of the object.
    RequireReplica,
    /// Materialize the object into the checkpoint at capture time.
    MaterializeOnCheckpoint,
}

/// A resolved Context Fabric object reference at restore time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextRefResolution {
    pub object_id: Id,
    pub required: bool,
    pub semantics: ContextRefSemantics,
}

// ---------------- Compute Fabric interface ----------------

/// Target requirements exposed for Compute Fabric placement decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputePlacement {
    pub checkpoint_id: Id,
    pub cpu_arch: String,
    pub accelerator: Option<String>,
    pub memory_bytes: u64,
    pub runtime_requirement: String,
    pub state_locality: Vec<String>,
    pub estimated_restore_bytes: u64,
    pub network_cost_estimate: u64,
}

/// Compute Fabric placement requirements for a checkpoint.
pub fn compute_placement(checkpoint: &CheckpointObject) -> ComputePlacement {
    ComputePlacement {
        checkpoint_id: checkpoint.checkpoint_id,
        cpu_arch: checkpoint.hardware_descriptor.arch.clone(),
        accelerator: checkpoint
            .hardware_descriptor
            .accelerator
            .as_ref()
            .map(|a| format!("{}:{}", a.vendor, a.capability)),
        memory_bytes: checkpoint.total_logical_bytes,
        runtime_requirement: checkpoint.runtime_descriptor.runtime_version.clone(),
        state_locality: checkpoint
            .durable_locations
            .iter()
            .map(|l| l.node.clone())
            .collect(),
        estimated_restore_bytes: checkpoint.total_physical_bytes,
        network_cost_estimate: checkpoint.total_physical_bytes.saturating_mul(2),
    }
}

// ---------------- FlashTier interface ----------------

/// Logical residency descriptor for a state component. Checkpoint correctness
/// never depends on preserving exact source-tier placement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlashTierResidency {
    pub logical_id: String,
    pub tier: String,
    pub hot: bool,
}

/// Whether a residency descriptor is admissible for portable checkpoints:
/// only logical, never physical, identities are recorded. Physical path
/// separators (`/` and `\`) are rejected; logical namespaces may use `:`.
pub fn is_portable_residency(r: &FlashTierResidency) -> bool {
    !r.logical_id.contains('/')
        && !r.logical_id.contains('\\')
        && !r.logical_id.is_empty()
        && !r.tier.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compatibility::sample_checkpoint;

    #[test]
    fn reclaim_view_shapes() {
        let ck = sample_checkpoint();
        let view = reclaim_view(&ck, 2);
        assert_eq!(view.checkpoint_id, ck.checkpoint_id);
        assert_eq!(view.min_generations_retained, 2);
        assert_eq!(view.protection, ProtectionState::None);
    }

    #[test]
    fn compute_placement_shapes() {
        let ck = sample_checkpoint();
        let p = compute_placement(&ck);
        assert_eq!(p.cpu_arch, ck.hardware_descriptor.arch);
        assert!(p.state_locality.is_empty());
    }

    #[test]
    fn portability_of_residency() {
        assert!(is_portable_residency(&FlashTierResidency {
            logical_id: "kv:block:42".into(),
            tier: "hot".into(),
            hot: true,
        }));
        assert!(!is_portable_residency(&FlashTierResidency {
            logical_id: "/dev/nvme0/pool/42".into(),
            tier: "hot".into(),
            hot: true,
        }));
        assert!(!is_portable_residency(&FlashTierResidency {
            logical_id: "x".into(),
            tier: "".into(),
            hot: false,
        }));
    }

    #[test]
    fn context_ref_serde() {
        let r = ContextRefResolution {
            object_id: Id::random(),
            required: true,
            semantics: ContextRefSemantics::RequireReplica,
        };
        let j = serde_json::to_string(&r).unwrap();
        let back: ContextRefResolution = serde_json::from_str(&j).unwrap();
        assert_eq!(r, back);
    }
}
