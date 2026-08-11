//! Canonical checkpoint manifest.
//!
//! Every sealed checkpoint has a canonical manifest serialized with `serde_json`
//! in fixed field order. The manifest digest (SHA-256 of the canonical bytes) is
//! the checkpoint's integrity anchor; the integrity root derives from component
//! content hashes and the canonical metadata.

use serde::{Deserialize, Serialize};

use crate::checkpoint::{
    CheckpointType, ComponentEntry, ConsistencyClass, DurableLocation, ExternalDependency,
    HardwareCompatibilityDescriptor, ResumabilityClass,
};
use crate::compatibility::RuntimeCompatibilityDescriptor;
use crate::frontier::ExecutionFrontier;
use crate::id::Id;
use crate::integrity::{compute_integrity_root, sha256_hex};

use crate::policy::PolicySet;

/// Canonical serialization format version.
pub const MANIFEST_FORMAT_VERSION: u32 = 1;

/// The canonical manifest of a sealed checkpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub checkpoint_id: Id,
    pub workload_id: Id,
    pub workload_generation: u64,
    pub checkpoint_generation: u64,
    pub parent_checkpoint: Option<Id>,
    pub capture_attempt: String,
    pub coordinator_epoch: u64,
    pub created_ms: u64,
    pub seal_ms: u64,
    pub source_node: String,
    pub source_backend: String,
    pub frontier: ExecutionFrontier,
    pub checkpoint_type: CheckpointType,
    pub consistency: ConsistencyClass,
    pub resumability: ResumabilityClass,
    pub components: Vec<ComponentEntry>,
    pub total_logical_bytes: u64,
    pub total_physical_bytes: u64,
    pub compressed_bytes: u64,
    pub durable_locations: Vec<DurableLocation>,
    pub replica_count: u32,
    pub lineage_parents: Vec<Id>,
    pub policy_version: u32,
    pub runtime_descriptor: RuntimeCompatibilityDescriptor,
    pub hardware_descriptor: HardwareCompatibilityDescriptor,
    pub dependencies: Vec<ExternalDependency>,
    pub integrity: IntegrityInfo,
    pub metadata: serde_json::Value,
}

/// Integrity anchor of the manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntegrityInfo {
    pub content_hash_algorithm: String,
    /// The integrity root, computed over component content hashes and canonical
    /// metadata. Empty during computation; filled at seal time.
    pub root: String,
}

impl Manifest {
    /// Canonical serialization: fixed field order, no whitespace.
    pub fn to_canonical_bytes(&self) -> Vec<u8> {
        // serde_json serializes struct fields in declaration order, so the output
        // is deterministic for identical inputs.
        serde_json::to_vec(self).expect("manifest is always serializable")
    }

    /// Compute the integrity root for this manifest (root field ignored).
    pub fn compute_integrity_root(&self) -> String {
        let mut with_empty_root = self.clone();
        with_empty_root.integrity.root.clear();
        let bytes = with_empty_root.to_canonical_bytes();
        let hashes: Vec<String> = self
            .components
            .iter()
            .map(|c| c.content_hash.clone())
            .collect();
        compute_integrity_root(&hashes, &bytes)
    }

    /// Verify that the embedded integrity root matches the canonical derivation.
    pub fn verify_integrity_root(&self) -> bool {
        self.compute_integrity_root() == self.integrity.root
    }

    /// Digest over the canonical bytes including the integrity root.
    pub fn digest(&self) -> String {
        sha256_hex(&self.to_canonical_bytes())
    }

    /// Canonical bytes with the integrity root emptied (the bytes whose hash the
    /// root is derived from; used by verification).
    pub fn canonical_without_root(&self) -> Vec<u8> {
        let mut m = self.clone();
        m.integrity.root.clear();
        m.to_canonical_bytes()
    }
}

/// Build a manifest scaffold from a checkpoint object; used before seal.
pub fn scaffold(checkpoint: &crate::checkpoint::CheckpointObject) -> Manifest {
    Manifest {
        format_version: MANIFEST_FORMAT_VERSION,
        checkpoint_id: checkpoint.checkpoint_id,
        workload_id: checkpoint.workload_id,
        workload_generation: checkpoint.workload_generation,
        checkpoint_generation: checkpoint.checkpoint_generation,
        parent_checkpoint: checkpoint.parent_checkpoint,
        capture_attempt: checkpoint.capture_attempt.clone(),
        coordinator_epoch: checkpoint.coordinator_epoch,
        created_ms: checkpoint.created_ms,
        seal_ms: checkpoint.seal_ms,
        source_node: checkpoint.source_node.clone(),
        source_backend: checkpoint.source_backend.clone(),
        frontier: checkpoint.frontier.clone(),
        checkpoint_type: checkpoint.checkpoint_type,
        consistency: checkpoint.consistency,
        resumability: checkpoint.resumability,
        components: checkpoint.components.clone(),
        total_logical_bytes: checkpoint.total_logical_bytes,
        total_physical_bytes: checkpoint.total_physical_bytes,
        compressed_bytes: checkpoint.compressed_bytes,
        durable_locations: checkpoint.durable_locations.clone(),
        replica_count: checkpoint.replica_count,
        lineage_parents: checkpoint.lineage_parents.clone(),
        policy_version: checkpoint.policy_version,
        runtime_descriptor: checkpoint.runtime_descriptor.clone(),
        hardware_descriptor: checkpoint.hardware_descriptor.clone(),
        dependencies: checkpoint.dependencies.clone(),
        integrity: IntegrityInfo {
            content_hash_algorithm: "sha256".into(),
            root: String::new(),
        },
        metadata: checkpoint.metadata.clone(),
    }
}

/// Sealed manifest record: canonical bytes plus digest and root.
#[derive(Debug, Clone)]
pub struct SealedManifest {
    pub manifest: Manifest,
    pub canonical_bytes: Vec<u8>,
    pub digest: String,
    pub integrity_root: String,
}

/// Seal a scaffold: compute the integrity root and digest.
pub fn seal(mut manifest: Manifest) -> SealedManifest {
    let root = manifest.compute_integrity_root();
    manifest.integrity.root = root.clone();
    let canonical_bytes = manifest.to_canonical_bytes();
    let digest = sha256_hex(&canonical_bytes);
    SealedManifest {
        manifest,
        canonical_bytes,
        digest,
        integrity_root: root,
    }
}

/// Parse and validate a manifest from canonical bytes.
pub fn parse(canonical: &[u8]) -> Result<Manifest, crate::errors::FabricError> {
    let m: Manifest = serde_json::from_slice(canonical).map_err(|e| {
        crate::errors::FabricError::CorruptedCheckpoint(format!("bad manifest: {e}"))
    })?;
    if m.format_version != MANIFEST_FORMAT_VERSION {
        return Err(crate::errors::FabricError::CorruptedCheckpoint(format!(
            "unsupported manifest format version {}",
            m.format_version
        )));
    }
    if !m.verify_integrity_root() {
        return Err(crate::errors::FabricError::IntegrityFailure(
            "manifest integrity root mismatch".into(),
        ));
    }
    Ok(m)
}

/// Whether a manifest claims EXACT resumability and would need full verification.
pub fn claims_exact(m: &Manifest) -> bool {
    m.resumability == ResumabilityClass::Exact
}

/// Policies that influence manifest contents.
pub fn component_count(m: &Manifest) -> usize {
    m.components.len()
}

/// Helper: validate that the manifest matches the policy version and format.
pub fn validate_policy(m: &Manifest, policy: &PolicySet) -> Result<(), crate::errors::FabricError> {
    if m.policy_version != policy.version {
        return Err(crate::errors::FabricError::PolicyViolation(format!(
            "manifest policy version {} does not match active policy {}",
            m.policy_version, policy.version
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{CheckpointObject, IntegrityState};
    use crate::lifecycle::LifecycleState;

    fn sample() -> CheckpointObject {
        crate::compatibility::sample_checkpoint()
    }

    #[test]
    fn canonical_bytes_are_stable() {
        let ck = sample();
        let m = scaffold(&ck);
        let b1 = m.to_canonical_bytes();
        let m2: Manifest = serde_json::from_slice(&b1).unwrap();
        let b2 = m2.to_canonical_bytes();
        assert_eq!(b1, b2);
    }

    #[test]
    fn seal_and_verify() {
        let ck = sample();
        let mut m = scaffold(&ck);
        m.components.push(ComponentEntry {
            component_id: "app".into(),
            component_type: crate::checkpoint::ComponentType::ApplicationState,
            generation: 0,
            required: true,
            logical_size: 4,
            storage_representation: crate::checkpoint::StorageRepresentation {
                codec: "none".into(),
                original_size: 4,
                stored_size: 4,
                stored_hash: sha256_hex(b"data"),
                relative_path: "components/app".into(),
            },
            content_hash: sha256_hex(b"data"),
            schema_version: 1,
            restore_handler: "application".into(),
            compatibility: serde_json::json!({}),
            dependencies: Vec::new(),
            capture_status: "captured".into(),
            restore_status: "pending".into(),
        });
        let sealed = seal(m);
        assert!(sealed.manifest.verify_integrity_root());
        let parsed = parse(&sealed.canonical_bytes).unwrap();
        assert_eq!(parsed, sealed.manifest);
        assert_eq!(parsed.digest(), sealed.digest);
    }

    #[test]
    fn tampered_manifest_rejected() {
        let ck = sample();
        let sealed = seal(scaffold(&ck));
        let mut tampered = sealed.canonical_bytes.clone();
        let len = tampered.len();
        tampered[len / 2] ^= 0xff;
        assert!(parse(&tampered).is_err());
    }

    #[test]
    fn root_changes_when_component_changes() {
        let ck = sample();
        let sealed1 = seal(scaffold(&ck));
        let mut ck2 = ck.clone();
        ck2.metadata = serde_json::json!({"x": 1});
        let sealed2 = seal(scaffold(&ck2));
        assert_ne!(sealed1.integrity_root, sealed2.integrity_root);
    }

    #[test]
    fn integrity_state_matches_lifecycle() {
        let ck = sample();
        assert_eq!(ck.integrity_state, IntegrityState::Valid);
        assert_eq!(ck.lifecycle, LifecycleState::Available);
    }
}
