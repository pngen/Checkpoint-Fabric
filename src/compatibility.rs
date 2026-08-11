//! Restore compatibility model.
//!
//! Compatibility evaluation is a pure, deterministic function of the checkpoint
//! requirements and the target environment, so identical inputs always produce
//! identical decisions (invariant #18).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::checkpoint::{CheckpointObject, HardwareCompatibilityDescriptor};
use crate::errors::{FabricError, FabricResult};
use crate::policy::PolicySet;

/// Runtime compatibility descriptor carried by workloads and checkpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCompatibilityDescriptor {
    pub os: String,
    pub arch: String,
    pub runtime_version: String,
    pub state_schema_version: u32,
    pub workload_class: String,
    pub backend_class: String,
    pub accelerator_capabilities: Vec<String>,
    pub provider_versions: BTreeMap<String, String>,
    pub application: Option<String>,
}

impl RuntimeCompatibilityDescriptor {
    /// A conservative descriptor for the current process.
    pub fn local_default() -> Self {
        Self {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            runtime_version: format!("checkpoint-fabric/{}", crate::VERSION),
            state_schema_version: 1,
            workload_class: "generic".into(),
            backend_class: "cpu".into(),
            accelerator_capabilities: Vec::new(),
            provider_versions: BTreeMap::new(),
            application: None,
        }
    }
}

/// The verdict of a compatibility evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompatVerdict {
    Compatible,
    CompatibleWithTranslation,
    CompatibleDegraded,
    Incompatible,
}

impl CompatVerdict {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Compatible => "COMPATIBLE",
            Self::CompatibleWithTranslation => "COMPATIBLE_WITH_TRANSLATION",
            Self::CompatibleDegraded => "COMPATIBLE_DEGRADED",
            Self::Incompatible => "INCOMPATIBLE",
        }
    }
}

/// Structured compatibility result with explicit reasons.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompatibilityResult {
    pub verdict: CompatVerdict,
    pub reasons: Vec<String>,
    pub checkpoint_id: Option<crate::id::Id>,
    pub source: RuntimeCompatibilityDescriptor,
    pub target: RuntimeCompatibilityDescriptor,
    pub policy_version: u32,
}

impl CompatibilityResult {
    /// The resumability class achievable given this compatibility result and the
    /// checkpoint's captured class. Never overstates the achieved class.
    pub fn resumability_for(
        &self,
        captured: crate::checkpoint::ResumabilityClass,
    ) -> crate::checkpoint::ResumabilityClass {
        use crate::checkpoint::ResumabilityClass as R;
        match self.verdict {
            CompatVerdict::Compatible => captured,
            CompatVerdict::CompatibleWithTranslation => match captured {
                R::Exact => R::Equivalent,
                R::Equivalent => R::Equivalent,
                R::Degraded => R::Degraded,
                R::RestartFromCheckpoint => R::RestartFromCheckpoint,
                R::NonResumable => R::NonResumable,
            },
            CompatVerdict::CompatibleDegraded => match captured {
                R::Exact | R::Equivalent | R::Degraded => R::Degraded,
                R::RestartFromCheckpoint => R::RestartFromCheckpoint,
                R::NonResumable => R::NonResumable,
            },
            CompatVerdict::Incompatible => R::NonResumable,
        }
    }
}

fn worse(a: CompatVerdict, b: CompatVerdict) -> CompatVerdict {
    a.max(b)
}

/// Evaluate whether `checkpoint` can be restored into an environment described by
/// `target`. Pure and deterministic.
pub fn evaluate(
    checkpoint: &CheckpointObject,
    target: &RuntimeCompatibilityDescriptor,
    policy: &PolicySet,
) -> FabricResult<CompatibilityResult> {
    let src = &checkpoint.runtime_descriptor;
    let mut verdict = CompatVerdict::Compatible;
    let mut reasons: Vec<String> = Vec::new();

    let reject = |v: CompatVerdict, reason: &str, reasons: &mut Vec<String>| {
        reasons.push(reason.to_string());
        v
    };

    if src.os == target.os {
        reasons.push(format!("os matches: {}", src.os));
    } else {
        verdict = worse(
            verdict,
            if policy.compatibility.allow_cross_os {
                reject(
                    CompatVerdict::CompatibleWithTranslation,
                    &format!(
                        "os differs ({} -> {}), translation required",
                        src.os, target.os
                    ),
                    &mut reasons,
                )
            } else {
                reject(
                    CompatVerdict::Incompatible,
                    &format!(
                        "os differs ({} -> {}) and policy forbids translation",
                        src.os, target.os
                    ),
                    &mut reasons,
                )
            },
        );
    }

    if src.arch == target.arch {
        reasons.push(format!("arch matches: {}", src.arch));
    } else {
        verdict = worse(
            verdict,
            if policy.compatibility.allow_cross_arch {
                reject(
                    CompatVerdict::CompatibleWithTranslation,
                    &format!(
                        "arch differs ({} -> {}), translation required",
                        src.arch, target.arch
                    ),
                    &mut reasons,
                )
            } else {
                reject(
                    CompatVerdict::Incompatible,
                    &format!(
                        "arch differs ({} -> {}) and policy forbids translation",
                        src.arch, target.arch
                    ),
                    &mut reasons,
                )
            },
        );
    }

    if src.backend_class == target.backend_class {
        reasons.push(format!("backend matches: {}", src.backend_class));
    } else {
        verdict = worse(
            verdict,
            reject(
                CompatVerdict::CompatibleDegraded,
                &format!(
                    "backend differs ({} -> {}): degraded resume only",
                    src.backend_class, target.backend_class
                ),
                &mut reasons,
            ),
        );
    }

    if src.state_schema_version == target.state_schema_version {
        reasons.push(format!(
            "state schema matches: v{}",
            src.state_schema_version
        ));
    } else if policy.compatibility.schema_strict {
        verdict = worse(
            verdict,
            reject(
                CompatVerdict::Incompatible,
                &format!(
                    "state schema mismatch (v{} -> v{}) and policy is strict",
                    src.state_schema_version, target.state_schema_version
                ),
                &mut reasons,
            ),
        );
    } else {
        verdict = worse(
            verdict,
            reject(
                CompatVerdict::CompatibleDegraded,
                &format!(
                    "state schema differs (v{} -> v{}): degraded resume only",
                    src.state_schema_version, target.state_schema_version
                ),
                &mut reasons,
            ),
        );
    }

    if src.workload_class == target.workload_class {
        reasons.push(format!("workload class matches: {}", src.workload_class));
    } else {
        verdict = worse(
            verdict,
            reject(
                CompatVerdict::CompatibleDegraded,
                &format!(
                    "workload class differs ({} -> {})",
                    src.workload_class, target.workload_class
                ),
                &mut reasons,
            ),
        );
    }

    let required_accel: Vec<&String> = src
        .accelerator_capabilities
        .iter()
        .filter(|c| c.starts_with("REQUIRED:"))
        .collect();
    if !required_accel.is_empty()
        && !required_accel
            .iter()
            .all(|c| target.accelerator_capabilities.contains(*c))
    {
        verdict = worse(
            verdict,
            reject(
                CompatVerdict::Incompatible,
                "target lacks required accelerator capabilities",
                &mut reasons,
            ),
        );
    } else {
        reasons.push("accelerator capabilities satisfied".to_string());
    }

    let checkpoint_format = crate::FORMAT_VERSION;
    let target_format = src.format_version();
    if checkpoint_format == target_format {
        reasons.push(format!("checkpoint format matches: v{checkpoint_format}"));
    } else if target_format > checkpoint_format {
        verdict = worse(
            verdict,
            reject(
                CompatVerdict::CompatibleWithTranslation,
                &format!(
                    "checkpoint format v{checkpoint_format} restored by newer runtime v{target_format}"
                ),
                &mut reasons,
            ),
        );
    } else {
        verdict = worse(
            verdict,
            reject(
                CompatVerdict::Incompatible,
                &format!(
                    "checkpoint format v{checkpoint_format} newer than target v{target_format}"
                ),
                &mut reasons,
            ),
        );
    }

    for c in &checkpoint.components {
        if c.restore_handler.is_empty() {
            continue;
        }
        match target.provider_versions.get(&c.restore_handler) {
            None => {
                let v = if c.required {
                    CompatVerdict::Incompatible
                } else {
                    CompatVerdict::CompatibleDegraded
                };
                verdict = worse(
                    verdict,
                    reject(
                        v,
                        &format!(
                            "target has no provider for required handler '{}'",
                            c.restore_handler
                        ),
                        &mut reasons,
                    ),
                );
            }
            Some(v) => reasons.push(format!(
                "handler '{}' available at version {v}",
                c.restore_handler
            )),
        }
    }

    if verdict == CompatVerdict::Compatible {
        reasons.push("all compatibility checks passed".to_string());
    }

    Ok(CompatibilityResult {
        verdict,
        reasons,
        checkpoint_id: Some(checkpoint.checkpoint_id),
        source: src.clone(),
        target: target.clone(),
        policy_version: policy.version,
    })
}

/// Compatibility of a hardware descriptor against a target environment.
pub fn evaluate_hardware(
    source: &HardwareCompatibilityDescriptor,
    os: &str,
    arch: &str,
    accel_caps: &[String],
) -> FabricResult<CompatVerdict> {
    let mut verdict = CompatVerdict::Compatible;
    if source.os != os {
        verdict = CompatVerdict::CompatibleWithTranslation;
    }
    if source.arch != arch {
        verdict = worse(verdict, CompatVerdict::CompatibleWithTranslation);
    }
    if let Some(req) = &source.accelerator {
        if !accel_caps.iter().any(|c| c == &req.capability) {
            verdict = CompatVerdict::Incompatible;
        }
    }
    if verdict == CompatVerdict::Incompatible {
        return Err(FabricError::CompatibilityFailure(
            "hardware requirements cannot be satisfied by the target".into(),
        ));
    }
    Ok(verdict)
}

impl RuntimeCompatibilityDescriptor {
    fn format_version(&self) -> u32 {
        crate::FORMAT_VERSION
    }
}

/// A fully populated sample checkpoint used by tests across the crate.
pub fn sample_checkpoint() -> crate::checkpoint::CheckpointObject {
    use crate::checkpoint::{
        CheckpointObject, CheckpointType, ConsistencyClass, IntegrityState, ProtectionState,
        ResumabilityClass, RetirementEligibility,
    };
    use crate::frontier::ExecutionFrontier;
    use crate::lifecycle::LifecycleState;
    CheckpointObject {
        checkpoint_id: crate::id::Id::random(),
        workload_id: crate::id::Id::random(),
        workload_generation: 0,
        checkpoint_generation: 1,
        parent_checkpoint: None,
        capture_attempt: "a".into(),
        coordinator_epoch: 1,
        created_ms: 0,
        seal_ms: 0,
        source_node: "n".into(),
        source_backend: "cpu".into(),
        frontier: ExecutionFrontier::default(),
        checkpoint_type: CheckpointType::Full,
        consistency: ConsistencyClass::ApplicationConsistent,
        resumability: ResumabilityClass::Exact,
        components: Vec::new(),
        total_logical_bytes: 0,
        total_physical_bytes: 0,
        compressed_bytes: 0,
        durable_locations: Vec::new(),
        replica_count: 1,
        lineage_parents: Vec::new(),
        lineage_children: Vec::new(),
        supersedes: None,
        superseded_by: None,
        policy_version: 1,
        runtime_descriptor: RuntimeCompatibilityDescriptor::local_default(),
        hardware_descriptor: HardwareCompatibilityDescriptor::default(),
        dependencies: Vec::new(),
        integrity_state: IntegrityState::Valid,
        lifecycle: LifecycleState::Available,
        restore_count: 0,
        last_restore_result: None,
        protection: ProtectionState::None,
        retirement_eligibility: RetirementEligibility::Eligible,
        metadata: serde_json::json!({}),
        manifest_digest: None,
        manifest_json: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{
        ComponentEntry, ComponentType, ResumabilityClass, StorageRepresentation,
    };
    use crate::policy::PolicySet;

    #[test]
    fn identical_environments_are_compatible() {
        let ck = sample_checkpoint();
        let target = RuntimeCompatibilityDescriptor::local_default();
        let res = evaluate(&ck, &target, &PolicySet::default()).unwrap();
        assert_eq!(res.verdict, CompatVerdict::Compatible);
    }

    #[test]
    fn incompatible_os_fails_closed_by_default() {
        let mut ck = sample_checkpoint();
        ck.runtime_descriptor.os = "linux".into();
        let target = RuntimeCompatibilityDescriptor::local_default();
        let res = evaluate(&ck, &target, &PolicySet::default()).unwrap();
        assert_eq!(res.verdict, CompatVerdict::Incompatible);
        assert!(res.reasons.iter().any(|r| r.contains("os differs")));
    }

    #[test]
    fn backend_mismatch_is_degraded() {
        let mut ck = sample_checkpoint();
        ck.runtime_descriptor.backend_class = "cuda".into();
        let target = RuntimeCompatibilityDescriptor::local_default();
        let res = evaluate(&ck, &target, &PolicySet::default()).unwrap();
        assert_eq!(res.verdict, CompatVerdict::CompatibleDegraded);
    }

    #[test]
    fn missing_required_handler_is_incompatible() {
        let mut ck = sample_checkpoint();
        ck.components.push(ComponentEntry {
            component_id: "c1".into(),
            component_type: ComponentType::CustomState,
            generation: 0,
            required: true,
            logical_size: 1,
            storage_representation: StorageRepresentation {
                codec: "none".into(),
                original_size: 1,
                stored_size: 1,
                stored_hash: "x".into(),
                relative_path: "components/c1".into(),
            },
            content_hash: "y".into(),
            schema_version: 1,
            restore_handler: "custom/checker".into(),
            compatibility: serde_json::json!({}),
            dependencies: Vec::new(),
            capture_status: "captured".into(),
            restore_status: "pending".into(),
        });
        let target = RuntimeCompatibilityDescriptor::local_default();
        let res = evaluate(&ck, &target, &PolicySet::default()).unwrap();
        assert_eq!(res.verdict, CompatVerdict::Incompatible);
    }

    #[test]
    fn optional_handler_missing_is_degraded() {
        let mut ck = sample_checkpoint();
        ck.components.push(ComponentEntry {
            component_id: "c1".into(),
            component_type: ComponentType::CustomState,
            generation: 0,
            required: false,
            logical_size: 1,
            storage_representation: StorageRepresentation {
                codec: "none".into(),
                original_size: 1,
                stored_size: 1,
                stored_hash: "x".into(),
                relative_path: "components/c1".into(),
            },
            content_hash: "y".into(),
            schema_version: 1,
            restore_handler: "custom/checker".into(),
            compatibility: serde_json::json!({}),
            dependencies: Vec::new(),
            capture_status: "captured".into(),
            restore_status: "pending".into(),
        });
        let target = RuntimeCompatibilityDescriptor::local_default();
        let res = evaluate(&ck, &target, &PolicySet::default()).unwrap();
        assert_eq!(res.verdict, CompatVerdict::CompatibleDegraded);
    }

    #[test]
    fn resumability_never_overstated() {
        let ck = sample_checkpoint();
        let target = RuntimeCompatibilityDescriptor::local_default();
        let ok = evaluate(&ck, &target, &PolicySet::default()).unwrap();
        let mut bad = ck.clone();
        bad.runtime_descriptor.os = "linux".into();
        let incompat = evaluate(&bad, &target, &PolicySet::default()).unwrap();
        let mut degraded_ck = ck.clone();
        degraded_ck.runtime_descriptor.backend_class = "cuda".into();
        let degraded = evaluate(&degraded_ck, &target, &PolicySet::default()).unwrap();

        assert_eq!(
            ok.resumability_for(ResumabilityClass::Exact),
            ResumabilityClass::Exact
        );
        assert_eq!(
            incompat.resumability_for(ResumabilityClass::Exact),
            ResumabilityClass::NonResumable
        );
        assert_eq!(
            degraded.resumability_for(ResumabilityClass::Exact),
            ResumabilityClass::Degraded
        );
        assert_eq!(
            incompat.resumability_for(ResumabilityClass::RestartFromCheckpoint),
            ResumabilityClass::NonResumable
        );
    }

    #[test]
    fn hardware_eval() {
        let hw = HardwareCompatibilityDescriptor {
            os: "linux".into(),
            arch: "x86_64".into(),
            accelerator: None,
            word_size_bits: 64,
            endianness: "little".into(),
        };
        assert!(evaluate_hardware(&hw, "linux", "x86_64", &[]).is_ok());
        assert!(evaluate_hardware(&hw, "windows", "x86_64", &[]).is_ok());
    }
}
