//! Deterministic, serializable, versioned policy engine.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::checkpoint::ConsistencyClass;
use crate::compression::Codec;
use crate::errors::{FabricError, FabricResult};

/// Authority policy: which actor roles are permitted for each operation kind.
/// An empty list means any registered actor may perform the operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityPolicy {
    pub capture: Vec<String>,
    pub restore: Vec<String>,
    pub seal: Vec<String>,
    pub retire: Vec<String>,
    pub migrate: Vec<String>,
    pub fence: Vec<String>,
    pub fork: Vec<String>,
    pub rollback: Vec<String>,
    pub verify: Vec<String>,
}

impl Default for AuthorityPolicy {
    fn default() -> Self {
        Self {
            capture: vec!["owner".into()],
            restore: vec!["owner".into(), "operator".into()],
            seal: vec!["owner".into()],
            retire: vec!["operator".into()],
            migrate: vec!["operator".into()],
            fence: vec!["operator".into()],
            fork: vec!["owner".into()],
            rollback: vec!["operator".into()],
            verify: vec!["operator".into(), "auditor".into()],
        }
    }
}

/// Compatibility policy knobs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatibilityPolicy {
    pub allow_cross_os: bool,
    pub allow_cross_arch: bool,
    pub schema_strict: bool,
}

impl Default for CompatibilityPolicy {
    fn default() -> Self {
        Self {
            allow_cross_os: false,
            allow_cross_arch: false,
            schema_strict: true,
        }
    }
}

/// The complete policy set. Versioned; every operation records the policy version used.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicySet {
    pub version: u32,
    /// Minimum interval between checkpoints of the same workload, in milliseconds.
    pub min_capture_interval_ms: u64,
    /// Maximum age of a checkpoint before it is eligible for retirement.
    pub max_checkpoint_age_ms: Option<u64>,
    /// Minimum valid replica count required for a checkpoint to be restorable.
    pub min_valid_replicas: u32,
    /// If set, capture must achieve at least this consistency class.
    pub required_consistency: Option<ConsistencyClass>,
    /// Require the portable checkpoint format.
    pub require_portable: bool,
    /// Forbid degraded restore.
    pub forbid_degraded_restore: bool,
    /// Number of most recent generations protected from automatic retirement.
    pub protected_generations: u32,
    /// Minimum number of generations that must be retained.
    pub min_generations_retained: u32,
    /// Maximum incremental chain depth before compaction is required.
    pub max_incremental_chain_depth: u32,
    /// Default single-active execution policy for new workloads.
    pub single_active_default: bool,
    /// Whether archived (RETIRED) checkpoints may be restored.
    pub archive_restore_permitted: bool,
    /// Maximum bytes for a single component payload at restore.
    pub max_component_bytes: Option<u64>,
    /// Compression for component payloads.
    pub compression: Codec,
    pub compression_level: i32,
    pub authority: AuthorityPolicy,
    pub compatibility: CompatibilityPolicy,
}

impl Default for PolicySet {
    fn default() -> Self {
        Self {
            version: 1,
            min_capture_interval_ms: 0,
            max_checkpoint_age_ms: None,
            min_valid_replicas: 1,
            required_consistency: None,
            require_portable: false,
            forbid_degraded_restore: false,
            protected_generations: 1,
            min_generations_retained: 1,
            max_incremental_chain_depth: 8,
            single_active_default: true,
            archive_restore_permitted: false,
            max_component_bytes: None,
            compression: Codec::Zstd,
            compression_level: 3,
            authority: AuthorityPolicy::default(),
            compatibility: CompatibilityPolicy::default(),
        }
    }
}

impl PolicySet {
    /// Serialize deterministically (no map ordering issues: only Vec fields).
    pub fn to_canonical_json(&self) -> FabricResult<String> {
        serde_json::to_string(self).map_err(|e| FabricError::Json(e.to_string()))
    }

    /// Parse from canonical JSON.
    pub fn from_json(s: &str) -> FabricResult<Self> {
        let p: PolicySet = serde_json::from_str(s)
            .map_err(|e| FabricError::InvalidArgument(format!("bad policy json: {e}")))?;
        if p.version == 0 {
            return Err(FabricError::InvalidArgument(
                "policy version must be >= 1".into(),
            ));
        }
        Ok(p)
    }

    /// Check that an actor with the given roles may perform an operation kind.
    pub fn authorize(&self, roles: &[String], required_roles: &[String]) -> FabricResult<()> {
        if required_roles.is_empty() {
            return Ok(());
        }
        if roles.iter().any(|r| r == "operator") || roles.iter().any(|r| r == "root") {
            return Ok(());
        }
        for need in required_roles {
            if roles.iter().any(|r| r == need) {
                return Ok(());
            }
        }
        Err(FabricError::PolicyViolation(format!(
            "actor roles {:?} lack required roles {:?}",
            roles, required_roles
        )))
    }

    pub fn compression_spec(&self) -> crate::compression::CompressionSpec {
        match self.compression {
            Codec::None => crate::compression::CompressionSpec::none(),
            Codec::Zstd => crate::compression::CompressionSpec::zstd(self.compression_level),
        }
    }
}

/// Actor identity used for authority checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Authority {
    pub actor: String,
    pub roles: Vec<String>,
}

impl Authority {
    pub fn operator(actor: &str) -> Self {
        Self {
            actor: actor.to_string(),
            roles: vec!["operator".into()],
        }
    }

    pub fn owner(actor: &str) -> Self {
        Self {
            actor: actor.to_string(),
            roles: vec!["owner".into()],
        }
    }

    pub fn named(actor: &str, roles: &[&str]) -> Self {
        Self {
            actor: actor.to_string(),
            roles: roles.iter().map(|s| s.to_string()).collect(),
        }
    }
}

/// Deterministic policy selection: the highest version, or an exact version.
pub fn select(policies: &[PolicySet], version: Option<u32>) -> FabricResult<&PolicySet> {
    match version {
        None => policies
            .iter()
            .max_by_key(|p| p.version)
            .ok_or_else(|| FabricError::PolicyViolation("no policies loaded".into())),
        Some(v) => policies
            .iter()
            .find(|p| p.version == v)
            .ok_or_else(|| FabricError::PolicyViolation(format!("policy version {v} not found"))),
    }
}

/// Enforce that a captured consistency class meets the policy requirement.
pub fn enforce_consistency(policy: &PolicySet, achieved: ConsistencyClass) -> FabricResult<()> {
    if let Some(required) = policy.required_consistency {
        if achieved < required {
            return Err(FabricError::PolicyViolation(format!(
                "capture achieved {} but policy requires {}",
                achieved.as_str(),
                required.as_str()
            )));
        }
    }
    Ok(())
}

/// Enforce degraded-restore prohibition.
pub fn enforce_degraded(
    policy: &PolicySet,
    verdict: crate::compatibility::CompatVerdict,
) -> FabricResult<()> {
    if policy.forbid_degraded_restore
        && verdict == crate::compatibility::CompatVerdict::CompatibleDegraded
    {
        return Err(FabricError::PolicyViolation(
            "degraded restore is forbidden by policy".into(),
        ));
    }
    Ok(())
}

/// Deterministic key for stable policy comparison (invariant #18 relies on this).
pub fn policy_key(p: &PolicySet) -> FabricResult<String> {
    let mut v = p.to_canonical_json()?;
    // Sort any lists that are semantically unordered: none exist by construction,
    // but keep the function for stable comparison in tests.
    let _ = &mut v;
    Ok(v)
}

/// A compact view used by Reclaim Fabric integration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetirementPolicyView {
    pub min_generations_retained: u32,
    pub protected_generations: u32,
    pub max_checkpoint_age_ms: Option<u64>,
}

impl From<&PolicySet> for RetirementPolicyView {
    fn from(p: &PolicySet) -> Self {
        Self {
            min_generations_retained: p.min_generations_retained,
            protected_generations: p.protected_generations,
            max_checkpoint_age_ms: p.max_checkpoint_age_ms,
        }
    }
}

/// BTreeMap alias used by compatibility descriptors.
pub type ProviderVersions = BTreeMap<String, String>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_is_valid_and_deterministic() {
        let p = PolicySet::default();
        let j1 = p.to_canonical_json().unwrap();
        let j2 = PolicySet::from_json(&j1)
            .unwrap()
            .to_canonical_json()
            .unwrap();
        assert_eq!(j1, j2);
        assert_eq!(p.version, 1);
    }

    #[test]
    fn authority_checks() {
        let p = PolicySet::default();
        assert!(p.authorize(&["owner".into()], &p.authority.capture).is_ok());
        assert!(p.authorize(&["owner".into()], &p.authority.retire).is_err());
        assert!(p
            .authorize(&["operator".into()], &p.authority.retire)
            .is_ok());
        assert!(p
            .authorize(&["auditor".into()], &p.authority.verify)
            .is_ok());
        assert!(p.authorize(&[], &p.authority.verify).is_err());
    }

    #[test]
    fn consistency_policy_enforced() {
        let p = PolicySet {
            required_consistency: Some(ConsistencyClass::ApplicationConsistent),
            ..PolicySet::default()
        };
        assert!(enforce_consistency(&p, ConsistencyClass::CrashConsistent).is_err());
        assert!(enforce_consistency(&p, ConsistencyClass::ApplicationConsistent).is_ok());
        assert!(enforce_consistency(&p, ConsistencyClass::ExecutionConsistent).is_ok());
    }

    #[test]
    fn version_selection() {
        let v1 = PolicySet::default();
        let mut v2 = v1.clone();
        v2.version = 2;
        let ps = vec![v1, v2];
        assert_eq!(select(&ps, None).unwrap().version, 2);
        assert_eq!(select(&ps, Some(1)).unwrap().version, 1);
        assert!(select(&ps, Some(3)).is_err());
    }
}
