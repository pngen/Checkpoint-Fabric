//! Checkpoint object model: types, consistency classes, resumability classes,
//! integrity and protection states.

use serde::{Deserialize, Serialize};

use crate::compatibility::RuntimeCompatibilityDescriptor;
use crate::frontier::ExecutionFrontier;
use crate::id::Id;

/// Checkpoint types supported by the fabric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CheckpointType {
    Full,
    Incremental,
    Delta,
    Application,
    Process,
    ExecutionFrontier,
    Portable,
    LocalOnly,
}

impl CheckpointType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Full => "FULL",
            Self::Incremental => "INCREMENTAL",
            Self::Delta => "DELTA",
            Self::Application => "APPLICATION",
            Self::Process => "PROCESS",
            Self::ExecutionFrontier => "EXECUTION_FRONTIER",
            Self::Portable => "PORTABLE",
            Self::LocalOnly => "LOCAL_ONLY",
        }
    }
}

/// Consistency classes: how coherent the captured state is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConsistencyClass {
    /// Durable enough to recover similarly to an abrupt process interruption.
    CrashConsistent,
    /// Application-defined quiescence or hooks established valid internal state.
    ApplicationConsistent,
    /// All mandatory execution components agree on one explicit execution frontier
    /// suitable for deterministic resume.
    ExecutionConsistent,
}

impl ConsistencyClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CrashConsistent => "CRASH_CONSISTENT",
            Self::ApplicationConsistent => "APPLICATION_CONSISTENT",
            Self::ExecutionConsistent => "EXECUTION_CONSISTENT",
        }
    }
}

/// Resumability classes: what restore may achieve.
///
/// The derived ordering is `NonResumable < RestartFromCheckpoint < Degraded <
/// Equivalent < Exact`, which the runtime uses to never overstate a class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResumabilityClass {
    /// Retained for audit/archive only.
    NonResumable,
    /// Application can restart from captured durable state, but not continue
    /// exact in-flight execution.
    RestartFromCheckpoint,
    /// Resume with explicitly reduced capability or a changed backend.
    Degraded,
    /// Resume semantically from the same frontier; physical mappings may differ.
    Equivalent,
    /// Resume at the same logical frontier with equivalent state and required resources.
    Exact,
}

impl ResumabilityClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Exact => "EXACT",
            Self::Equivalent => "EQUIVALENT",
            Self::Degraded => "DEGRADED",
            Self::RestartFromCheckpoint => "RESTART_FROM_CHECKPOINT",
            Self::NonResumable => "NON_RESUMABLE",
        }
    }
}

/// Integrity state of a durable checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrityState {
    Pending,
    Valid,
    Corrupt,
    Unverifiable,
}

impl IntegrityState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Valid => "valid",
            Self::Corrupt => "corrupt",
            Self::Unverifiable => "unverifiable",
        }
    }
}

/// Protection state of a checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtectionState {
    None,
    Protected,
    Pinned,
}

/// Retirement eligibility classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetirementEligibility {
    Eligible,
    Protected,
    Pinned,
    MinimumGenerations,
    ActiveRestore,
    NotEligible,
}

/// A durable location of a checkpoint replica.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableLocation {
    pub node: String,
    pub path: String,
    pub verified: bool,
}

/// A state component manifest entry recorded in a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComponentEntry {
    pub component_id: String,
    pub component_type: ComponentType,
    pub generation: u64,
    pub required: bool,
    pub logical_size: u64,
    pub storage_representation: StorageRepresentation,
    pub content_hash: String,
    pub schema_version: u32,
    pub restore_handler: String,
    pub compatibility: serde_json::Value,
    pub dependencies: Vec<String>,
    pub capture_status: String,
    pub restore_status: String,
}

/// Typed component classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ComponentType {
    ProcessMetadata,
    ApplicationState,
    MemoryRegion,
    AcceleratorState,
    RuntimeState,
    QueueState,
    SchedulerState,
    RngState,
    FilesystemState,
    OpenResourceDescriptor,
    ToolState,
    ModelState,
    KvState,
    CustomState,
}

impl ComponentType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ProcessMetadata => "PROCESS_METADATA",
            Self::ApplicationState => "APPLICATION_STATE",
            Self::MemoryRegion => "MEMORY_REGION",
            Self::AcceleratorState => "ACCELERATOR_STATE",
            Self::RuntimeState => "RUNTIME_STATE",
            Self::QueueState => "QUEUE_STATE",
            Self::SchedulerState => "SCHEDULER_STATE",
            Self::RngState => "RNG_STATE",
            Self::FilesystemState => "FILESYSTEM_STATE",
            Self::OpenResourceDescriptor => "OPEN_RESOURCE_DESCRIPTOR",
            Self::ToolState => "TOOL_STATE",
            Self::ModelState => "MODEL_STATE",
            Self::KvState => "KV_STATE",
            Self::CustomState => "CUSTOM_STATE",
        }
    }
}

/// How a component's bytes are stored on disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageRepresentation {
    pub codec: String,
    pub original_size: u64,
    pub stored_size: u64,
    pub stored_hash: String,
    pub relative_path: String,
}

/// External dependency tracked by a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalDependency {
    pub kind: String,
    pub identity: String,
    pub expected_hash: Option<String>,
    pub required: bool,
}

/// The authoritative checkpoint object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckpointObject {
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
    pub lineage_children: Vec<Id>,
    pub supersedes: Option<Id>,
    pub superseded_by: Option<Id>,
    pub policy_version: u32,
    pub runtime_descriptor: RuntimeCompatibilityDescriptor,
    pub hardware_descriptor: HardwareCompatibilityDescriptor,
    pub dependencies: Vec<ExternalDependency>,
    pub integrity_state: IntegrityState,
    pub lifecycle: crate::lifecycle::LifecycleState,
    pub restore_count: u64,
    pub last_restore_result: Option<String>,
    pub protection: ProtectionState,
    pub retirement_eligibility: RetirementEligibility,
    pub metadata: serde_json::Value,
    /// Digest of the canonical manifest (set at seal time).
    pub manifest_digest: Option<String>,
    /// Canonical manifest JSON (set at seal time).
    pub manifest_json: Option<String>,
}

impl CheckpointObject {
    pub fn is_restorable(&self) -> bool {
        self.lifecycle.is_restorable() && self.integrity_state == IntegrityState::Valid
    }
}

/// Hardware compatibility requirements of a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct HardwareCompatibilityDescriptor {
    pub os: String,
    pub arch: String,
    pub accelerator: Option<AcceleratorRequirement>,
    pub word_size_bits: u32,
    pub endianness: String,
}

/// Accelerator requirements.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceleratorRequirement {
    pub vendor: String,
    pub model: Option<String>,
    pub capability: String,
    pub memory_bytes: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_roundtrip_types() {
        for t in [
            CheckpointType::Full,
            CheckpointType::Incremental,
            CheckpointType::ExecutionFrontier,
        ] {
            let json = serde_json::to_string(&t).unwrap();
            let back: CheckpointType = serde_json::from_str(&json).unwrap();
            assert_eq!(t, back);
        }
        assert_eq!(
            serde_json::to_string(&ConsistencyClass::ExecutionConsistent).unwrap(),
            "\"EXECUTION_CONSISTENT\""
        );
    }

    #[test]
    fn resumability_order() {
        assert!(ResumabilityClass::Exact > ResumabilityClass::NonResumable);
    }
}
