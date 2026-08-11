//! Checkpoint lineage: parents, supersession, forks, rollbacks, delta bases,
//! and migrations. Lineage history is immutable and durable.

use serde::{Deserialize, Serialize};

use crate::errors::{FabricError, FabricResult};
use crate::id::Id;

/// Relationships tracked in lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LineageRelation {
    Parent,
    Supersedes,
    ForkedFrom,
    RollbackOf,
    DeltaBase,
    MigratedFrom,
}

impl LineageRelation {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Parent => "PARENT",
            Self::Supersedes => "SUPERSEDES",
            Self::ForkedFrom => "FORKED_FROM",
            Self::RollbackOf => "ROLLBACK_OF",
            Self::DeltaBase => "DELTA_BASE",
            Self::MigratedFrom => "MIGRATED_FROM",
        }
    }

    pub fn parse_str(s: &str) -> Self {
        match s {
            "PARENT" => Self::Parent,
            "SUPERSEDES" => Self::Supersedes,
            "FORKED_FROM" => Self::ForkedFrom,
            "ROLLBACK_OF" => Self::RollbackOf,
            "DELTA_BASE" => Self::DeltaBase,
            _ => Self::MigratedFrom,
        }
    }
}

/// One durable lineage record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageRecord {
    pub seq: i64,
    pub relation: LineageRelation,
    pub workload_id: Option<Id>,
    pub checkpoint_id: Option<Id>,
    pub other_workload: Option<Id>,
    pub other_checkpoint: Option<Id>,
    pub ts_ms: u64,
    pub detail: String,
}

/// Validate an incremental chain: walk from `leaf` through `DeltaBase`/`Parent`
/// links and ensure no cycles, missing links, or broken chains.
pub fn validate_incremental_chain(
    checkpoint: &crate::checkpoint::CheckpointObject,
    get_parent: &dyn Fn(&Id) -> FabricResult<Option<crate::checkpoint::CheckpointObject>>,
    max_depth: u32,
) -> FabricResult<()> {
    let mut depth = 0u32;
    let mut seen = std::collections::HashSet::new();
    let mut cur = Some(checkpoint.clone());
    while let Some(c) = cur {
        if !seen.insert(c.checkpoint_id) {
            return Err(FabricError::LineageViolation(format!(
                "incremental chain contains a cycle at {}",
                c.checkpoint_id
            )));
        }
        depth += 1;
        if depth > max_depth {
            return Err(FabricError::LineageViolation(format!(
                "incremental chain exceeds policy depth limit of {max_depth}"
            )));
        }
        let base = match c.checkpoint_type {
            crate::checkpoint::CheckpointType::Incremental
            | crate::checkpoint::CheckpointType::Delta => c.parent_checkpoint,
            _ => None,
        };
        cur = match base {
            None => None,
            Some(parent_id) => {
                let parent = get_parent(&parent_id)?.ok_or_else(|| {
                    FabricError::MissingDependency(format!(
                        "incremental chain requires parent checkpoint {parent_id} which is missing"
                    ))
                })?;
                if parent.lifecycle.is_terminal()
                    || parent.integrity_state == crate::checkpoint::IntegrityState::Corrupt
                {
                    return Err(FabricError::MissingDependency(format!(
                        "parent checkpoint {parent_id} is not restorable ({:?}, {:?})",
                        parent.lifecycle, parent.integrity_state
                    )));
                }
                Some(parent)
            }
        };
    }
    Ok(())
}

/// Build the ancestor path of a checkpoint through parent links.
pub fn ancestor_path(
    checkpoint: &crate::checkpoint::CheckpointObject,
    get: &dyn Fn(&Id) -> FabricResult<Option<crate::checkpoint::CheckpointObject>>,
) -> FabricResult<Vec<Id>> {
    let mut path = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut cur = checkpoint.parent_checkpoint;
    while let Some(id) = cur {
        if !seen.insert(id) {
            return Err(FabricError::LineageViolation(
                "lineage cycle detected".into(),
            ));
        }
        path.push(id);
        let c = get(&id)?.ok_or_else(|| {
            FabricError::MissingDependency(format!("lineage ancestor {id} missing"))
        })?;
        cur = match c.checkpoint_type {
            crate::checkpoint::CheckpointType::Incremental
            | crate::checkpoint::CheckpointType::Delta => c.parent_checkpoint,
            _ => None,
        };
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{CheckpointObject, CheckpointType};
    use crate::compatibility::sample_checkpoint;

    fn set_type(
        mut c: CheckpointObject,
        t: CheckpointType,
        parent: Option<Id>,
    ) -> CheckpointObject {
        c.checkpoint_type = t;
        c.parent_checkpoint = parent;
        c
    }

    #[test]
    fn full_chain_is_valid() {
        let ck = sample_checkpoint();
        assert!(validate_incremental_chain(&ck, &|_| Ok(None), 8).is_ok());
    }

    #[test]
    fn broken_chain_detected() {
        let leaf = set_type(
            sample_checkpoint(),
            CheckpointType::Incremental,
            Some(Id::random()),
        );
        let err = validate_incremental_chain(&leaf, &|_| Ok(None), 8).unwrap_err();
        assert!(matches!(err, FabricError::MissingDependency(_)));
    }

    #[test]
    fn cycle_detected() {
        let a = Id::random();
        let mut leaf = set_type(sample_checkpoint(), CheckpointType::Incremental, Some(a));
        leaf.checkpoint_id = Id::random();
        let leaf_id = leaf.checkpoint_id;
        let mut a_obj = set_type(
            sample_checkpoint(),
            CheckpointType::Incremental,
            Some(leaf_id),
        );
        a_obj.checkpoint_id = a;
        let err = validate_incremental_chain(
            &leaf,
            &|id| {
                if *id == a {
                    Ok(Some(a_obj.clone()))
                } else if *id == leaf_id {
                    Ok(Some(leaf.clone()))
                } else {
                    Ok(None)
                }
            },
            8,
        )
        .unwrap_err();
        assert!(matches!(err, FabricError::LineageViolation(_)));
    }

    #[test]
    fn depth_limit_enforced() {
        let chain = std::cell::RefCell::new(Vec::new());
        let leaf = set_type(
            sample_checkpoint(),
            CheckpointType::Incremental,
            Some(Id::random()),
        );
        for _ in 0..10 {
            let mut c = set_type(
                sample_checkpoint(),
                CheckpointType::Incremental,
                Some(Id::random()),
            );
            c.checkpoint_id = Id::random();
            chain.borrow_mut().push(c);
        }
        let err = validate_incremental_chain(
            &leaf,
            &|_| {
                let mut chain = chain.borrow_mut();
                if chain.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(chain.pop().unwrap()))
                }
            },
            3,
        )
        .unwrap_err();
        assert!(matches!(err, FabricError::LineageViolation(_)));
    }
}
