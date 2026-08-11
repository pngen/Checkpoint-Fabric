//! Example 6: fork from a checkpoint.
//!
//! A checkpoint becomes the origin of an independent descendant workload with
//! its own lineage; the parent checkpoint is never mutated.

mod common;

use checkpoint_fabric::workload::{ProtectionSpec, WorkloadSpec};

fn main() {
    common::run("06-fork-from-checkpoint", || {
        let h = common::harness("06", common::HarnessOptions::default());
        let out = h
            .coord
            .request_capture(&h.workload.workload_id, &common::capture_options(), &h.ctx)
            .unwrap();
        let ckpt_id = out.checkpoint_id.unwrap();

        common::banner("fork a child workload from the checkpoint");
        let child_spec = WorkloadSpec {
            workload_id: None,
            owner: "example".into(),
            class: "forked".into(),
            backend_class: "cpu".into(),
            state_schema_version: 1,
            runtime:
                checkpoint_fabric::compatibility::RuntimeCompatibilityDescriptor::local_default(),
            metadata: serde_json::json!({ "origin": "fork" }),
            protection: ProtectionSpec::default(),
            single_active: true,
        };
        let child = h.coord.fork(&ckpt_id, &child_spec, &h.ctx).unwrap();
        println!(
            "child workload {} forked from parent {} (fork generation {})",
            child.workload_id,
            child.parent_workload.unwrap(),
            child.fork_generation
        );
        assert_eq!(child.fork_generation, 1);

        // The parent checkpoint is untouched: still AVAILABLE with no children
        // recorded against its own object.
        let parent_ckpt = h.coord.checkpoint_get(&ckpt_id).unwrap().unwrap();
        assert_eq!(parent_ckpt.lifecycle.as_str(), "AVAILABLE");

        // The child's lineage records the fork.
        let lineage = h.coord.workload_lineage(&child.workload_id).unwrap();
        assert!(lineage
            .iter()
            .any(|l| l.relation == checkpoint_fabric::lineage::LineageRelation::ForkedFrom));
        println!("child lineage records FORKED_FROM checkpoint {ckpt_id}");
        h.coord.shutdown();
        h.node.shutdown();
    });
}
