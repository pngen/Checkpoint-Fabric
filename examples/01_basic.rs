//! Example 1: basic workload checkpoint.
//!
//! Registers a workload, captures a checkpoint, and inspects the sealed result.

mod common;

use checkpoint_fabric::capture::{CaptureComponentRequest, CaptureOptions};
use checkpoint_fabric::checkpoint::ComponentType;

fn main() {
    common::run("01-basic-workload-checkpoint", || {
        let h = common::harness("01", common::HarnessOptions::default());
        common::banner("capture");
        let opts = CaptureOptions {
            components: vec![CaptureComponentRequest {
                component_id: "app".into(),
                component_type: ComponentType::ApplicationState,
                required: true,
                schema_version: 1,
                restore_handler: "example/app".into(),
            }],
            ..CaptureOptions::default()
        };
        let out = h
            .coord
            .request_capture(&h.workload.workload_id, &opts, &h.ctx)
            .unwrap();
        println!(
            "checkpoint {} generation {} state {}",
            out.checkpoint_id.unwrap(),
            out.workload_generation.unwrap_or_default(),
            out.state
        );

        let ckpt = h
            .coord
            .checkpoint_get(&out.checkpoint_id.unwrap())
            .unwrap()
            .unwrap();
        println!(
            "lifecycle={} consistency={} resumability={} components={} logical_bytes={}",
            ckpt.lifecycle.as_str(),
            ckpt.consistency.as_str(),
            ckpt.resumability.as_str(),
            ckpt.components.len(),
            ckpt.total_logical_bytes
        );
        assert_eq!(ckpt.components.len(), 1);
        assert_eq!(ckpt.total_logical_bytes, 23);
        h.coord.shutdown();
        h.node.shutdown();
    });
}
