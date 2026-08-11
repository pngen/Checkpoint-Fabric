//! Example 9: compatibility rejection.
//!
//! Evaluates a checkpoint against an incompatible target (different OS by
//! default policy) and shows the structured reasons; a compatible target
//! restores normally.

mod common;

use checkpoint_fabric::compatibility::RuntimeCompatibilityDescriptor;

fn main() {
    common::run("09-compatibility-rejection", || {
        let h = common::harness("09", common::HarnessOptions::default());
        let out = h
            .coord
            .request_capture(&h.workload.workload_id, &common::capture_options(), &h.ctx)
            .unwrap();
        let ckpt_id = out.checkpoint_id.unwrap();

        common::banner("evaluate against an incompatible target (different OS)");
        let mut foreign = RuntimeCompatibilityDescriptor::local_default();
        foreign.os = if std::env::consts::OS == "windows" {
            "linux".into()
        } else {
            "windows".into()
        };
        let verdict = h.coord.compatibility(&ckpt_id, &foreign, &h.ctx).unwrap();
        println!("verdict: {}", verdict.verdict.as_str());
        for reason in &verdict.reasons {
            println!("  - {reason}");
        }
        assert_eq!(
            verdict.verdict,
            checkpoint_fabric::compatibility::CompatVerdict::Incompatible
        );

        common::banner("restore to the compatible local node still works");
        let restored = h
            .coord
            .request_restore(
                &ckpt_id,
                &h.node.node_id,
                &checkpoint_fabric::restore::RestoreOptions::default(),
                &h.ctx,
            )
            .unwrap();
        println!("compatible restore state={}", restored.state);
        h.coord.shutdown();
        h.node.shutdown();
    });
}
