//! Example 2: application-consistent checkpoint.
//!
//! Uses a cooperative quiescence hook so the checkpoint claims
//! APPLICATION_CONSISTENT, and an explicit execution frontier.

mod common;

use checkpoint_fabric::capture::{CaptureComponentRequest, CaptureOptions, QuiescenceMode};
use checkpoint_fabric::checkpoint::{ComponentType, ConsistencyClass};
use checkpoint_fabric::frontier::{ExecutionFrontier, ResumabilityFlags};
use checkpoint_fabric::providers::{ApplicationStateProvider, ProviderSpec};
use std::sync::Arc;

fn main() {
    common::run("02-application-consistent", || {
        let h = common::harness("02", common::HarnessOptions::default());

        // Attach a provider with a cooperative quiesce + resume hook.
        let quiesced = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let provider = ApplicationStateProvider::new(
            ProviderSpec {
                component_id: "app".into(),
                component_type: ComponentType::ApplicationState,
                required: true,
                schema_version: 1,
                restore_handler: "example/app".into(),
                compatibility: serde_json::json!({}),
                dependencies: Vec::new(),
            },
            {
                let cell = h.cell.clone();
                move || Ok(cell.lock().unwrap().clone())
            },
            {
                let cell = h.cell.clone();
                move |bytes| {
                    *cell.lock().unwrap() = bytes.to_vec();
                    Ok(())
                }
            },
        )
        .with_quiesce({
            let quiesced = quiesced.clone();
            move || {
                quiesced.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        })
        .with_resume({
            let quiesced = quiesced.clone();
            move || {
                quiesced.store(false, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        });

        h.coord
            .fence_workload(&h.workload.workload_id, &h.ctx)
            .unwrap();
        let w = h
            .coord
            .inspect_workload(&h.workload.workload_id)
            .unwrap()
            .unwrap();
        h.node
            .attach_workload(&w.workload_id, w.execution_epoch, vec![Arc::new(provider)])
            .unwrap();

        let frontier = ExecutionFrontier {
            workload_generation: w.workload_generation,
            execution_epoch: w.execution_epoch,
            logical_step: 42,
            flags: ResumabilityFlags {
                deterministic_replay_verified: false,
                exact: false,
                ..ResumabilityFlags::default()
            },
            ..ExecutionFrontier::default()
        };

        common::banner("application-consistent capture with cooperative quiescence");
        let opts = CaptureOptions {
            consistency: Some(ConsistencyClass::ApplicationConsistent),
            quiescence: QuiescenceMode::Cooperative,
            frontier: Some(frontier),
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
            .request_capture(&w.workload_id, &opts, &h.ctx)
            .unwrap();
        let ckpt = h
            .coord
            .checkpoint_get(&out.checkpoint_id.unwrap())
            .unwrap()
            .unwrap();
        println!(
            "achieved consistency {} resumability {} (quiesce hook fired during capture: {})",
            ckpt.consistency.as_str(),
            ckpt.resumability.as_str(),
            quiesced.load(std::sync::atomic::Ordering::SeqCst)
        );
        assert_eq!(ckpt.consistency, ConsistencyClass::ApplicationConsistent);
        assert_eq!(ckpt.frontier.logical_step, 42);
        assert!(
            !quiesced.load(std::sync::atomic::Ordering::SeqCst),
            "resume hook must have run"
        );
        h.coord.shutdown();
        h.node.shutdown();
    });
}
