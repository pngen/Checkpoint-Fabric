//! Example 8: checkpoint-based migration.
//!
//! Migrates a single-active workload from one node to a second node: the
//! checkpoint is streamed to the target, the source is fenced, and authority
//! transfers to the target.

mod common;

use std::sync::Arc;

use checkpoint_fabric::checkpoint::ComponentType;
use checkpoint_fabric::node::{NodeConfig, NodeRuntime};
use checkpoint_fabric::providers::{ApplicationStateProvider, ProviderSpec};
use checkpoint_fabric::restore::RestoreOptions;

fn main() {
    common::run("08-migration", || {
        let h = common::harness("08", common::HarnessOptions::default());
        let out = h
            .coord
            .request_capture(&h.workload.workload_id, &common::capture_options(), &h.ctx)
            .unwrap();
        let ckpt_id = out.checkpoint_id.unwrap();
        let source = h.node.node_id.clone();

        // Start a second node to act as the migration target.
        let target_dir = common::temp_dir("08-target");
        let target_cfg = NodeConfig::default_in(
            "target",
            h.coord.listen_addr().unwrap(),
            target_dir.path().join("node"),
        );
        let target = NodeRuntime::start(target_cfg).unwrap();

        // Host the application provider on the target so the migration's
        // compatibility check sees the required handler, then wait for the
        // coordinator to learn about it through the next heartbeat.
        let target_cell = Arc::new(std::sync::Mutex::new(Vec::new()));
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
                let target_cell = target_cell.clone();
                move || Ok(target_cell.lock().unwrap().clone())
            },
            {
                let target_cell = target_cell.clone();
                move |bytes| {
                    *target_cell.lock().unwrap() = bytes.to_vec();
                    Ok(())
                }
            },
        );
        target
            .providers
            .register(h.workload.workload_id, Arc::new(provider));
        wait_for_provider(&h, &target, "example/app");

        common::banner("migrate the workload to the target node");
        let migrated = h
            .coord
            .request_restore(
                &ckpt_id,
                &target.node_id,
                &RestoreOptions {
                    migration: true,
                    resume: true,
                    ..RestoreOptions::default()
                },
                &h.ctx,
            )
            .unwrap();
        println!("migration restore state={}", migrated.state);

        // Authority moved: the workload's active node is the target, the source
        // is fenced (its old fence token is revoked).
        let w = h
            .coord
            .inspect_workload(&h.workload.workload_id)
            .unwrap()
            .unwrap();
        assert_eq!(w.active_node.as_deref(), Some(target.node_id.as_str()));
        assert_eq!(w.execution_epoch, 3);
        println!(
            "workload now active on {} (epoch {})",
            target.node_id, w.execution_epoch
        );
        assert!(w.validate_fence(&source, "stale-token").is_err());

        // The checkpoint now has a replica on the target.
        let ckpt = h.coord.checkpoint_get(&ckpt_id).unwrap().unwrap();
        assert!(ckpt
            .durable_locations
            .iter()
            .any(|l| l.node == target.node_id));
        println!(
            "replicas: {:?}",
            ckpt.durable_locations
                .iter()
                .map(|l| l.node.clone())
                .collect::<Vec<_>>()
        );

        h.coord.shutdown();
        h.node.shutdown();
        target.shutdown();
    });
}

/// Wait (up to a few seconds) for the coordinator to see the target node's
/// advertised restore handler through its heartbeat.
fn wait_for_provider(h: &common::Harness, target: &NodeRuntime, handler: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let nodes = h.coord.list_nodes().unwrap();
        if nodes
            .iter()
            .any(|n| n.id == target.node_id && n.resources.contains(handler))
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "target provider for {handler} never advertised"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}
