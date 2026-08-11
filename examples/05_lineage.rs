//! Example 5: checkpoint lineage.
//!
//! Creates three consecutive checkpoint generations and inspects the lineage
//! (supersession) links between them.

mod common;

fn main() {
    common::run("05-checkpoint-lineage", || {
        let h = common::harness("05", common::HarnessOptions::default());
        let mut ids = Vec::new();
        for i in 0..3 {
            *h.cell.lock().unwrap() = format!("state-generation-{i}").into_bytes();
            let out = h
                .coord
                .request_capture(&h.workload.workload_id, &common::capture_options(), &h.ctx)
                .unwrap();
            ids.push(out.checkpoint_id.unwrap());
        }

        common::banner("inspect each generation and its supersession links");
        for id in &ids {
            let ckpt = h.coord.checkpoint_get(id).unwrap().unwrap();
            println!(
                "generation {} supersedes {:?} superseded_by {:?}",
                ckpt.checkpoint_generation, ckpt.supersedes, ckpt.superseded_by
            );
        }
        assert_eq!(
            ids[0],
            h.coord
                .checkpoint_get(&ids[1])
                .unwrap()
                .unwrap()
                .supersedes
                .unwrap()
        );
        assert_eq!(
            ids[1],
            h.coord
                .checkpoint_get(&ids[2])
                .unwrap()
                .unwrap()
                .supersedes
                .unwrap()
        );

        common::banner("durable lineage records");
        let lineage = h.coord.checkpoint_lineage(&ids[2]).unwrap();
        for l in &lineage {
            println!("{} {:?}", l.relation.as_str(), l.detail);
        }
        assert!(lineage
            .iter()
            .any(|l| l.relation == checkpoint_fabric::lineage::LineageRelation::Supersedes));
        h.coord.shutdown();
        h.node.shutdown();
    });
}
