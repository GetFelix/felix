//! What the listing promises an operator.
use super::*;

fn replica(node: &str, reason: &'static str) -> HaltedReplica {
    HaltedReplica {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 3,
        kind: "stream",
        node_id: node.to_string(),
        generation: 7,
        next_offset: 120,
        reason,
        remedy: "",
    }
}

#[test]
fn a_healthy_broker_lists_nothing() {
    assert!(HaltedReplicas::new().snapshot().is_empty());
}

#[test]
fn a_pass_replaces_the_listing_rather_than_adding_to_it() {
    // A halt that has been resolved has to stop being reported, or the listing
    // sends an operator after a replica that is already shipping again.
    let halted = HaltedReplicas::new();
    halted.publish(vec![
        replica("broker-b", "diverged"),
        replica("broker-c", "fenced"),
    ]);
    assert_eq!(halted.snapshot().len(), 2);

    halted.publish(vec![replica("broker-c", "fenced")]);
    let now = halted.snapshot();
    assert_eq!(now.len(), 1);
    assert_eq!(now[0].node_id, "broker-c");

    halted.publish(Vec::new());
    assert!(halted.snapshot().is_empty());
}

#[test]
fn every_halt_names_a_remedy_and_a_stable_reason() {
    // The reason is what a runbook keys off, so it is asserted literally; the
    // enum's Display is prose and may be reworded.
    for (halt, expected) in [
        (Halt::Diverged, "diverged"),
        (Halt::Fenced, "fenced"),
        (Halt::NeedsBootstrap, "needs_bootstrap"),
    ] {
        let (reason, remedy) = describe(halt);
        assert_eq!(reason, expected);
        assert!(
            remedy.len() > 40,
            "{reason} has no remedy worth reading, which leaves the operator \
             exactly where the bare count did",
        );
    }
}
