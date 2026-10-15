use std::collections::{BTreeMap, BTreeSet};

use felix_controlplane_service::model::{ReplicaReport, ShardKey, ShardKind};

use super::*;

fn report(leader_offset: u64, caught_up: &[&str], offsets: &[(&str, u64)]) -> ReplicaReport {
    ReplicaReport {
        key: ShardKey {
            tenant_id: "t".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shard: 0,
            kind: ShardKind::Stream,
        },
        stream_incarnation: None,
        by_round: false,
        leader_incarnation: None,
        generation: 2,
        caught_up: caught_up
            .iter()
            .map(|node| node.to_string())
            .collect::<BTreeSet<_>>(),
        offsets: offsets
            .iter()
            .map(|(node, at)| (node.to_string(), *at))
            .collect::<BTreeMap<_, _>>(),
        reported_at_millis: 0,
        drained: false,
        leader_offset: Some(leader_offset),
        halted: BTreeMap::new(),
        counter_offsets: BTreeMap::new(),
    }
}

#[test]
fn a_caught_up_follower_at_the_tail_holds_it() {
    let report = report(5, &["broker-b"], &[("broker-b", 5), ("broker-c", 3)]);
    assert_eq!(
        holds_tail(Some(&report), Some(2), 5),
        Ok("broker-b".to_string())
    );
}

#[test]
fn a_report_from_before_the_last_publish_does_not() {
    // What the start-up probe leaves behind: a level follower, one record ago.
    let report = report(4, &["broker-b"], &[("broker-b", 4)]);
    let short = holds_tail(Some(&report), Some(2), 5).unwrap_err();
    assert!(short.contains("predates the tail"), "{short}");
}

#[test]
fn caught_up_short_of_the_tail_does_not() {
    // `caught_up` is measured against the acknowledged offset, not the tail.
    let report = report(5, &["broker-b"], &[("broker-b", 4)]);
    let short = holds_tail(Some(&report), Some(2), 5).unwrap_err();
    assert!(
        short.contains("broker-b"),
        "the message names the replica: {short}"
    );
}

#[test]
fn a_follower_at_the_tail_but_not_caught_up_does_not() {
    let report = report(5, &[], &[("broker-b", 5)]);
    assert!(holds_tail(Some(&report), Some(2), 5).is_err());
}

#[test]
fn a_report_from_another_generation_does_not() {
    let report = report(5, &["broker-b"], &[("broker-b", 5)]);
    assert!(holds_tail(Some(&report), Some(3), 5).is_err());
}

#[test]
fn no_report_does_not() {
    assert!(holds_tail(None, Some(2), 0).is_err());
}
