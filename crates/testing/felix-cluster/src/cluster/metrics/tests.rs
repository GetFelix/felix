use std::collections::{BTreeMap, BTreeSet};

use felix_controlplane_service::model::{ReplicaReport, ShardKey, ShardKind};

use super::*;

fn report(leader_offset: u64, caught_up: &[&str], offsets: &[(&str, u64)]) -> ReplicaReport {
    ReplicaReport {
        given_up: false,
        inherited_storage: Default::default(),
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
        leader: None,
        node_incarnations: Default::default(),
        follower_storage: Default::default(),
        joint_start_acked: None,
        refused: None,
        holds_start: Default::default(),
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

fn member(
    id: &str,
    role: &str,
    term: u64,
    leader: Option<&str>,
    last: Option<u64>,
) -> (String, felix_wire::InspectedRaft) {
    (
        id.to_string(),
        felix_wire::InspectedRaft {
            role: role.to_string(),
            term,
            leader: leader.map(str::to_string),
            last_index: last,
            committed: last,
            voters: Vec::new(),
        },
    )
}

#[test]
fn a_raft_wait_names_the_leader_once_a_majority_holds_its_tail() {
    let members = [
        member("a", "leader", 3, Some("a"), Some(10)),
        member("b", "follower", 3, Some("a"), Some(10)),
        member("c", "follower", 3, Some("a"), Some(7)),
    ];
    assert_eq!(
        raft_leader_caught_up(&members, &members),
        Ok("a".to_string())
    );
}

#[test]
fn a_raft_wait_holds_while_only_the_leader_has_the_tail() {
    let members = [
        member("a", "leader", 3, Some("a"), Some(10)),
        member("b", "follower", 3, Some("a"), Some(7)),
        member("c", "follower", 3, Some("a"), Some(7)),
    ];
    assert!(raft_leader_caught_up(&members, &members).is_err());
}

#[test]
fn a_raft_wait_ignores_a_deposed_leader_that_hears_no_one() {
    let members = [
        member("a", "leader", 2, None, Some(10)),
        member("b", "leader", 3, Some("b"), Some(12)),
        member("c", "follower", 3, Some("b"), Some(12)),
    ];
    assert_eq!(
        raft_leader_caught_up(&members, &members),
        Ok("b".to_string())
    );
}

#[test]
fn a_raft_wait_needs_a_leader_and_does_not_count_learners() {
    assert!(raft_leader_caught_up(&[member("a", "follower", 1, None, Some(1))], &[]).is_err());
    let members = [
        member("a", "leader", 3, Some("a"), Some(10)),
        member("b", "follower", 3, Some("a"), Some(5)),
        member("c", "learner", 3, Some("a"), Some(10)),
    ];
    assert!(raft_leader_caught_up(&members, &members).is_err());
}

#[test]
fn a_raft_liveness_check_measures_the_commit_index_not_the_tail() {
    let mut members = vec![
        member("a", "leader", 4, Some("a"), Some(12)),
        member("b", "follower", 4, Some("a"), Some(10)),
        member("c", "follower", 4, Some("a"), Some(8)),
    ];
    members[0].1.committed = Some(10);
    assert!(raft_leader_caught_up(&members, &members).is_err());
    assert_eq!(
        raft_leader_committed(&members, &members),
        Ok(("a".to_string(), 4))
    );
    members[1].1.last_index = Some(9);
    assert!(raft_leader_committed(&members, &members).is_err());
}

/// `member`, as a leader that names its group's voters.
fn leading(id: &str, term: u64, last: u64, voters: &[&str]) -> (String, felix_wire::InspectedRaft) {
    let mut led = member(id, "leader", term, Some(id), Some(last));
    led.1.voters = voters.iter().map(|voter| voter.to_string()).collect();
    led
}

/// A drained broker's member is cut out of the group without hearing of it,
/// so it still calls itself a follower. The leader's voters leave it out.
#[test]
fn a_raft_liveness_check_does_not_count_a_removed_member() {
    let members = [
        member("broker-0", "follower", 4, Some("broker-2"), Some(1446)),
        member("broker-1", "follower", 3, None, Some(282)),
        leading("broker-2", 4, 1448, &["broker-0", "broker-2", "broker-3"]),
        member("broker-3", "follower", 4, Some("broker-2"), Some(1448)),
    ];
    assert_eq!(
        raft_leader_committed(&members, &members),
        Ok(("broker-2".to_string(), 4))
    );
}

/// A voter that could not be asked is still one of the voters.
#[test]
fn a_raft_liveness_check_counts_a_voter_it_could_not_ask() {
    let first = [
        member("a", "follower", 3, Some("b"), Some(9)),
        leading("b", 3, 10, &["a", "b", "c"]),
    ];
    assert!(raft_leader_committed(&first, &first).is_err());
    let then = [
        member("a", "follower", 3, Some("b"), Some(11)),
        leading("b", 3, 11, &["a", "b", "c"]),
    ];
    assert_eq!(
        raft_leader_committed(&first, &then),
        Ok(("b".to_string(), 3))
    );
}

/// Brokers are asked in turn, so under a workload a follower asked before
/// the leader trails what the leader then reports. The followers are read
/// again after the leader, and only that later look is measured.
#[test]
fn a_raft_liveness_check_reads_the_followers_after_the_leader() {
    let first = [
        member("a", "follower", 5, Some("c"), Some(241)),
        member("b", "follower", 5, Some("c"), Some(245)),
        leading("c", 5, 251, &["a", "b", "c"]),
    ];
    let then = [
        member("a", "follower", 5, Some("c"), Some(253)),
        member("b", "follower", 5, Some("c"), Some(250)),
        leading("c", 5, 258, &["a", "b", "c"]),
    ];
    assert_eq!(
        raft_leader_committed(&first, &then),
        Ok(("c".to_string(), 5))
    );
    assert_eq!(raft_leader_caught_up(&first, &then), Ok("c".to_string()));
}
