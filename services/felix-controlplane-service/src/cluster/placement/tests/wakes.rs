//! Running placement on demand, and timing the moves it writes.
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::super::metrics::{MoveClock, MoveTimes};
use super::*;
use crate::store::ControlPlaneStore;
use crate::test_support::{one_shard_cluster, shard_zero};

/// A drained leader is cut over as soon as a pass is asked for, not at the
/// next tick of an interval that here is an hour away.
#[tokio::test]
async fn a_requested_pass_runs_without_waiting_for_the_interval() {
    let (store, _keys) = one_shard_cluster().await;
    let fenced = store
        .put_shard_assignment(ShardAssignment {
            key: shard_zero(),
            leader: "broker-x".to_string(),
            replicas: vec!["broker-y".to_string()],
            generation: 0,
            state: ShardState::Draining,
            successor: Some("broker-y".to_string()),
            joining: None,
            move_started_at_millis: None,
            move_reason: None,
            incarnation: 0,
            fence_storage_ids: Default::default(),
            member_storage_ids: Default::default(),
            learners: Default::default(),
            pending_set: None,
            previous_generation: None,
            readmitted: Default::default(),
            placed: None,
            switch: None,
            raft_copy_ids: Default::default(),
            raft_target_counter: 0,
            raft_target: None,
            raft_floor: None,
            raft_formed: None,
            raft_voters: None,
            raft_seed: None,
            raft_bases: None,
            raft_leader: None,
            raft_ordered: None,
        })
        .await
        .expect("fenced move");

    let wakes = Arc::new(PlacementWakes::default());
    let written = store.watch_shard_assignment_changes().await;
    let shutdown = tokio_util::sync::CancellationToken::new();
    let task = spawn_reconciler(
        Arc::clone(&store) as _,
        Default::default(),
        MovePolicy::default(),
        Duration::from_secs(3600),
        "test-holder".to_string(),
        crate::raft::LeadershipGate::Always,
        Arc::clone(&wakes),
        shutdown.clone(),
    );
    // The interval's first tick is immediate; let that pass find nothing to do.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        store
            .get_shard_assignment(&shard_zero())
            .await
            .expect("get"),
        fenced,
        "nothing to do until the leader reports drained",
    );

    store
        .record_replica_report(
            crate::model::ReplicaReport {
                inherited_storage: Default::default(),
                given_up: false,
                counter_offsets: Default::default(),
                stream_incarnation: None,
                by_round: false,
                leader_incarnation: None,
                key: shard_zero(),
                generation: fenced.generation,
                caught_up: ["broker-y".to_string()].into_iter().collect(),
                offsets: Default::default(),
                reported_at_millis: store.now_millis().await.expect("clock"),
                drained: true,
                leader_offset: None,
                halted: Default::default(),
                leader: None,
                node_incarnations: Default::default(),
                follower_storage: Default::default(),
                joint_start_acked: None,
                refused: None,
                holds_start: Default::default(),
            },
            &fenced.leader,
        )
        .await
        .expect("report");
    wakes.request_pass();

    let cut_over = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = store
                .get_shard_assignment(&shard_zero())
                .await
                .expect("get");
            if current.leader == "broker-y" {
                return current;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the requested pass cut the shard over");
    assert_eq!(cut_over.state, ShardState::Assigning);
    assert!(
        written.has_changed().expect("sender alive"),
        "the write wakes the long-polls",
    );

    shutdown.cancel();
    task.await.expect("reconciler");
}

fn key() -> ShardKey {
    shard_zero()
}

fn stage() -> MoveStep {
    MoveStep::Stage {
        successor: "broker-y".to_string(),
    }
}

fn cut_over() -> MoveStep {
    MoveStep::CutOver {
        from: "broker-x".to_string(),
        to: "broker-y".to_string(),
    }
}

#[test]
fn a_move_is_timed_from_stage_and_from_fence() {
    let t0 = Instant::now();
    let mut clock = MoveClock::default();
    assert_eq!(clock.written(&key(), &stage(), 1, false, t0), None);
    assert_eq!(
        clock.written(
            &key(),
            &MoveStep::Fence,
            2,
            true,
            t0 + Duration::from_secs(7)
        ),
        None
    );
    assert_eq!(
        clock.written(&key(), &cut_over(), 3, true, t0 + Duration::from_secs(9)),
        Some(MoveTimes {
            total: Some(Duration::from_secs(9)),
            fenced: Duration::from_secs(2),
        })
    );
    // Finished: a later cut-over of the same shard is not this move.
    assert_eq!(clock.written(&key(), &cut_over(), 4, false, t0), None);
}

/// A move that goes straight to the fence starts there.
#[test]
fn a_move_that_starts_at_the_fence_is_timed_from_it() {
    let t0 = Instant::now();
    let mut clock = MoveClock::default();
    clock.written(&key(), &MoveStep::Fence, 1, false, t0);
    let times = clock
        .written(&key(), &cut_over(), 2, true, t0 + Duration::from_secs(1))
        .expect("timed");
    assert_eq!(times.total, Some(Duration::from_secs(1)));
    assert_eq!(times.fenced, Duration::from_secs(1));
}

/// Staged by another instance: when is not known, so only the fence is timed.
#[test]
fn a_fence_after_someone_elses_stage_times_only_the_fence() {
    let t0 = Instant::now();
    let mut clock = MoveClock::default();
    clock.written(&key(), &MoveStep::Fence, 1, true, t0);
    let times = clock
        .written(&key(), &cut_over(), 2, true, t0 + Duration::from_secs(1))
        .expect("timed");
    assert_eq!(times.total, None);
}

/// Another writer touched the shard between this instance's steps; the
/// timing is not this instance's any more.
#[test]
fn a_shard_changed_elsewhere_is_forgotten() {
    let t0 = Instant::now();
    let mut clock = MoveClock::default();
    clock.written(&key(), &stage(), 1, false, t0);
    clock.written(&key(), &MoveStep::Fence, 2, true, t0);

    clock.forget_changed(&[(key(), 2)].into_iter().collect());
    clock.forget_changed(&[(key(), 3)].into_iter().collect());
    assert_eq!(clock.written(&key(), &cut_over(), 4, true, t0), None);
}

#[test]
fn an_abandoned_move_is_forgotten() {
    let t0 = Instant::now();
    let mut clock = MoveClock::default();
    clock.written(&key(), &stage(), 1, false, t0);
    clock.written(
        &key(),
        &MoveStep::Abandon {
            successor: "broker-y".to_string(),
        },
        2,
        true,
        t0,
    );
    assert_eq!(clock.written(&key(), &cut_over(), 3, false, t0), None);
}

fn voters_report(index: u64, epoch: u64, voters: &[&str]) -> crate::model::RaftFloorReport {
    crate::model::RaftFloorReport {
        key: shard_zero(),
        incarnation: 0,
        epoch,
        index,
        term: 1,
        voters: Some(voters.iter().map(|node| (node.to_string(), 7)).collect()),
    }
}

/// **A change of a Raft shard's reported voters wakes placement once
/// (#3337).** A one-member move's shrink waits on exactly that report, and
/// leaders report every second, so the repeats, an older report and one that
/// does not name its sender wake nothing. A changed copy id makes the next
/// report news again.
#[test]
fn only_a_change_of_reported_voters_wakes_placement() {
    let wakes = PlacementWakes::default();
    let report = |node: &str, r| wakes.raft_voters_reported(node, &[r]);
    assert!(report("broker-a", voters_report(5, 0, &["broker-a"])));
    assert!(!report("broker-a", voters_report(6, 0, &["broker-a"])));
    assert!(report(
        "broker-a",
        voters_report(7, 0, &["broker-a", "broker-b"])
    ));
    for _ in 0..100 {
        assert!(!report(
            "broker-a",
            voters_report(8, 0, &["broker-a", "broker-b"])
        ));
    }
    assert!(
        !report("broker-a", voters_report(4, 0, &["broker-a"])),
        "older"
    );
    assert!(
        !report("broker-c", voters_report(9, 0, &["broker-a"])),
        "a sender outside its own voters leads no group"
    );
    assert!(
        report("broker-b", voters_report(1, 1, &["broker-b"])),
        "re-formed"
    );
    assert!(
        !report("broker-a", voters_report(99, 0, &["broker-a"])),
        "old epoch"
    );
    wakes.forget_raft_voters(&shard_zero());
    assert!(report("broker-b", voters_report(2, 1, &["broker-b"])));
}

/// However many reports ask for a pass while none is taken, one pass runs.
#[tokio::test]
async fn requested_passes_coalesce() {
    let wakes = PlacementWakes::default();
    for _ in 0..1000 {
        wakes.request_pass();
    }
    tokio::time::timeout(Duration::from_millis(100), wakes.pass_requested())
        .await
        .expect("one pass");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), wakes.pass_requested())
            .await
            .is_err(),
        "and no second"
    );
}
