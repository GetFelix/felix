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
        })
        .await
        .expect("fenced move");

    let wakes = Arc::new(PlacementWakes::default());
    let written = wakes.watch_assignments();
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
                key: shard_zero(),
                generation: fenced.generation,
                caught_up: ["broker-y".to_string()].into_iter().collect(),
                offsets: Default::default(),
                reported_at_millis: store.now_millis().await.expect("clock"),
                drained: true,
                leader_offset: None,
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
        "the write wakes this instance's long-polls",
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
