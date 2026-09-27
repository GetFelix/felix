//! What leaders report about their replicas, as every backend must keep it.
use super::shards::{assignment, key};
use crate::model::ReplicaReport;
use crate::store::{ControlPlaneStore, ReportWrite, StoreError};

/// The leader every case assigns, and so the node its reports come from.
const LEADER: &str = "broker-x";

/// A report is read back exactly as recorded, by whichever instance asks:
/// that is the property promotion depends on across several control planes.
pub(super) async fn a_replica_report_is_kept_and_read_back(store: &dyn ControlPlaneStore) {
    let shard = 3;
    store
        .put_shard_assignment(assignment(shard, LEADER))
        .await
        .expect("assign");
    let recorded = report(shard, generation(store, shard).await, &["broker-b"], 5_000);
    assert_eq!(
        store
            .record_replica_report(recorded.clone(), LEADER)
            .await
            .expect("record"),
        ReportWrite::Stored
    );

    assert_eq!(report_for(store, shard).await, Some(unstamped(recorded)));
}

/// **An older generation's report is dropped**, silently: leadership moved on,
/// and the old leader's view is about a replica set that may no longer exist.
pub(super) async fn a_report_from_a_superseded_leader_is_dropped(store: &dyn ControlPlaneStore) {
    let shard = 3;
    let old = generation(store, shard).await;
    store
        .put_shard_assignment(assignment(shard, LEADER))
        .await
        .expect("reassign");
    let current = report(shard, generation(store, shard).await, &[], 6_000);
    assert_eq!(
        store
            .record_replica_report(current.clone(), LEADER)
            .await
            .expect("record"),
        ReportWrite::Stored
    );
    assert_eq!(
        store
            .record_replica_report(report(shard, old, &["broker-b"], 6_100), LEADER)
            .await
            .expect("an old report is dropped, not refused"),
        ReportWrite::Stale,
        "a dropped report must not read as stored",
    );

    assert_eq!(
        report_for(store, shard).await,
        Some(unstamped(current)),
        "a superseded leader's report overwrote the current one",
    );
}

/// A report at the same generation is an update, not a stale duplicate: the
/// same leader reporting again is exactly the normal case.
pub(super) async fn a_report_at_the_same_generation_is_an_update(store: &dyn ControlPlaneStore) {
    let shard = 3;
    let later = report(shard, generation(store, shard).await, &["broker-b"], 6_200);
    assert_eq!(
        store
            .record_replica_report(later.clone(), LEADER)
            .await
            .expect("record"),
        ReportWrite::Stored
    );

    assert_eq!(report_for(store, shard).await, Some(unstamped(later)));
}

/// **Within a generation, reports only move forward.** A request that lands
/// after a newer one, with a smaller leader tail, is dropped: it would put
/// back a view in which a follower that has since fallen behind looks caught
/// up, and failover promotes on that view.
pub(super) async fn a_report_behind_the_held_one_is_dropped(store: &dyn ControlPlaneStore) {
    let shard = 3;
    let at = generation(store, shard).await;
    let mut newer = report(shard, at, &[], 6_300);
    newer.leader_offset = Some(40);
    assert_eq!(
        store
            .record_replica_report(newer.clone(), LEADER)
            .await
            .expect("record"),
        ReportWrite::Stored
    );
    let mut late = report(shard, at, &["broker-b"], 6_400);
    late.leader_offset = Some(30);
    assert_eq!(
        store
            .record_replica_report(late, LEADER)
            .await
            .expect("record"),
        ReportWrite::Stale
    );
    assert_eq!(report_for(store, shard).await, Some(unstamped(newer)));

    // The same tail again is the normal case of a quiet shard, and updates.
    let mut level = report(shard, at, &["broker-b"], 6_500);
    level.leader_offset = Some(40);
    assert_eq!(
        store
            .record_replica_report(level.clone(), LEADER)
            .await
            .expect("record"),
        ReportWrite::Stored
    );
    assert_eq!(report_for(store, shard).await, Some(unstamped(level)));
}

/// **A deposed leader's report is refused as it is written**, not only when
/// the API read the assignment. A report checked against a read taken before
/// a promotion can reach the store after it; stored, it would put the old
/// leader's replica positions under the new leader's shard.
pub(super) async fn a_report_from_a_deposed_leader_is_refused(store: &dyn ControlPlaneStore) {
    let shard = 3;
    let before = generation(store, shard).await;
    let held = report_for(store, shard).await;
    assert!(held.is_some(), "the case needs a report to protect");

    store
        .put_shard_assignment(assignment(shard, "broker-y"))
        .await
        .expect("promote");
    let after = generation(store, shard).await;

    for at in [before, after] {
        assert_eq!(
            store
                .record_replica_report(report(shard, at, &["broker-b"], 6_600), LEADER)
                .await
                .expect("a refused report is an answer, not an error"),
            ReportWrite::NotLeader,
            "the old leader's report at generation {at} was not refused",
        );
    }
    // The new leader cannot report against the generation it replaced.
    assert_eq!(
        store
            .record_replica_report(report(shard, before, &["broker-b"], 6_700), "broker-y")
            .await
            .expect("record"),
        ReportWrite::Stale,
    );
    assert_eq!(
        report_for(store, shard).await,
        held,
        "a refused report changed the stored one",
    );

    // Back to the leader the cases after this one expect.
    store
        .put_shard_assignment(assignment(shard, LEADER))
        .await
        .expect("restore");
}

/// Nobody leads an unassigned shard, so nobody can report on it; and a
/// deleted assignment takes its report with it, so a shard removed and
/// recreated does not inherit the old one's promotability.
pub(super) async fn a_report_needs_an_assignment_and_goes_with_it(store: &dyn ControlPlaneStore) {
    let unassigned = 2;
    let err = store
        .record_replica_report(report(unassigned, 1, &["broker-b"], 7_000), LEADER)
        .await
        .expect_err("a report on an unassigned shard was kept");
    assert!(matches!(err, StoreError::NotFound(_)), "{err:?}");

    let shard = 3;
    assert!(report_for(store, shard).await.is_some());
    store
        .delete_shard_assignment(&key(shard))
        .await
        .expect("delete");
    assert_eq!(
        report_for(store, shard).await,
        None,
        "a deleted assignment left its report behind",
    );
}

/// The drained flag rides the report and is read back with it.
pub(super) async fn a_drained_report_is_kept(store: &dyn ControlPlaneStore) {
    let shard = 2;
    store
        .put_shard_assignment(assignment(shard, "broker-x"))
        .await
        .expect("assign");
    let mut drained = report(shard, generation(store, shard).await, &["broker-y"], 7_000);
    drained.drained = true;
    assert_eq!(
        store
            .record_replica_report(drained.clone(), LEADER)
            .await
            .expect("record"),
        ReportWrite::Stored
    );
    assert_eq!(report_for(store, shard).await, Some(unstamped(drained)));
}

/// The leader's own tail rides the report too.
pub(super) async fn the_leader_offset_is_kept(store: &dyn ControlPlaneStore) {
    let shard = 3;
    store
        .put_shard_assignment(assignment(shard, "broker-x"))
        .await
        .expect("assign");
    let mut with_tail = report(shard, generation(store, shard).await, &["broker-y"], 7_000);
    with_tail.leader_offset = Some(12);
    assert_eq!(
        store
            .record_replica_report(with_tail.clone(), LEADER)
            .await
            .expect("record"),
        ReportWrite::Stored
    );
    assert_eq!(report_for(store, shard).await, Some(unstamped(with_tail)));
}

fn report(shard: u32, generation: u64, caught_up: &[&str], at: u64) -> ReplicaReport {
    ReplicaReport {
        key: key(shard),
        generation,
        caught_up: caught_up.iter().map(|n| n.to_string()).collect(),
        offsets: caught_up.iter().map(|n| (n.to_string(), 10)).collect(),
        reported_at_millis: at,
        drained: false,
        leader_offset: None,
    }
}

/// The generation `shard`'s assignment is at: the store owns it.
async fn generation(store: &dyn ControlPlaneStore, shard: u32) -> u64 {
    store
        .get_shard_assignment(&key(shard))
        .await
        .expect("assignment")
        .generation
}

/// The report held for `shard`, with its stamp normalised away.
///
/// Under Raft the leader replaces `reported_at_millis` with its own clock as
/// the command enters the log, so the stamp read back is the leader's and not
/// the caller's; everything else must come back exactly as recorded.
async fn report_for(store: &dyn ControlPlaneStore, shard: u32) -> Option<ReplicaReport> {
    store
        .list_replica_reports()
        .await
        .expect("list reports")
        .into_iter()
        .find(|report| report.key == key(shard))
        .map(|report| ReplicaReport {
            reported_at_millis: 0,
            ..report
        })
}

fn unstamped(report: ReplicaReport) -> ReplicaReport {
    ReplicaReport {
        reported_at_millis: 0,
        ..report
    }
}
