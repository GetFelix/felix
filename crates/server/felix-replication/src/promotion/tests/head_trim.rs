//! A trim a promoted leader inherits by taking a replica's tail.

use felix_broker::LogKind;
use felix_storage::log::RecordMark;

use super::*;

const FLOOR: u64 = 3;

/// **A trim in a tail the promoted leader takes stays a trim.**
///
/// broker-b lags and broker-c holds six records and a trim to floor 3, which
/// the old leader may have acknowledged. The fence takes c's tail through
/// `ReplicateFetch`, and the leader must store the trim as a trim, not as a
/// plain record, or the floor is lost with the failover. The floor still
/// applies only once the commit offset passes the trim.
#[tokio::test]
async fn a_trim_in_a_taken_tail_is_stored_as_a_trim() {
    let mut replicas = Replicas::new();
    let c_log = replicas
        .get("broker-c")
        .broker
        .shard_log(LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    c_log.record_generation(4, 0).expect("generation");
    let values: Vec<Bytes> = (0..6).map(|i| Bytes::from(format!("v{i}"))).collect();
    c_log.append(&values).await.expect("append");
    let pending = c_log
        .begin_append_marked_at(
            6,
            &[Bytes::from_static(b"tag")],
            &[RecordMark::Trim { floor: FLOOR }],
            &[],
            &[],
        )
        .await
        .expect("append")
        .expect("at the tail");
    c_log.commit(&pending).await.expect("commit");

    // The lagging leader holds the first record only; with c it is a majority.
    replicas.get_mut("broker-b").reachable = false;
    let (leader, _dir) = leader_holding(4, &["v0"]).await;
    let log = leader
        .shard_log(LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    log.hold_retention_at_commit(true).await.expect("hold");

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route(), true).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: Some("broker-c".to_string())
        }
    );
    let stored = log
        .read_replicated_from(log.physical_base(), 1 << 20)
        .await
        .expect("read");
    assert_eq!(stored.len(), 7);
    assert_eq!(stored[6].mark, RecordMark::Trim { floor: FLOOR });
    assert_eq!(
        log.readable_base(),
        0,
        "the commit offset has not passed it"
    );
    log.advance_commit_offset(7).await.expect("commit offset");
    assert_eq!(log.readable_base(), FLOOR);
}
