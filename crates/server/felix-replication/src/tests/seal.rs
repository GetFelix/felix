//! A seal leader's shipping: while its promise at the shard's generation
//! holds, its batches name its copy, and once it claimed the record at `N`
//! it ships nothing from there on.

use felix_storage::StorageId;
use felix_storage::disk_log::DiskLog;

use super::*;

fn own() -> StorageId {
    StorageId::from_bytes([1; 16])
}

fn on_commit() -> LogConfig {
    LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

/// A leader at `GENERATION` holding `a b c`, promised to its own seal, and
/// its two side logs.
async fn sealing_leader() -> (StreamLog, [DiskLog; 2], TempDir, felix_broker::Broker) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(dir.path().join("streams"), on_commit()).expect("storage");
    let broker = felix_broker::Broker::new(felix_storage::EphemeralCache::new().into())
        .with_durable_storage(storage);
    broker.record_generations_when(|| true);
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("open");
    log.accept_generation(GENERATION, Some("broker-a"))
        .await
        .expect("accept");
    log.record_generation(GENERATION, 0).expect("record");
    log.lead_at(GENERATION);
    for value in ["a", "b", "c"] {
        log.append(&[Bytes::copy_from_slice(value.as_bytes())])
            .await
            .expect("append");
    }
    log.disk_log()
        .promise_seal(GENERATION, "broker-a", own())
        .await
        .expect("promise")
        .expect("one sealer");
    let side =
        |name: &str| DiskLog::open(dir.path().join(name), name, on_commit()).expect("side log");
    let sides = [side("dead-letters"), side("cursors")];
    (log, sides, dir, broker)
}

async fn ship_from_start(follower: &ScriptedFollower, log: &StreamLog) -> Vec<String> {
    let mut cursor = cursor(0);
    ship_once(
        follower,
        log,
        &shard(),
        felix_broker::LogKind::Stream,
        &mut cursor,
        BATCH_BYTES,
        &Rebuilds::disabled(),
    )
    .await;
    follower
        .sent()
        .into_iter()
        .flat_map(|(_, payloads)| payloads)
        .collect()
}

/// **A promised leader's batches name its copy** to a follower that offered
/// the seal, so a follower holding the same promise takes them. One that did
/// not offer it is sent the frame it always was.
#[tokio::test]
async fn a_promised_leaders_batches_name_its_copy() {
    let (log, _sides, _dir, _broker) = sealing_leader().await;
    for (offered, copies) in [
        (PeerCapabilities::SWITCH_SEAL, vec![own().to_bytes()]),
        (PeerCapabilities::NONE, Vec::new()),
    ] {
        let follower = ScriptedFollower::new([Ok(stored(3))]).offering(offered);
        assert_eq!(
            ship_from_start(&follower, &log).await,
            vec_of(&["a", "b", "c"])
        );
        assert_eq!(*follower.copies.lock().expect("lock"), copies);
    }
}

/// **Once the leader claimed the record at `N`, it ships nothing from
/// there.** A follower takes the record only through the seal's
/// acknowledgement, which checks its copy first; shipped as a batch it would
/// land unchecked.
#[tokio::test]
async fn a_claimed_seal_record_is_not_shipped() {
    let (log, [dead_letters, cursors], _dir, _broker) = sealing_leader().await;
    let base = log
        .disk_log()
        .claim_seal(own(), 0, (&dead_letters, 0), (&cursors, 0))
        .await
        .expect("claim")
        .expect("claimed");
    assert_eq!(base.offset, 3);
    assert_eq!(log.tail_offset().await.expect("tail"), 4);

    let follower = ScriptedFollower::new([Ok(stored(3))]).offering(PeerCapabilities::SWITCH_SEAL);
    assert_eq!(
        ship_from_start(&follower, &log).await,
        vec_of(&["a", "b", "c"])
    );
}
