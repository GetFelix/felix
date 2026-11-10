//! Record key-and-headers blocks through replication and promotion: a follower
//! stores the leader's, so a promoted replica delivers the headers readers saw
//! before the failover.

use felix_broker::LogKind;
use felix_wire::internal::{Fence, GenerationStart, ReplicateFetch};

use super::times::{Follower, TIMES, ship_all, stream_log, timed_leader};
use super::*;

fn block(text: &'static str) -> Bytes {
    Bytes::from_static(text.as_bytes())
}

/// The blocks of `a`, `b` and `c`: only `b` has one.
fn blocks() -> Vec<Option<Bytes>> {
    vec![None, Some(block("block")), None]
}

fn payloads() -> Vec<Bytes> {
    ["a", "b", "c"].map(block).to_vec()
}

/// A leader at `generation` holding `a`, `b` and `c`, with `blocks()`.
async fn headered_leader(generation: u64) -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker_on(dir.path());
    let log = stream_log(&broker).await;
    log.record_generation(generation, 0).expect("generation");
    let pending = log
        .begin_append_marked_at(
            0,
            &payloads(),
            &[],
            &[],
            &TIMES,
            Some(&blocks()),
            felix_storage::log::WrittenBy::Leader,
        )
        .await
        .expect("append")
        .expect("at the tail");
    log.commit(&pending).await.expect("commit");
    (broker, dir)
}

/// The blocks `broker` holds for the shard's records.
async fn stored_blocks(broker: &Broker) -> Option<Vec<Option<Bytes>>> {
    let records = stream_log(broker)
        .await
        .read_replicated_from(0, 1 << 20)
        .await
        .expect("read");
    felix_broker::replication::headers_to_wire(&records)
}

/// A batch of `a`, `b` and `c` at `generation` with `blocks()`, as a leader
/// that offers everything ships it.
fn headered_batch(generation: u64) -> ReplicateRecords {
    let mut shipped = batch(generation, 0, &["a", "b", "c"]);
    shipped.generations = Some(vec![GenerationStart {
        generation,
        start_offset: 0,
    }]);
    shipped.times = Some(TIMES.to_vec());
    shipped.checksum = batch_checksum(&shipped.payloads, &[], &[], &blocks());
    shipped.headers = Some(blocks());
    shipped
}

fn offers_headers() -> PeerCapabilities {
    REQUIRED
        .union(PeerCapabilities::GENERATION_LABELS)
        .union(PeerCapabilities::RECORD_TIMES)
        .union(PeerCapabilities::RECORD_HEADERS)
}

/// **A follower stores each block with its record**, so its log reads back
/// what the leader's does and a promoted replica delivers the same headers.
#[tokio::test]
async fn a_follower_stores_the_leaders_header_blocks() {
    let (leader, _leader_dir) = headered_leader(1).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let replica = broker_on(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&replica), router_for("broker-b", 1));
    let follower = Follower::new(&handler, offers_headers());

    ship_all(&follower, &leader, 1).await;

    assert_eq!(
        *follower.sent.lock().expect("lock"),
        vec![Kind::ReplicateHeaderedRecords]
    );
    assert_eq!(stored_blocks(&replica).await, Some(blocks()));
    assert_eq!(held(&replica).await, vec!["a", "b", "c"]);
}

/// **A promoted leader that takes a replica's tail takes its blocks too**:
/// the catch-up fetch asks for them, and the records it appends carry them.
#[tokio::test]
async fn a_tail_taken_from_a_replica_keeps_its_header_blocks() {
    let mut replicas = Replicas::new();
    for id in ["broker-b", "broker-c"] {
        replicas.get_mut(id).capabilities = offers_headers();
    }
    let handler = ReplicaHandler::new(
        Arc::clone(&replicas.get("broker-b").broker),
        router_for("broker-b", 4),
    );
    let answer = handler
        .apply(None, headered_batch(4), LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = timed_leader(4, &["a"]).await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route(), true, None).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: Some("broker-b".to_string())
        }
    );
    assert_eq!(held(&leader).await, vec!["a", "b", "c"]);
    assert_eq!(stored_blocks(&leader).await, Some(blocks()));
    assert!(!replicas.sent(Kind::ReplicateHeaderedFetch).is_empty());
}

/// **A replica will not hand a fetch that did not ask for blocks a page that
/// holds one**: the reader would store those records bare and differ from the
/// replica.
#[tokio::test]
async fn a_fetch_that_did_not_ask_for_blocks_is_refused_a_page_that_has_one() {
    let replicas = Replicas::new();
    let replica = replicas.get("broker-b");
    let storing = ReplicaHandler::new(Arc::clone(&replica.broker), router_for("broker-b", 4));
    let answer = storing
        .apply(None, headered_batch(4), LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    let handler = &replica.handler;
    let shard = batch(PROMOTED, 0, &[]).shard;
    let fence = handler
        .fence(
            None,
            Fence {
                correlation_id: 2,
                shard: shard.clone(),
                log: ReplicaLog::Stream,
                election: None,
            },
        )
        .await;
    assert!(matches!(fence, InternalMessage::FenceOk(_)), "{fence:?}");
    let fetch = |headers| ReplicateFetch {
        writers: false,
        correlation_id: 3,
        shard: shard.clone(),
        log: ReplicaLog::Stream,
        from_offset: 0,
        max_bytes: 1 << 20,
        labelled: true,
        timed: true,
        headers,
    };

    let refused = handler.fetch(None, fetch(false)).await;
    let InternalMessage::ReplicateError(refused) = refused else {
        panic!("{refused:?}");
    };
    assert_eq!(refused.code, ErrorCode::UnsupportedKind);

    let served = handler.fetch(None, fetch(true)).await;
    let InternalMessage::ReplicateRecords(served) = served else {
        panic!("{served:?}");
    };
    assert_eq!(served.headers, Some(blocks()));
    assert_eq!(
        InternalMessage::ReplicateRecords(served).kind(),
        Kind::ReplicateHeaderedRecords
    );
}

/// A replica that offered no headers capability is never asked for blocks,
/// and a tail it holds without any is taken as before.
#[tokio::test]
async fn a_replica_without_the_offer_is_fetched_from_without_blocks() {
    let mut replicas = Replicas::new();
    for id in ["broker-b", "broker-c"] {
        replicas.get_mut(id).capabilities = REQUIRED
            .union(PeerCapabilities::GENERATION_LABELS)
            .union(PeerCapabilities::RECORD_TIMES);
    }
    let mut shipped = headered_batch(4);
    shipped.headers = None;
    shipped.checksum = batch_checksum(&shipped.payloads, &[], &[], &[]);
    let handler = ReplicaHandler::new(
        Arc::clone(&replicas.get("broker-b").broker),
        router_for("broker-b", 4),
    );
    let answer = handler.apply(None, shipped, LogKind::Stream).await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = timed_leader(4, &["a"]).await;

    fence_shard(&replicas, &leader, LEADER, &key(), &route(), true, None).await;

    assert!(replicas.sent(Kind::ReplicateHeaderedFetch).is_empty());
    assert_eq!(stored_blocks(&leader).await, None);
}

/// Only a stream's log has blocks. A headered batch for another log is
/// refused, because storing it without them would leave that log unlike the
/// leader's.
#[tokio::test]
async fn a_headered_batch_for_another_log_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let replica = broker_on(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&replica), router_for("broker-b", 4));

    for log in [LogKind::Cache, LogKind::GroupCursors] {
        let answer = handler.apply(None, headered_batch(4), log).await;
        let InternalMessage::ReplicateError(refused) = answer else {
            panic!("{log:?}: {answer:?}");
        };
        assert_eq!(refused.code, ErrorCode::Malformed, "{log:?}");
    }
}
