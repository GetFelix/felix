//! A follower storing records with header blocks.

use felix_wire::RecordHeaders;

use super::trim::{quorum_follower, stored_ok};
use super::*;

/// **A follower marks its data directory before it stores a record with a
/// header block.** The marker is what lets it refuse a fleet that has not
/// finalized `record_headers`, so such a record must never be durable
/// without it.
#[tokio::test]
async fn a_follower_writes_the_record_headers_marker_before_it_stores_a_block() {
    let (broker, dir) = quorum_follower().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    stored_ok(
        &handler
            .apply(SENDER, batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
            .await,
    );
    assert!(
        !felix_storage::run_state::has_record_headers(dir.path()).expect("read"),
        "an ordinary batch leaves no marker"
    );

    let block = RecordHeaders {
        key: Some(Bytes::from_static(b"k")),
        headers: Vec::new(),
    }
    .encode()
    .expect("block");
    let mut headered = batch(4, 1, &["b"]);
    headered.headers = Some(vec![Some(block)]);
    headered.checksum = batch_checksum(
        &headered.payloads,
        &[],
        &[],
        headered.headers.as_deref().unwrap_or_default(),
    );

    // A directory in the marker's temporary path makes its write fail.
    let blocker = dir.path().join("record-headers.tmp");
    std::fs::create_dir(&blocker).expect("block the marker");
    let answer = handler
        .apply(SENDER, headered.clone(), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(&answer, InternalMessage::ReplicateError(_)),
        "a block whose marker cannot be written is refused, got {answer:?}"
    );
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    assert_eq!(log.tail_offset().await.expect("tail"), 1, "nothing stored");

    std::fs::remove_dir(&blocker).expect("unblock");
    stored_ok(
        &handler
            .apply(SENDER, headered, felix_broker::LogKind::Stream)
            .await,
    );
    assert!(felix_storage::run_state::has_record_headers(dir.path()).expect("read"));
}
