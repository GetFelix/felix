//! Record headers: what a publish stores, what subscribers are told, and when
//! a broker refuses them.

use bytes::Bytes;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::RecordHeaders;

use crate::{Broker, BrokerError, DurableStorage, SequenceReuse, StartPosition, StreamMetadata};

async fn broker_with(
    stream: &str,
    metadata: StreamMetadata,
    storage: Option<DurableStorage>,
) -> Broker {
    let mut broker = Broker::new(EphemeralCache::new().into())
        .with_log_capacity(2)
        .expect("capacity");
    if let Some(storage) = storage {
        broker = broker.with_durable_storage(storage);
    }
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", stream, metadata)
        .await
        .expect("register");
    broker
}

fn storage(dir: &std::path::Path) -> DurableStorage {
    DurableStorage::open(
        dir,
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage")
}

fn durable() -> StreamMetadata {
    StreamMetadata {
        durable: true,
        ..StreamMetadata::default()
    }
}

fn block(name: &str) -> Bytes {
    RecordHeaders {
        key: Some(Bytes::from_static(b"k")),
        headers: vec![(name.to_owned(), Bytes::from_static(b"v"))],
    }
    .encode()
    .expect("block")
}

fn payloads(values: &[&'static str]) -> Vec<Bytes> {
    values
        .iter()
        .map(|v| Bytes::from_static(v.as_bytes()))
        .collect()
}

async fn stream_handle(broker: &Broker, stream: &str) -> crate::StreamHandle {
    broker
        .resolve_stream_handle("t1", "default", stream, 0)
        .await
        .expect("handle")
}

async fn stored_headers(broker: &Broker, stream: &str) -> Vec<Option<Bytes>> {
    broker
        .read_durable("t1", "default", stream, 0, 0, 1 << 20)
        .await
        .expect("read")
        .into_iter()
        .map(|record| record.headers.map(|block| *block))
        .collect()
}

/// A durable publish keeps each record's block in the log and tells live
/// subscribers the same blocks; a record with none stays without.
#[tokio::test]
async fn a_durable_publish_stores_and_delivers_its_headers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker_with("orders", durable(), Some(storage(dir.path()))).await;
    broker.record_headers_when(|| true);
    let resumed = broker
        .subscribe_from("t1", "default", "orders", 0, StartPosition::Latest)
        .await
        .expect("subscribe");
    let (mut receiver, _guard) = resumed.subscription.into_parts();

    let first = block("trace");
    let handle = stream_handle(&broker, "orders").await;
    broker
        .publish_batch_with_outcome(
            &handle,
            &payloads(&["a", "b"]),
            None,
            Some(&[Some(first.clone()), None]),
        )
        .await
        .expect("publish");

    let envelope = receiver.recv().await.expect("delivered");
    assert_eq!(
        envelope.record_headers(),
        Some(&[Some(first.clone()), None][..])
    );
    assert_eq!(
        stored_headers(&broker, "orders").await,
        vec![Some(first.clone()), None]
    );

    // The ring, which holds the last two, reports what was stored.
    let replayed = broker
        .subscribe_from("t1", "default", "orders", 0, StartPosition::Offset(0))
        .await
        .expect("resume");
    let ring: Vec<_> = replayed
        .backlog
        .into_iter()
        .map(|record| record.headers)
        .collect();
    assert_eq!(ring, vec![Some(first), None]);
}

/// A restarted broker fills its ring, and a reader finds the log, with the
/// blocks stored.
#[tokio::test]
async fn headers_survive_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sent = block("trace");
    {
        let broker = broker_with("orders", durable(), Some(storage(dir.path()))).await;
        broker.record_headers_when(|| true);
        let handle = stream_handle(&broker, "orders").await;
        broker
            .publish_batch_with_outcome(
                &handle,
                &payloads(&["a"]),
                None,
                Some(&[Some(sent.clone())]),
            )
            .await
            .expect("publish");
    }
    let broker = broker_with("orders", durable(), Some(storage(dir.path()))).await;
    assert_eq!(
        stored_headers(&broker, "orders").await,
        vec![Some(sent.clone())]
    );
    let resumed = broker
        .subscribe_from("t1", "default", "orders", 0, StartPosition::Offset(0))
        .await
        .expect("resume");
    assert_eq!(resumed.backlog[0].headers, Some(sent));
}

/// A broker that does not accept headers refuses a durable publish that
/// carries them, with a typed error, and writes nothing.
#[tokio::test]
async fn a_durable_publish_with_headers_is_refused_while_the_gate_is_off() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker_with("orders", durable(), Some(storage(dir.path()))).await;
    let handle = stream_handle(&broker, "orders").await;
    let blocks = [Some(block("trace"))];

    let refused = broker
        .publish_batch_with_outcome(&handle, &payloads(&["a"]), None, Some(&blocks))
        .await
        .expect_err("refused");
    assert!(matches!(refused, BrokerError::RecordHeadersNotEnabled));
    assert!(stored_headers(&broker, "orders").await.is_empty());

    // The claim path refuses before it takes an offset.
    let refused = broker
        .claim_publish(&handle, &payloads(&["a"]), None, Some(&blocks))
        .await;
    assert!(matches!(refused, Err(BrokerError::RecordHeadersNotEnabled)));

    // The same batch goes through once the node says so.
    broker.record_headers_when(|| true);
    broker
        .publish_batch_with_outcome(&handle, &payloads(&["a"]), None, Some(&blocks))
        .await
        .expect("accepted");
    assert_eq!(stored_headers(&broker, "orders").await.len(), 1);
}

/// A publish without headers writes the records it always did, and a frame
/// that listed no block at all is the same publish.
#[tokio::test]
async fn a_publish_without_headers_stores_none() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker_with("orders", durable(), Some(storage(dir.path()))).await;
    broker.record_headers_when(|| true);
    let resumed = broker
        .subscribe_from("t1", "default", "orders", 0, StartPosition::Latest)
        .await
        .expect("subscribe");
    let (mut receiver, _guard) = resumed.subscription.into_parts();
    let handle = stream_handle(&broker, "orders").await;

    broker
        .publish_batch_with_outcome(&handle, &payloads(&["a"]), None, None)
        .await
        .expect("publish");
    broker
        .publish_batch_with_outcome(&handle, &payloads(&["b", "c"]), None, Some(&[None, None]))
        .await
        .expect("publish");

    assert!(
        receiver
            .recv()
            .await
            .expect("first")
            .record_headers()
            .is_none()
    );
    assert!(
        receiver
            .recv()
            .await
            .expect("second")
            .record_headers()
            .is_none()
    );
    assert_eq!(
        stored_headers(&broker, "orders").await,
        vec![None, None, None]
    );
    // A gate that is off refuses nothing here: there is nothing to refuse.
    let off = broker_with("plain", durable(), Some(storage(dir.path()))).await;
    let plain = stream_handle(&off, "plain").await;
    off.publish_batch_with_outcome(&plain, &payloads(&["a"]), None, Some(&[None]))
        .await
        .expect("no blocks, no refusal");
}

/// An in-memory stream stores nothing, so it has nothing to gate: headers
/// reach its subscribers whatever the node says.
#[tokio::test]
async fn an_in_memory_stream_delivers_headers_without_the_gate() {
    let broker = broker_with("live", StreamMetadata::default(), None).await;
    let resumed = broker
        .subscribe_from("t1", "default", "live", 0, StartPosition::Latest)
        .await
        .expect("subscribe");
    let (mut receiver, _guard) = resumed.subscription.into_parts();
    let sent = block("trace");
    let handle = stream_handle(&broker, "live").await;
    broker
        .publish_batch_with_outcome(
            &handle,
            &payloads(&["a", "b"]),
            None,
            Some(&[None, Some(sent.clone())]),
        )
        .await
        .expect("publish");
    let envelope = receiver.recv().await.expect("delivered");
    assert_eq!(envelope.record_headers(), Some(&[None, Some(sent)][..]));
}

/// Header blocks that do not line up with the records are not a batch's
/// headers: refused before anything is written.
#[tokio::test]
async fn a_block_count_that_does_not_match_the_records_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker_with("orders", durable(), Some(storage(dir.path()))).await;
    broker.record_headers_when(|| true);
    let handle = stream_handle(&broker, "orders").await;
    let refused = broker
        .publish_batch_with_outcome(
            &handle,
            &payloads(&["a", "b"]),
            None,
            Some(&[Some(block("x"))]),
        )
        .await;
    assert!(refused.is_err());
    assert!(stored_headers(&broker, "orders").await.is_empty());
}

/// An idempotent batch stores its headers like any other, and a re-send is
/// answered as the duplicate it is.
#[tokio::test]
async fn an_idempotent_batch_stores_its_headers_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker_with("orders", durable(), Some(storage(dir.path()))).await;
    broker.record_headers_when(|| true);
    let handle = stream_handle(&broker, "orders").await;
    let sent = block("trace");
    let blocks = [Some(sent.clone()), None];
    let batch = payloads(&["a", "b"]);

    let claimed = broker
        .claim_batch_idempotent(
            &handle,
            7,
            0,
            &batch,
            SequenceReuse::Refuse,
            None,
            Some(&blocks),
        )
        .await
        .expect("claim");
    let first = broker.complete_idempotent(claimed).await.expect("complete");
    assert!(!first.duplicate);

    let claimed = broker
        .claim_batch_idempotent(
            &handle,
            7,
            0,
            &batch,
            SequenceReuse::Refuse,
            None,
            Some(&blocks),
        )
        .await
        .expect("claim");
    let again = broker.complete_idempotent(claimed).await.expect("complete");
    assert!(again.duplicate);
    assert_eq!(
        stored_headers(&broker, "orders").await,
        vec![Some(sent), None]
    );
}
