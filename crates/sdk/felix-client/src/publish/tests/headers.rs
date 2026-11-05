//! The frame a publish with headers puts on the wire, and when it is not sent.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use bytes::Bytes;
use felix_wire::{AckMode, RecordHeaders};
use tokio::sync::mpsc;

use crate::RecordHeadersError;
use crate::publish::writer::{PublishRequest, PublishWorker};
use crate::publish::{Acked, PublishSharding, Publisher, PublisherInner};

fn publisher(takes_record_headers: bool) -> (Publisher, mpsc::Receiver<PublishRequest>) {
    let (tx, rx) = mpsc::channel::<PublishRequest>(2);
    let publisher = Publisher {
        inner: Arc::new(PublisherInner::new(
            Arc::new(vec![PublishWorker {
                tx,
                handle: tokio::sync::Mutex::new(None),
                request_counter: AtomicU64::new(1),
                server_flags: felix_wire::KNOWN_FLAGS | felix_wire::FLAG_EXTENDED,
                publish_window: 0,
                takes_width: false,
                takes_shard: false,
                takes_record_headers,
                head: Default::default(),
            }]),
            PublishSharding::RoundRobin,
        )),
    };
    (publisher, rx)
}

fn headers(name: &str) -> RecordHeaders {
    RecordHeaders {
        key: Some(Bytes::from_static(b"k")),
        headers: vec![(name.to_string(), Bytes::from_static(b"v"))],
    }
}

/// Run `publish` against a writer that answers at once, and return the bytes
/// of the one frame it queued, or `None` if it queued nothing. The publisher
/// is dropped with the call, which closes the queue and ends the wait.
async fn frame_sent<F>(
    takes_record_headers: bool,
    publish: impl FnOnce(Publisher) -> F + Send + 'static,
) -> (anyhow::Result<Option<u64>>, Option<Bytes>)
where
    F: Future<Output = anyhow::Result<Option<u64>>> + Send + 'static,
{
    let (publisher, mut rx) = publisher(takes_record_headers);
    let task = tokio::spawn(async move { publish(publisher).await });
    let sent = tokio::select! {
        request = rx.recv() => match request {
            Some(PublishRequest::BinaryBytes { bytes, response, .. }) => {
                let _ = response.send(Ok(Acked::default()));
                Some(bytes)
            }
            Some(_) => panic!("expected a binary frame"),
            None => None,
        },
    };
    (task.await.expect("task"), sent)
}

/// **The frame carries each record's block**, acked or not, and decodes to
/// what was published.
#[tokio::test]
async fn the_frame_carries_each_records_block() {
    let records = || {
        vec![
            (headers("a"), b"one".to_vec()),
            (RecordHeaders::default(), b"two".to_vec()),
        ]
    };
    for ack in [AckMode::None, AckMode::PerBatch] {
        let (outcome, bytes) = frame_sent(true, move |publisher| async move {
            publisher
                .publish_batch_with_headers("t", "ns", "s", records(), ack)
                .await
        })
        .await;
        outcome.expect("publish");
        let frame = felix_wire::Frame::decode(bytes.expect("a frame")).expect("frame");
        let batch = if ack == AckMode::None {
            felix_wire::binary::decode_publish_batch(&frame).expect("decode")
        } else {
            felix_wire::binary::decode_acked_publish_batch(&frame)
                .expect("decode")
                .batch
        };
        assert_eq!(batch.payloads, [&b"one"[..], b"two"]);
        assert_eq!(
            batch.record_headers,
            Some(vec![Some(headers("a").encode().expect("encode")), None])
        );
    }
}

/// **Not sent, not downgraded**: a stream whose broker did not answer the
/// extension queues nothing.
#[tokio::test]
async fn a_stream_that_did_not_answer_the_extension_queues_nothing() {
    let (outcome, bytes) = frame_sent(false, move |publisher| async move {
        publisher
            .publish_with_headers(
                "t",
                "ns",
                "s",
                headers("a"),
                b"x".to_vec(),
                AckMode::PerBatch,
            )
            .await
    })
    .await;
    let err = outcome.expect_err("unsupported");
    assert_eq!(
        err.downcast_ref::<RecordHeadersError>(),
        Some(&RecordHeadersError::Unsupported)
    );
    assert!(bytes.is_none());
}

/// **Names the broker reserves never leave the client**, even where the broker
/// answered the extension.
#[tokio::test]
async fn a_reserved_or_empty_name_queues_nothing() {
    for (name, expected) in [
        (
            "felix.offset",
            RecordHeadersError::ReservedName {
                name: "felix.offset".into(),
            },
        ),
        ("", RecordHeadersError::EmptyName),
    ] {
        let (outcome, bytes) = frame_sent(true, move |publisher| async move {
            publisher
                .publish_with_headers(
                    "t",
                    "ns",
                    "s",
                    headers(name),
                    b"x".to_vec(),
                    AckMode::PerBatch,
                )
                .await
        })
        .await;
        let err = outcome.expect_err("refused");
        assert_eq!(err.downcast_ref::<RecordHeadersError>(), Some(&expected));
        assert!(bytes.is_none());
    }
}

/// **With nothing to carry it is the plain publish**, byte for byte, and needs
/// no extension from the broker.
#[tokio::test]
async fn records_without_headers_give_the_plain_frame() {
    for ack in [AckMode::None, AckMode::PerBatch] {
        let (outcome, with) = frame_sent(false, move |publisher| async move {
            publisher
                .publish_batch_with_headers(
                    "t",
                    "ns",
                    "s",
                    vec![(RecordHeaders::default(), b"one".to_vec())],
                    ack,
                )
                .await
        })
        .await;
        outcome.expect("publish");
        let (outcome, plain) = frame_sent(false, move |publisher| async move {
            publisher
                .publish_batch("t", "ns", "s", vec![b"one".to_vec()], ack)
                .await
        })
        .await;
        outcome.expect("publish");
        assert!(with.is_some());
        assert_eq!(with, plain);
    }
}
