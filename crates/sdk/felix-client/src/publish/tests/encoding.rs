use std::sync::Arc;
use std::sync::atomic::AtomicU64;

#[cfg(feature = "telemetry")]
use anyhow::{Context, Result};
use felix_wire::{AckMode, Message};
use tokio::sync::mpsc;

#[cfg(feature = "telemetry")]
use crate::publish::PublishAdmission;
use crate::publish::writer::{PublishRequest, PublishWorker};
use crate::publish::{Acked, PublishSharding, Publisher, PublisherInner};

/// An acked batch falls back to JSON against a broker that never advertised
/// the acked binary frame, and the fallback carries no key.
///
/// The fallback used to route through the public `publish_batch_json`, which
/// is deprecated and goes away in 0.6.0. It now calls the private keyed form
/// with `key: None`, and this pins that the rewiring did not quietly start
/// sending a key — an empty key is a key, and would hash to a shard rather
/// than resolving to shard 0.
#[tokio::test]
async fn an_acked_batch_falls_back_to_keyless_json_without_the_binary_flag() {
    let (tx, mut rx) = mpsc::channel::<PublishRequest>(2);
    let publisher = Publisher {
        inner: Arc::new(PublisherInner::new(
            Arc::new(vec![PublishWorker {
                tx,
                handle: tokio::sync::Mutex::new(None),
                request_counter: AtomicU64::new(1),
                // What a broker predating capability negotiation resolves to.
                server_flags: felix_wire::ORIGINAL_V1_FLAGS,
                publish_window: 0,
            }]),
            PublishSharding::RoundRobin,
        )),
    };

    let publish = tokio::spawn({
        let publisher = publisher.clone();
        async move {
            publisher
                .publish_batch("t", "ns", "s", vec![b"one".to_vec()], AckMode::PerBatch)
                .await
        }
    });

    match rx.recv().await.expect("request") {
        PublishRequest::Message {
            message, response, ..
        } => {
            match message {
                Message::PublishBatch { key, payloads, .. } => {
                    assert!(key.is_none(), "the unkeyed fallback must not invent a key");
                    assert_eq!(payloads, vec![b"one".to_vec()]);
                }
                other => panic!("expected a JSON publish_batch, got {other:?}"),
            }
            let _ = response.send(Ok(Acked::default()));
        }
        _ => panic!("a broker without the acked binary frame must get JSON"),
    }
    publish.await.expect("task").expect("publish");
}

/// An idempotent batch goes out as a binary frame when the broker
/// advertised the bit, and as `publish_idempotent` when it did not.
#[tokio::test]
async fn an_idempotent_batch_is_binary_only_when_advertised() {
    for (server_flags, binary) in [
        (felix_wire::KNOWN_FLAGS, true),
        (
            felix_wire::KNOWN_FLAGS & !felix_wire::FLAG_BINARY_PUBLISH_IDEMPOTENT,
            false,
        ),
    ] {
        let (tx, mut rx) = mpsc::channel::<PublishRequest>(2);
        let publisher = Publisher {
            inner: Arc::new(PublisherInner::new(
                Arc::new(vec![PublishWorker {
                    tx,
                    handle: tokio::sync::Mutex::new(None),
                    request_counter: AtomicU64::new(1),
                    server_flags,
                    publish_window: 0,
                }]),
                PublishSharding::RoundRobin,
            )),
        };
        let publish = tokio::spawn({
            let publisher = publisher.clone();
            async move {
                publisher
                    .publish_idempotent_batch("t", "ns", "s", vec![b"one".to_vec()], 42, 7)
                    .await
            }
        });
        match rx.recv().await.expect("request") {
            PublishRequest::BinaryBytes {
                bytes, response, ..
            } => {
                assert!(binary, "sent a binary frame the broker never advertised");
                let frame = felix_wire::Frame::decode(bytes).expect("frame");
                let decoded =
                    felix_wire::binary::decode_acked_publish_batch(&frame).expect("decode");
                assert_eq!(
                    decoded.producer,
                    Some(felix_wire::binary::ProducerSequence {
                        producer_id: 42,
                        sequence: 7,
                    })
                );
                let _ = response.send(Ok(Acked::default()));
            }
            PublishRequest::Message {
                message, response, ..
            } => {
                assert!(
                    !binary,
                    "fell back to JSON against a broker that has the bit"
                );
                assert!(matches!(
                    message,
                    Message::PublishIdempotent {
                        producer_id: 42,
                        sequence: 7,
                        ..
                    }
                ));
                let _ = response.send(Ok(Acked::default()));
            }
            PublishRequest::Finish { .. } => panic!("unexpected finish"),
        }
        publish.await.expect("task").expect("publish");
    }
}

#[tokio::test]
async fn unacked_publish_defaults_to_binary_and_json_is_explicit() {
    let (tx, mut rx) = mpsc::channel::<PublishRequest>(2);
    let publisher = Publisher {
        inner: Arc::new(PublisherInner::new(
            Arc::new(vec![PublishWorker {
                tx,
                handle: tokio::sync::Mutex::new(None),
                request_counter: AtomicU64::new(1),
                server_flags: felix_wire::KNOWN_FLAGS,
                publish_window: 0,
            }]),
            PublishSharding::RoundRobin,
        )),
    };

    let binary_publish = tokio::spawn({
        let publisher = publisher.clone();
        async move {
            publisher
                .publish("t", "ns", "s", b"binary".to_vec(), AckMode::None)
                .await
        }
    });
    let request = rx.recv().await.expect("binary request");
    match request {
        PublishRequest::BinaryBytes {
            bytes,
            item_count,
            response,
            ..
        } => {
            let frame = felix_wire::Frame::decode(bytes).expect("binary frame");
            let batch = felix_wire::binary::decode_publish_batch(&frame).expect("binary batch");
            assert_eq!(item_count, 1);
            assert_eq!(batch.payloads.len(), 1);
            assert_eq!(batch.payloads[0], b"binary".as_slice());
            let _ = response.send(Ok(Acked::default()));
        }
        _ => panic!("unacked publish should use binary encoding"),
    }
    binary_publish
        .await
        .expect("binary task")
        .expect("binary publish");

    // Deliberately the JSON encoding: this asserts the compatibility arm
    // still produces a Message::Publish, which is what a broker predating
    // the binary frames gets.
    #[allow(deprecated)]
    let json_publish = tokio::spawn({
        let publisher = publisher.clone();
        async move {
            publisher
                .publish_json("t", "ns", "s", b"json".to_vec(), AckMode::None)
                .await
        }
    });
    let request = rx.recv().await.expect("json request");
    match request {
        PublishRequest::Message {
            message, response, ..
        } => {
            assert!(matches!(message, Message::Publish { .. }));
            let _ = response.send(Ok(Acked::default()));
        }
        _ => panic!("explicit JSON publish should use message encoding"),
    }
    json_publish
        .await
        .expect("json task")
        .expect("json publish");
}

#[tokio::test]
#[cfg(feature = "telemetry")]
async fn publish_batch_binary_appends_bench_ts_when_enabled() -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<PublishRequest>(1);
    let handle = tokio::spawn(async move {
        if let Some(PublishRequest::BinaryBytes {
            bytes, response, ..
        }) = rx.recv().await
        {
            let frame = felix_wire::Frame::decode(bytes).context("decode frame")?;
            let decoded =
                felix_wire::binary::decode_publish_batch(&frame).context("decode publish batch")?;
            assert_eq!(decoded.payloads.len(), 1);
            assert!(decoded.payloads[0].len() > 1);
            let _ = response.send(Ok(Acked::default()));
        }
        Ok(())
    });

    let publisher = Publisher {
        inner: Arc::new(PublisherInner::with_runtime_config(
            Arc::new(vec![PublishWorker {
                tx,
                handle: tokio::sync::Mutex::new(Some(handle)),
                request_counter: AtomicU64::new(1),
                server_flags: felix_wire::KNOWN_FLAGS,
                publish_window: 0,
            }]),
            PublishSharding::RoundRobin,
            Arc::new(PublishAdmission::new(
                crate::config::DEFAULT_PUBLISH_INFLIGHT_BYTES,
            )),
            ahash::RandomState::new(),
            None,
            true,
        )),
    };
    publisher
        .publish_batch_binary("t", "ns", "s", &[b"x".to_vec()])
        .await
        .expect("publish batch binary");
    publisher.finish().await.expect("finish");
    Ok(())
}
