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
                takes_width: false,
                takes_record_headers: false,
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
        inner: Arc::new({
            let mut inner = PublisherInner::with_admission(
                Arc::new(vec![PublishWorker {
                    tx,
                    handle: tokio::sync::Mutex::new(Some(handle)),
                    request_counter: AtomicU64::new(1),
                    server_flags: felix_wire::KNOWN_FLAGS,
                    publish_window: 0,
                    takes_width: false,
                    takes_record_headers: false,
                }]),
                PublishSharding::RoundRobin,
                Arc::new(PublishAdmission::new(
                    crate::config::DEFAULT_PUBLISH_INFLIGHT_BYTES,
                )),
                ahash::RandomState::new(),
            );
            inner.bench_embed_ts = true;
            inner
        }),
    };
    publisher
        .publish_batch_binary("t", "ns", "s", &[b"x".to_vec()])
        .await
        .expect("publish batch binary");
    publisher.finish().await.expect("finish");
    Ok(())
}
