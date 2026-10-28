//! A producer whose publishes land once, however many times they are sent.
//!
//! A call is handed to the producer's task under a lock and then awaited
//! without it, so a close never waits on a leader's answer. An abandoned
//! promise does not stop the batch: it is still sent and answered, which is
//! what keeps its sequence from pointing at a batch nobody knows the fate of.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use felix_client::IdempotentProducer;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::sync::Mutex;

use crate::errors::{classify, invalid};

/// An idempotent producer, made by `client.producer()`.
#[napi]
pub struct Producer {
    slot: Mutex<Option<IdempotentProducer>>,
    /// Held for a whole close, so a second close waits for the drain.
    closing: Mutex<()>,
    id: u64,
    closed: AtomicBool,
}

impl Producer {
    pub(crate) fn new(producer: IdempotentProducer) -> Self {
        Self {
            id: producer.producer_id(),
            slot: Mutex::new(Some(producer)),
            closing: Mutex::new(()),
            closed: AtomicBool::new(false),
        }
    }
}

fn closed() -> Error {
    classify(anyhow::anyhow!("the producer is closed"))
}

fn batch_of(payloads: Vec<Buffer>) -> Result<Vec<Vec<u8>>> {
    if payloads.is_empty() {
        return Err(invalid("a batch needs at least one payload"));
    }
    Ok(payloads.into_iter().map(|p| p.to_vec()).collect())
}

#[napi]
impl Producer {
    /// Publish one payload, once. Resolves to its log offset, or `null` when
    /// the leader reported none. See `publishBatch`.
    #[napi]
    pub async fn publish(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        payload: Buffer,
        key: Option<Buffer>,
    ) -> Result<Option<BigInt>> {
        self.publish_batch(tenant_id, namespace, stream, vec![payload], key)
            .await
    }

    /// Publish a batch, once. The batch takes one sequence whatever its size.
    ///
    /// Resolves to the log offset of its first record, from the ack that
    /// settled it. `key` picks the shard as it does for `client.publish`;
    /// without one the batch goes to shard 0.
    ///
    /// Any rejection but `PublishRefusedError` leaves the batch in doubt: the
    /// sequence has not advanced and the batch may have landed. Call again with
    /// the same payloads and key to settle it; the leader answers a batch it
    /// holds without appending it. A different batch is rejected without being
    /// sent until then. A promise you stop waiting for does not cancel the
    /// batch, which is then in doubt too.
    #[napi]
    pub async fn publish_batch(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        payloads: Vec<Buffer>,
        key: Option<Buffer>,
    ) -> Result<Option<BigInt>> {
        let payloads = batch_of(payloads)?;
        let key = key.map(|k| Bytes::copy_from_slice(&k));
        let handed = {
            let slot = self.slot.lock().await;
            let producer = slot.as_ref().ok_or_else(closed)?;
            producer
                .hand_off_batch(&tenant_id, &namespace, &stream, key, payloads)
                .await
                .map_err(classify)?
        };
        let offset = handed.offset().await.map_err(classify)?;
        Ok(offset.map(BigInt::from))
    }

    /// Publish several batches to shard 0, once each, under consecutive
    /// sequences. On a failure the batches already acknowledged stay
    /// acknowledged and the rest are in doubt together: call again with the
    /// same batches to send only those.
    #[napi]
    pub async fn publish_batches(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        batches: Vec<Vec<Buffer>>,
    ) -> Result<()> {
        if batches.is_empty() {
            return Err(invalid("publishBatches needs at least one batch"));
        }
        let batches = batches
            .into_iter()
            .map(batch_of)
            .collect::<Result<Vec<_>>>()?;
        let handed = {
            let slot = self.slot.lock().await;
            let producer = slot.as_ref().ok_or_else(closed)?;
            producer
                .hand_off_batches(&tenant_id, &namespace, &stream, batches)
                .await
                .map_err(classify)?
        };
        handed.done().await.map_err(classify)
    }

    /// Give up on the stream's batches in doubt and number the stream from 0
    /// under a fresh producer id. **A batch given up may land twice.**
    #[napi]
    pub async fn abandon_in_doubt(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
    ) -> Result<()> {
        let handed = {
            let slot = self.slot.lock().await;
            let producer = slot.as_ref().ok_or_else(closed)?;
            producer
                .hand_off_abandon(&tenant_id, &namespace, &stream)
                .await
                .map_err(classify)?
        };
        handed.done().await.map_err(classify)
    }

    /// The id the broker assigned this producer.
    #[napi(getter)]
    pub fn id(&self) -> BigInt {
        BigInt::from(self.id)
    }

    /// The id this producer's batches to `stream` go out under: `id`, unless
    /// the stream was given up with `abandonInDoubt` or created again at
    /// another width.
    #[napi]
    pub async fn id_for(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
    ) -> Result<BigInt> {
        let slot = self.slot.lock().await;
        let producer = slot.as_ref().ok_or_else(closed)?;
        Ok(BigInt::from(
            producer.producer_id_for(&tenant_id, &namespace, &stream),
        ))
    }

    /// Wait for the batches already handed over, then stop. `timeoutMs`
    /// bounds the wait; without one it waits as long as it takes. Resolves
    /// quietly when already closed; a close that finds another running waits
    /// for it to finish. On a timeout it rejects and the producer stays
    /// closed, and a batch already sent may still land. The timeout also bounds
    /// the wait behind a publish parked on a full queue; if that wait runs out
    /// the producer is still open and the error says so.
    #[napi]
    pub async fn close(&self, timeout_ms: Option<f64>) -> Result<()> {
        let timeout = match timeout_ms {
            None => None,
            Some(ms) if ms.is_finite() && ms >= 0.0 => Some(Duration::from_secs_f64(ms / 1000.0)),
            Some(ms) => return Err(invalid(format!("timeoutMs must be at least 0, not {ms}"))),
        };
        let _closing = self.closing.lock().await;
        let started = std::time::Instant::now();
        let taken = match timeout {
            Some(timeout) => tokio::time::timeout(timeout, self.slot.lock())
                .await
                .map_err(|_| {
                    classify(anyhow::anyhow!(
                        "close timed out waiting behind a queued publish; the producer is still open"
                    ))
                })?
                .take(),
            None => self.slot.lock().await.take(),
        };
        let Some(producer) = taken else {
            return Ok(());
        };
        self.closed.store(true, Ordering::SeqCst);
        match timeout {
            Some(timeout) => {
                producer
                    .close_within(timeout.saturating_sub(started.elapsed()))
                    .await
            }
            None => producer.close().await,
        }
        .map_err(classify)
    }

    #[napi(getter)]
    pub fn closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}
