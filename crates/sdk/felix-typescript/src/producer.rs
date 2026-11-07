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

/// A batch the producer's task has taken and not yet answered, from the
/// producer's `handOff*` methods. Read its outcome once, with `offset` or
/// `done`; dropping it does not stop the batch.
#[napi]
pub struct Handed {
    slot: Mutex<Option<felix_client::Handed>>,
}

#[napi]
impl Handed {
    /// Resolves to the log offset of the batch's first record, or `null` as
    /// `publishBatch` does, once the leader has answered. Rejects when it
    /// failed; the batch is then in doubt as for `publishBatch`. A `Handed`
    /// can be read once.
    #[napi]
    pub async fn offset(&self) -> Result<Option<BigInt>> {
        let handed = self.take().await?;
        handed
            .offset()
            .await
            .map(|offset| offset.map(BigInt::from))
            .map_err(classify)
    }

    /// Resolves once the call has been answered, rejecting when it failed. A
    /// `Handed` can be read once.
    #[napi]
    pub async fn done(&self) -> Result<()> {
        self.take().await?.done().await.map_err(classify)
    }
}

impl Handed {
    fn new(handed: felix_client::Handed) -> Self {
        Self {
            slot: Mutex::new(Some(handed)),
        }
    }

    async fn take(&self) -> Result<felix_client::Handed> {
        self.slot
            .lock()
            .await
            .take()
            .ok_or_else(|| invalid("this Handed was already read"))
    }
}

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

    /// Like `publishBatch`, but resolves as soon as the producer's task has
    /// the batch, with a `Handed` to read the answer from. The batch has then
    /// taken its place in the stream's sequence, so a caller can queue many
    /// batches without waiting on each leader's answer and still know they go
    /// out in the order they were handed over. Refusals that come before the
    /// hand-over reject here.
    #[napi]
    pub async fn hand_off_batch(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        payloads: Vec<Buffer>,
        key: Option<Buffer>,
    ) -> Result<Handed> {
        let payloads = batch_of(payloads)?;
        let key = key.map(|k| Bytes::copy_from_slice(&k));
        let slot = self.slot.lock().await;
        let producer = slot.as_ref().ok_or_else(closed)?;
        producer
            .hand_off_batch(&tenant_id, &namespace, &stream, key, payloads)
            .await
            .map(Handed::new)
            .map_err(classify)
    }

    /// `publishBatches` handed over: the batches go to shard 0 under
    /// consecutive sequences, and the `Handed` settles when the last is
    /// answered. Read it with `done`.
    #[napi]
    pub async fn hand_off_batches(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        batches: Vec<Vec<Buffer>>,
    ) -> Result<Handed> {
        if batches.is_empty() {
            return Err(invalid("handOffBatches needs at least one batch"));
        }
        let batches = batches
            .into_iter()
            .map(batch_of)
            .collect::<Result<Vec<_>>>()?;
        let slot = self.slot.lock().await;
        let producer = slot.as_ref().ok_or_else(closed)?;
        producer
            .hand_off_batches(&tenant_id, &namespace, &stream, batches)
            .await
            .map(Handed::new)
            .map_err(classify)
    }

    /// `abandonInDoubt` handed over. **A batch given up may land twice.** Read
    /// the `Handed` with `done`.
    #[napi]
    pub async fn hand_off_abandon(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
    ) -> Result<Handed> {
        let slot = self.slot.lock().await;
        let producer = slot.as_ref().ok_or_else(closed)?;
        producer
            .hand_off_abandon(&tenant_id, &namespace, &stream)
            .await
            .map(Handed::new)
            .map_err(classify)
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
