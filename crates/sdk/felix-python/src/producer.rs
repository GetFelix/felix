//! The idempotent producer both surfaces share.
//!
//! A call is handed to the producer's task under a lock and then awaited
//! without it, so a close never waits on a leader's answer and a cancelled
//! call (a dropped future, an abandoned asyncio task) leaves the batch to be
//! sent anyway, which is what keeps its sequence from pointing at a batch
//! nobody knows the fate of.

use std::time::Duration;

use bytes::Bytes;
use felix_client::IdempotentProducer;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use tokio::sync::Mutex;

/// The producer and the id it was given, `None` in the slot once closed.
pub(crate) struct Core {
    slot: Mutex<Option<IdempotentProducer>>,
    /// Held for a whole close, so a second close waits for the drain.
    closing: Mutex<()>,
    id: u64,
}

fn closed() -> anyhow::Error {
    anyhow::anyhow!("the producer is closed")
}

impl Core {
    pub(crate) fn new(producer: IdempotentProducer) -> Self {
        Self {
            id: producer.producer_id(),
            slot: Mutex::new(Some(producer)),
            closing: Mutex::new(()),
        }
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// Waits for the lock, which a call parked on a full queue holds, so the
    /// asyncio surface awaits this rather than blocking its event loop.
    pub(crate) async fn id_for(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> anyhow::Result<u64> {
        let slot = self.slot.lock().await;
        Ok(slot
            .as_ref()
            .ok_or_else(closed)?
            .producer_id_for(tenant_id, namespace, stream))
    }

    pub(crate) async fn is_closed(&self) -> bool {
        self.slot.lock().await.is_none()
    }

    pub(crate) async fn publish_batch(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        key: Option<Bytes>,
        payloads: Vec<Vec<u8>>,
    ) -> anyhow::Result<Option<u64>> {
        let handed = {
            let slot = self.slot.lock().await;
            let producer = slot.as_ref().ok_or_else(closed)?;
            producer
                .hand_off_batch(tenant_id, namespace, stream, key, payloads)
                .await?
        };
        handed.offset().await
    }

    pub(crate) async fn publish_batches(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        batches: Vec<Vec<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        let handed = {
            let slot = self.slot.lock().await;
            let producer = slot.as_ref().ok_or_else(closed)?;
            producer
                .hand_off_batches(tenant_id, namespace, stream, batches)
                .await?
        };
        handed.done().await
    }

    pub(crate) async fn abandon_in_doubt(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> anyhow::Result<()> {
        let handed = {
            let slot = self.slot.lock().await;
            let producer = slot.as_ref().ok_or_else(closed)?;
            producer
                .hand_off_abandon(tenant_id, namespace, stream)
                .await?
        };
        handed.done().await
    }

    /// Wait for what was handed over, then stop. A second close does nothing.
    ///
    /// A close that finds another one running waits for it to finish.
    ///
    /// `timeout` also bounds the wait for the lock, which a call parked on a
    /// full queue holds during an outage. If that wait runs out the producer
    /// is still open and the error says so.
    pub(crate) async fn close(&self, timeout: Option<Duration>) -> anyhow::Result<()> {
        let _closing = self.closing.lock().await;
        let started = std::time::Instant::now();
        let taken = match timeout {
            Some(timeout) => tokio::time::timeout(timeout, self.slot.lock())
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "close timed out waiting behind a queued publish; the producer is still open"
                    )
                })?
                .take(),
            None => self.slot.lock().await.take(),
        };
        let Some(producer) = taken else {
            return Ok(());
        };
        match timeout {
            Some(timeout) => {
                producer
                    .close_within(timeout.saturating_sub(started.elapsed()))
                    .await
            }
            None => producer.close().await,
        }
    }
}

/// Every batch of a `publish_batches` call needs a payload, and the call a batch.
pub(crate) fn check_batches(batches: &[Vec<Vec<u8>>]) -> PyResult<()> {
    if batches.is_empty() || batches.iter().any(Vec::is_empty) {
        return Err(PyValueError::new_err(
            "publish_batches needs at least one batch, and each batch at least one payload",
        ));
    }
    Ok(())
}

/// A batch needs a payload.
pub(crate) fn check_payloads(payloads: &[Vec<u8>]) -> PyResult<()> {
    if payloads.is_empty() {
        return Err(PyValueError::new_err("a batch needs at least one payload"));
    }
    Ok(())
}

/// Seconds to a duration, or a `ValueError` for one that is not usable.
pub(crate) fn timeout_of(seconds: Option<f64>) -> PyResult<Option<Duration>> {
    match seconds {
        None => Ok(None),
        Some(s) if s.is_finite() && (0.0..=365.0 * 24.0 * 3600.0).contains(&s) => {
            Ok(Some(Duration::from_secs_f64(s)))
        }
        Some(s) => Err(PyValueError::new_err(format!(
            "timeout must be a number of seconds, at least 0, not {s}"
        ))),
    }
}
