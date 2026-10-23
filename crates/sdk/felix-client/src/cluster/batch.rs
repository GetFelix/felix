//! Batched publishing through a [`ClusterClient`]: each shard's records go
//! as one ordered batch straight to that shard's owner.
//!
//! A keyed batch is routed by its shard, so a batch spanning several keys is
//! split by shard first. Each piece takes the route a single publish would
//! (`publish_to_shard`): the cached owner when one is known, else the entry
//! broker, which forwards and names the owner for next time. Pieces for
//! different shards are sent concurrently, each on its shard's own publish
//! stream while the client has room for one (`FELIX_PUB_SHARD_STREAMS`) and on
//! the shared pool past that cap, which is the isolation the per-shard
//! streams exist for.
//!
//! Owners come from the cluster's shard-owner discovery, which the first
//! publish to a stream waits for (the pieces racing it wait too, up to a
//! second), and which refreshes in the background every few seconds and soon
//! after a publish finds an owner wrong; they also come from acks that name the
//! owner of a forwarded batch. If the cluster does not answer in time the
//! batch is forwarded, as before. That holds for
//! [`AckMode::None`](felix_wire::AckMode::None) too, which gets no ack to learn
//! from. It is a routing snapshot: a write that lands while ownership is moving
//! may still be forwarded by a broker that is no longer the owner, which is
//! correct and only costs the hop.
//!
//! With `AckMode::None` a call returns once its pieces are queued. When the
//! route for a shard switches between calls (the owner moved, or discovery
//! replaced a forwarding route), two un-acked calls travel on different
//! connections and can reach the broker out of order. Await acks, or keep to
//! one call at a time, where order across calls matters.

use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use felix_wire::AckMode;

use super::{Attempt, ClusterClient, Next, Retrying, ShardKey};

/// How one shard's piece of a batch fared.
#[derive(Debug)]
pub struct ShardPublish {
    /// The shard the records resolve to.
    pub shard: u32,
    /// Positions, in the order the caller passed them, of the records in this
    /// piece. Ascending, and the order they were sent in.
    pub records: Vec<usize>,
    /// The log offset of the piece's first record when the broker reported
    /// one (see [`ClusterClient::publish`]), or why the piece was not
    /// acknowledged. A piece is all or nothing.
    pub outcome: Result<Option<u64>>,
}

/// What a batch publish did, shard by shard. Nothing is dropped silently:
/// every input record is in exactly one [`ShardPublish`].
#[derive(Debug, Default)]
pub struct BatchReport {
    /// One entry per shard, ascending by shard. After
    /// [`ClusterClient::publish_records_at_least_once`] re-splits records under
    /// a new width, one shard number can appear twice: once for the piece that
    /// was accepted earlier and once for a later one.
    pub shards: Vec<ShardPublish>,
}

impl BatchReport {
    /// Whether every piece was accepted.
    pub fn is_ok(&self) -> bool {
        self.shards.iter().all(|shard| shard.outcome.is_ok())
    }

    /// The pieces that failed.
    pub fn failed(&self) -> impl Iterator<Item = &ShardPublish> {
        self.shards.iter().filter(|shard| shard.outcome.is_err())
    }

    /// Positions of the records in failed pieces, ascending. With
    /// [`ClusterClient::publish_records`] a failed record may still have
    /// landed; see its documentation.
    pub fn failed_records(&self) -> Vec<usize> {
        let mut failed: Vec<usize> = self
            .failed()
            .flat_map(|shard| shard.records.iter().copied())
            .collect();
        failed.sort_unstable();
        failed
    }

    /// `Ok` when every piece was accepted, else the first failure with the
    /// number of records it covers.
    pub fn into_result(self) -> Result<()> {
        let mut first = None;
        let mut records = 0;
        for shard in self.shards {
            if let Err(err) = shard.outcome {
                records += shard.records.len();
                first.get_or_insert((shard.shard, err));
            }
        }
        match first {
            None => Ok(()),
            Some((shard, err)) => Err(err.context(format!(
                "batch publish failed for {records} records (first failing shard {shard})"
            ))),
        }
    }
}

impl ClusterClient {
    /// Publish an unkeyed batch. Every record resolves to shard 0, as with
    /// [`Self::publish`], so the batch goes to that shard's owner as one
    /// frame and keeps its order.
    ///
    /// Retries and failures are those of [`Self::publish`]: a batch the broker
    /// may have written is not sent again, one a cached owner refused without
    /// applying is sent once through the entry broker. Returns the first
    /// record's log offset, when there is one.
    pub async fn publish_batch(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        ack: AckMode,
    ) -> Result<Option<u64>> {
        if payloads.is_empty() {
            return Ok(None);
        }
        let shard: ShardKey = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            0,
        );
        self.publish_to_shard(shard, payloads, None, ack, None)
            .await
    }

    /// Publish a batch under one routing key, owner-direct. Records sharing a
    /// key share a shard, so the batch is one frame to that shard's owner and
    /// keeps its order. Retries and failures are those of
    /// [`Self::publish_keyed`]. Returns the first record's log offset, when
    /// there is one.
    pub async fn publish_batch_keyed(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        key: Bytes,
        payloads: Vec<Vec<u8>>,
        ack: AckMode,
    ) -> Result<Option<u64>> {
        if payloads.is_empty() {
            return Ok(None);
        }
        let (shard, width) = self
            .widths
            .shard_of(tenant_id, namespace, stream, key.as_ref())
            .await;
        let shard: ShardKey = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            shard,
        );
        let outcome = self
            .publish_to_shard(shard, payloads, Some(key), ack, Some(width))
            .await;
        self.forget_width_if_stale(tenant_id, namespace, stream, &outcome);
        outcome
    }

    /// Publish records that carry their own routing keys, splitting them by
    /// shard and sending each shard's records to its owner.
    ///
    /// **Order.** Within a shard, records are sent in the order given, as one
    /// batch that the broker appends contiguously and acknowledges as a unit.
    /// There is no order between shards. Order across two calls is the
    /// caller's: await the first before the second.
    ///
    /// **Retries.** As [`Self::publish_keyed`]: a piece the broker may have
    /// written is not sent again, so no record can arrive twice. The one
    /// re-send is of a piece a cached owner said it did not apply (it moved or
    /// is draining), which goes once through the entry broker with the
    /// shard's order intact. For re-sending ambiguous pieces too, accepting
    /// duplicates, see [`Self::publish_records_at_least_once`].
    ///
    /// **Failure is per shard.** A piece is accepted or not as a unit, and
    /// the report says which records each piece held. A failed piece does not
    /// stop the others. Under [`AckMode::None`] a piece counts as accepted
    /// once queued, so only local failures show.
    ///
    /// This does not use an idempotent producer; for deduplication across
    /// reconnects use [`Self::idempotent_producer`].
    pub async fn publish_records(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        records: Vec<(Bytes, Vec<u8>)>,
        ack: AckMode,
    ) -> BatchReport {
        let pending: Vec<usize> = (0..records.len()).collect();
        let shards = self
            .send_pieces(tenant_id, namespace, stream, &records, pending, ack)
            .await;
        BatchReport { shards }
    }

    /// [`Self::publish_records`], but a piece that failed with an outcome in
    /// doubt is sent again, after the reconnect policy's backoff, until it is
    /// accepted or the policy gives up.
    ///
    /// **This can produce duplicates**: a piece that failed after the broker
    /// wrote it exists twice, and a retried piece lands after records the
    /// caller sent later. Order within a piece is kept. A retry re-splits the
    /// pending records under the stream's current width, so a stream recreated
    /// at another width is followed rather than failed. Errors the broker marks
    /// fatal are not retried. The report holds each shard's last outcome.
    pub async fn publish_records_at_least_once(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        records: Vec<(Bytes, Vec<u8>)>,
        ack: AckMode,
    ) -> BatchReport {
        let started = Instant::now();
        let attempts = self.policy.attempts.max(1);
        let mut report = BatchReport::default();
        let mut pending: Vec<usize> = (0..records.len()).collect();
        // Failed pieces waiting for the next attempt, kept so the last
        // attempt's errors can be reported if the policy gives up.
        let mut held: Vec<ShardPublish> = Vec::new();
        let mut retrying = Retrying::default();
        // What the broker asked to wait at least, from the last attempt.
        let mut at_least = Duration::ZERO;
        for attempt in 0..attempts {
            if attempt > 0 {
                let delay = self.policy.delay_before(attempt - 1).max(at_least);
                if let Some(budget) = self.policy.deadline
                    && started.elapsed() + delay >= budget
                {
                    break;
                }
                tokio::time::sleep(delay).await;
            }
            held.clear();
            let sent = self
                .send_pieces(tenant_id, namespace, stream, &records, pending, ack)
                .await;
            pending = Vec::new();
            at_least = Duration::ZERO;
            for piece in sent {
                let again = match &piece.outcome {
                    Ok(_) => false,
                    Err(err) => {
                        let attempt = Attempt {
                            resend_ambiguous: true,
                            ..Attempt::default()
                        };
                        match retrying.next(err, attempt) {
                            Next::Fail => false,
                            Next::Reroute => true,
                            Next::Backoff { at_least: asked } => {
                                at_least = at_least.max(asked);
                                true
                            }
                        }
                    }
                };
                if again {
                    pending.extend(piece.records.iter().copied());
                    held.push(piece);
                } else {
                    report.shards.push(piece);
                }
            }
            if pending.is_empty() {
                break;
            }
            // Re-split in the caller's order.
            pending.sort_unstable();
        }
        // Out of attempts or time: what was still pending failed.
        report.shards.append(&mut held);
        report.shards.sort_by_key(|piece| piece.shard);
        report
    }

    /// Split `pending` records by shard and send each piece, concurrently.
    /// Returns every piece, ascending by shard.
    async fn send_pieces(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        records: &[(Bytes, Vec<u8>)],
        pending: Vec<usize>,
        ack: AckMode,
    ) -> Vec<ShardPublish> {
        let Some(&first) = pending.first() else {
            return Vec::new();
        };
        // One width for the whole call, so every record is split by the same
        // mapping.
        let (_, width) = self
            .widths
            .shard_of(tenant_id, namespace, stream, records[first].0.as_ref())
            .await;
        let mut pieces: Vec<(u32, Vec<usize>)> = Vec::new();
        for index in pending {
            let shard = felix_wire::routing::shard_for_routing(
                width.routing,
                width.shards,
                Some(records[index].0.as_ref()),
            );
            match pieces.iter_mut().find(|(known, _)| *known == shard) {
                Some((_, held)) => held.push(index),
                None => pieces.push((shard, vec![index])),
            }
        }
        let sends = pieces.into_iter().map(|(shard, indices)| {
            let width = std::sync::Arc::clone(&width);
            let key = records[indices[0]].0.clone();
            let payloads: Vec<Vec<u8>> = indices.iter().map(|&i| records[i].1.clone()).collect();
            Box::pin(async move {
                let target: ShardKey = (
                    tenant_id.to_string(),
                    namespace.to_string(),
                    stream.to_string(),
                    shard,
                );
                let outcome = self
                    .publish_to_shard(target, payloads, Some(key), ack, Some(width))
                    .await;
                self.forget_width_if_stale(tenant_id, namespace, stream, &outcome);
                ShardPublish {
                    shard,
                    records: indices,
                    outcome,
                }
            }) as Pin<Box<dyn Future<Output = ShardPublish> + Send + '_>>
        });
        let mut done = join_all(sends.collect()).await;
        done.sort_by_key(|piece| piece.shard);
        done
    }
}

/// Drive every future to completion on the current task.
pub(super) async fn join_all<'a, T>(
    futures: Vec<Pin<Box<dyn Future<Output = T> + Send + 'a>>>,
) -> Vec<T> {
    let mut futures: Vec<_> = futures.into_iter().map(Some).collect();
    let mut results: Vec<Option<T>> = futures.iter().map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut waiting = false;
        for (slot, result) in futures.iter_mut().zip(results.iter_mut()) {
            if let Some(future) = slot {
                match future.as_mut().poll(cx) {
                    Poll::Ready(value) => {
                        *result = Some(value);
                        *slot = None;
                    }
                    Poll::Pending => waiting = true,
                }
            }
        }
        if waiting {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await;
    results
        .into_iter()
        .map(|r| r.expect("polled to completion"))
        .collect()
}
