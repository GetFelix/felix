//! Publishing through a [`ClusterClient`]: once, at least once, or
//! idempotently. See the parent module for why the first two differ.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use felix_wire::AckMode;

use super::{
    Attempt, ClusterClient, Next, Retrying, ShardKey, StreamKey, next_step, wants_reconnect,
};
use crate::client::Client;
use crate::publish::{AckOutcome, IdempotentProducer};

impl ClusterClient {
    /// Publish, reconnecting if the broker in use has gone.
    ///
    /// **A record that may have landed is not sent again.** A failure is
    /// returned to the caller with the connection already replaced, so the next
    /// publish goes to a live broker. See [`Self::publish_at_least_once`] for
    /// the other choice.
    ///
    /// One case is sent again: a cached shard owner that answers
    /// `shard_unavailable`, `draining` or `not_leader` has said it applied
    /// nothing, so the owner is forgotten and the record goes once through the
    /// entry broker, which routes by the current assignment.
    ///
    /// Returns the record's log offset. `None` when the broker acknowledged
    /// before writing it, the stream has no log, the broker predates
    /// `FLAG_BINARY_PUBLISH_ACK_OFFSET`, or `ack` is `AckMode::None`.
    ///
    /// A broker acknowledges before writing only when it owns the shard of a
    /// `Leader` stream and runs with `ack_on_commit` off; every other ack comes
    /// after the write. A publish forwarded through another broker is one of
    /// those, so the same stream can give an offset for one record and `None`
    /// for the next, depending on which broker answered. An offset is never
    /// reported before the write, and is as durable as the broker's fsync
    /// policy makes the write.
    pub async fn publish(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
        ack: AckMode,
    ) -> Result<Option<u64>> {
        // An unkeyed publish has nothing to hash, so it always resolves to
        // shard 0 -- no width lookup needed.
        let shard: ShardKey = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            0,
        );
        self.publish_to_shard(shard, payload, None, ack).await
    }

    /// Publish with a routing key, which decides the shard.
    ///
    /// Without a key every record lands on shard 0, which makes a multi-shard
    /// stream behave like a single-shard one — the shards exist and only one
    /// is ever written to. The key is what spreads records, and records
    /// sharing a key share a shard and therefore stay ordered with respect to
    /// each other.
    ///
    /// Returns the record's log offset, when there is one; see [`Self::publish`].
    pub async fn publish_keyed(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
        key: bytes::Bytes,
        ack: AckMode,
    ) -> Result<Option<u64>> {
        let stream_key: StreamKey = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        // The key decides the shard, and the shard decides the owner. Computed
        // with the same function the broker routes with, so the cache is
        // bounded by shard count rather than by distinct keys.
        let shard = self.shard_of(&stream_key, Some(key.as_ref())).await;
        let shard: ShardKey = (stream_key.0, stream_key.1, stream_key.2, shard);
        self.publish_to_shard(shard, payload, Some(key), ack).await
    }

    /// Publish, reconnecting *and sending the record again* if the broker in
    /// use has gone.
    ///
    /// **This can produce duplicates.** A publish that failed after the broker
    /// had written the record will leave it in the stream twice, and nothing in
    /// the broker can tell the two apart — only the application holds an
    /// identity that would make deduplication possible. Use it for streams
    /// whose consumers tolerate that, which is what `AtLeastOnce` means.
    ///
    /// Returns the log offset of the attempt that was acknowledged, when there
    /// is one; see [`Self::publish`].
    pub async fn publish_at_least_once(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
        ack: AckMode,
    ) -> Result<Option<u64>> {
        let started = std::time::Instant::now();
        let mut last: Option<anyhow::Error> = None;
        let mut retrying = Retrying::default();
        let mut at_least = Duration::ZERO;

        for attempt in 0..self.policy.attempts.max(1) {
            if attempt > 0 {
                let delay = self.policy.delay_before(attempt - 1).max(at_least);
                // Checked before sleeping, not after: sleeping past a deadline
                // and then reporting it wastes exactly the time the deadline
                // exists to save.
                if let Some(budget) = self.policy.deadline
                    && started.elapsed() + delay >= budget
                {
                    break;
                }
                tokio::time::sleep(delay).await;
                if let Err(err) = self.reconnect().await {
                    last = Some(err.context("no broker answered"));
                    continue;
                }
            }
            let client = self.client().await;
            match publish_once(&client, tenant_id, namespace, stream, payload.clone(), ack).await {
                Ok(acked) => return Ok(acked.offset),
                Err(err) => {
                    let attempt = Attempt {
                        resend_ambiguous: true,
                        ..Attempt::default()
                    };
                    match retrying.next(&err, attempt) {
                        Next::Fail => return Err(err.context("not retried")),
                        // Never produced: this path does not use the owner cache.
                        Next::Reroute => at_least = Duration::ZERO,
                        Next::Backoff { at_least: asked } => at_least = asked,
                    }
                    last = Some(err);
                }
            }
        }

        Err(last
            .unwrap_or_else(|| anyhow::anyhow!("publish failed"))
            .context(format!(
                "gave up after {:?} and at most {} attempts across {} endpoints",
                started.elapsed(),
                self.policy.attempts.max(1),
                self.endpoints.read().await.len()
            )))
    }

    /// A producer whose publishes land once, however many times they are
    /// sent, re-sent across reconnects like [`Self::publish_at_least_once`]
    /// and without the duplicate. See [`crate::IdempotentProducer`].
    ///
    /// The producer keeps a handle on this client, so it can be stored or
    /// moved into a task of its own.
    pub async fn idempotent_producer(self: &Arc<Self>) -> Result<IdempotentProducer> {
        let producer_id = self.client().await.producer_init().await?;
        Ok(IdempotentProducer::for_cluster(
            Arc::clone(self),
            producer_id,
        ))
    }

    /// Wait until every publish already handed to this client has been
    /// written and, if it asked for one, acknowledged, then close the publish
    /// streams.
    ///
    /// Call it before exiting after `AckMode::None` publishes, which return
    /// once queued: without it, records still queued are lost with the
    /// process. It covers every broker this client holds a connection to, and
    /// on each both the pooled streams and the per-shard streams, so
    /// publishes that went straight to a shard's owner are flushed as well as
    /// those sent through the entry broker.
    ///
    /// This ends publishing through this client, as [`Publisher::finish`]
    /// does for one broker: later publishes fail. Every broker is finished
    /// even when one fails, and the first failure is returned.
    ///
    /// [`Publisher::finish`]: crate::Publisher::finish
    pub async fn finish(&self) -> Result<()> {
        let mut first: Option<anyhow::Error> = None;
        for client in self.nodes.clients().await {
            // The shard publisher finishes the pooled streams and the shard
            // streams this client's publishes go on.
            let finished = client.publisher_handle().finish().await;
            if let Err(err) = finished {
                let err = err.context(format!("finish publishing to {}", client.dialled()));
                first.get_or_insert(err);
            }
        }
        first.map_or(Ok(()), Err)
    }

    /// One publish to one shard: through its cached owner when there is one,
    /// else through the entry broker, which forwards.
    async fn publish_to_shard(
        &self,
        shard: ShardKey,
        payload: Vec<u8>,
        key: Option<bytes::Bytes>,
        ack: AckMode,
    ) -> Result<Option<u64>> {
        let routed = self
            .owners
            .read()
            .await
            .get(&shard)
            .map(|owner| (Arc::clone(&owner.client), owner.node_id.clone()));
        let payload = match routed {
            None => payload,
            Some((owner, node_id)) => {
                // Kept only so a refusal that applied nothing can be sent on.
                let retained = payload.clone();
                let err = match publish_once_to(&owner, &shard, payload, key.clone(), ack).await {
                    Ok(acked) => {
                        if let Some(owner) = acked.forwarded_to {
                            self.remember_owner(shard, owner).await;
                        }
                        return Ok(acked.offset);
                    }
                    Err(err) => err,
                };
                let attempt = Attempt {
                    routed: true,
                    ..Attempt::default()
                };
                let reroute = next_step(&err, attempt) == Next::Reroute;
                let err = self.forget_owner(&shard, node_id, err).await;
                if !reroute {
                    return Err(err);
                }
                tracing::debug!(
                    error = %format!("{err:#}"),
                    "the cached owner applied nothing; sending through the entry broker",
                );
                retained
            }
        };

        let client = self.client().await;
        match publish_once_to(&client, &shard, payload, key, ack).await {
            Ok(acked) => {
                if let Some(owner) = acked.forwarded_to {
                    self.remember_owner(shard, owner).await;
                }
                Ok(acked.offset)
            }
            Err(err) if !wants_reconnect(&err) => Err(err),
            Err(err) => {
                // Reconnect before returning, so the caller's next publish does
                // not repeat this failure against the same dead broker.
                match self.reconnect().await {
                    Ok(()) => Err(err.context("publish failed; reconnected to another broker")),
                    Err(reconnect_err) => Err(err.context(format!(
                        "publish failed and no other broker answered: {reconnect_err:#}"
                    ))),
                }
            }
        }
    }
}

async fn publish_once(
    client: &Client,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payload: Vec<u8>,
    ack: AckMode,
) -> AckOutcome {
    let publisher = client.publisher_handle();
    publisher
        .publish_reporting_owner(tenant_id, namespace, stream, payload, ack)
        .await
}

/// One publish to `shard`, on that shard's own stream when the client has
/// room for one.
async fn publish_once_to(
    client: &Client,
    shard: &ShardKey,
    payload: Vec<u8>,
    key: Option<bytes::Bytes>,
    ack: AckMode,
) -> AckOutcome {
    let (tenant_id, namespace, stream, shard) = (&shard.0, &shard.1, &shard.2, shard.3);
    let publisher = client.publisher_handle();
    match key {
        Some(key) => {
            publisher
                .publish_keyed_reporting_owner(
                    tenant_id, namespace, stream, key, shard, payload, ack,
                )
                .await
        }
        // Unkeyed is shard 0, which is what the publisher assumes.
        None => {
            publisher
                .publish_reporting_owner(tenant_id, namespace, stream, payload, ack)
                .await
        }
    }
}
