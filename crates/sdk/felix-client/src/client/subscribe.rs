//! Subscribing through a [`Client`]: one shard of one stream per subscription.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_wire::{Message, StartPosition};
use tokio::sync::oneshot;

use super::Client;
use crate::connection::{EventRouterCommand, OpenedStream};
use crate::frame_io::{read_message_with_limit, write_message};
use crate::subscribe::{Subscription, SubscriptionPipelineConfig};
use crate::{NotLeaderError, SubscribeCursorError};

impl Client {
    /// Subscribe from the live tail, delivering only what is published from now.
    pub async fn subscribe(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<Subscription> {
        self.subscribe_from(tenant_id, namespace, stream, None)
            .await
    }

    /// Subscribe from a chosen position, resuming a durable stream.
    ///
    /// `start: None` is exactly [`Client::subscribe`]. Pass
    /// `Some(StartPosition::Offset(n))` to resume at the first record not yet
    /// seen -- an application that checkpoints the offset of the last event it
    /// handled resumes at that offset plus one.
    ///
    /// Fails with a `CursorTooOld` error if retention has already discarded the
    /// requested offset, rather than silently restarting at the tail: a resume
    /// that quietly skips records is the failure mode this exists to remove.
    pub async fn subscribe_from(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        start: Option<StartPosition>,
    ) -> Result<Subscription> {
        self.subscribe_shard(tenant_id, namespace, stream, 0, start)
            .await
    }

    /// Subscribe to one shard of a stream.
    ///
    /// A subscription reads a single shard. A stream's shards can have
    /// different owners and a subscription is bound to one connection, so
    /// reading a whole multi-shard stream means one of these per shard — see
    /// #297, which is about doing that for the caller.
    ///
    /// Shard 0 is every record of a single-shard stream, which is what
    /// [`Client::subscribe`] asks for.
    pub async fn subscribe_shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        start: Option<StartPosition>,
    ) -> Result<Subscription> {
        if tenant_id != self.auth_tenant_id {
            return Err(anyhow::anyhow!(
                "tenant mismatch: client auth is scoped to {}",
                self.auth_tenant_id
            ));
        }
        // The subscription goes on whichever event connection has the most
        // room, and holds its lease for as long as it lives. The id is left to
        // the broker (`subscription_id: None`): two clients counting from 1
        // would collide in the broker's bookkeeping, and the client discards
        // event batches whose id does not match, so a collision would look
        // like events vanishing.
        let OpenedStream {
            mut send,
            mut recv,
            negotiated,
            lease,
        } = self.open_event_stream().await?;
        let connection_index = lease.slot();
        let server_flags = negotiated.server_flags;

        // A broker that predates resume ignores the unknown `start` field and
        // subscribes at the tail, then answers `Subscribed` -- so the client
        // would report success while silently losing everything published
        // during the disconnect. That is worse than an error, because the
        // application has no way to notice. `Latest` is safe to send either
        // way: it is what an old broker does anyway.
        let needs_replay = matches!(
            start,
            Some(StartPosition::Earliest) | Some(StartPosition::Offset(_))
        );
        if needs_replay && !felix_wire::supports(server_flags, felix_wire::FLAG_EVENT_BATCH_OFFSETS)
        {
            return Err(anyhow::anyhow!(
                "broker does not support resumable subscriptions (negotiated flags {server_flags:#06x}); \
                 resuming from a position would silently start at the tail instead"
            ));
        }
        let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
        write_message(
            &mut send,
            Message::Subscribe {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
                subscription_id: None,
                start,
                // Absent for shard 0, so a subscribe to a single-shard stream
                // is byte-identical to what a client sent before sharding.
                shard: (shard != 0).then_some(shard),
            },
        )
        .await?;
        send.finish()?;
        let response = read_message_with_limit(
            &mut recv,
            &mut frame_scratch,
            self.runtime_config.max_frame_bytes,
        )
        .await?;
        let (subscription_id, start_offset, live_offset) = match response {
            Some(Message::Subscribed {
                subscription_id,
                start_offset,
                live_offset,
            }) => (subscription_id, start_offset, live_offset),
            Some(Message::Ok) => {
                return Err(anyhow::anyhow!(
                    "subscribe response missing subscription id"
                ));
            }
            Some(Message::SubscribeCursorError {
                reason,
                requested,
                available,
            }) => {
                // Surfaced as a typed error rather than a debug-formatted
                // message, because the two reasons have opposite remedies and an
                // application has to be able to tell them apart in code.
                return Err(SubscribeCursorError {
                    reason,
                    requested,
                    available,
                }
                .into());
            }
            Some(Message::NotLeader {
                node_id,
                addr,
                generation,
            }) => {
                // Typed, for the same reason as the cursor error above: this is
                // not a failure but an instruction, and the only thing an
                // application can do with a formatted string is fail.
                return Err(NotLeaderError {
                    node_id,
                    addr,
                    generation,
                }
                .into());
            }
            Some(Message::Error {
                message,
                code,
                retry,
                detail,
            }) => {
                return Err(crate::error::refused(
                    "subscribe refused",
                    message,
                    code,
                    retry,
                    detail,
                ));
            }
            other => return Err(anyhow::anyhow!("subscribe failed: {other:?}")),
        };
        let tenant_id = Arc::<str>::from(tenant_id);
        let namespace = Arc::<str>::from(namespace);
        let stream = Arc::<str>::from(stream);
        let (stream_tx, stream_rx) = oneshot::channel();
        lease
            .router()
            .send(EventRouterCommand::Register {
                subscription_id,
                response: stream_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("event stream router closed"))?;
        let recv = stream_rx.await.context("event stream response dropped")??;
        let current = self.event_conn_counts[connection_index].fetch_add(1, Ordering::Relaxed) + 1;
        t_gauge!(
            "felix_client_event_conn_subscriptions",
            "conn" => connection_index.to_string()
        )
        .set(current as f64);
        t_counter!(
            "felix_client_event_conn_subscriptions_total",
            "conn" => connection_index.to_string()
        )
        .increment(1);
        Ok(Subscription::spawn_pipeline(SubscriptionPipelineConfig {
            recv,
            connection: lease.connection().clone(),
            queue_capacity: self.runtime_config.client_sub_queue_capacity.max(1),
            queue_policy: self.runtime_config.client_sub_queue_policy,
            subscription_id,
            tenant_id,
            namespace,
            stream,
            event_conn_index: connection_index,
            lease,
            event_conn_counts: Arc::clone(&self.event_conn_counts),
            max_frame_bytes: self.runtime_config.max_frame_bytes,
            live_offset,
            #[cfg(feature = "telemetry")]
            bench_embed_ts: self.runtime_config.bench_embed_ts,
        })
        .with_join(start_offset, live_offset))
    }
}
