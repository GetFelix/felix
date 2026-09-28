//! Opening cache watches through a [`Client`].

use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_wire::Message;
use tokio::sync::oneshot;

use super::Client;
use crate::cache::{CacheWatch, CacheWatchFilter, filter_fields};
use crate::connection::{EventRouterCommand, OpenedStream};
use crate::frame_io::{read_message_with_limit, write_message};
use crate::{NotLeaderError, SubscribeCursorError};

impl Client {
    /// Watch a cache key or key prefix for changes.
    ///
    /// The watch delivers each applied write — a put with its value, a delete
    /// as a change with none — in the cache shard's write order, each carrying
    /// the cache-log offset that is its resume anchor. `from_offset: None`
    /// watches from now; `Some(n)` resumes at the first change not yet seen,
    /// replaying `[n, tail)` from the log first. A resume whose history
    /// compaction has collapsed begins with each matching key's current value
    /// instead, and says so via [`CacheWatch::resnapshot`] — defined and loud,
    /// never a silent gap. A watch that falls behind is ended with
    /// [`CacheWatchItem::Lagged`] naming the offset to re-watch from.
    ///
    /// A watch reads one shard. A key names its shard by hashing, exactly as a
    /// get does. A prefix watch reads shard 0 of a single-shard cache and is
    /// refused on a multi-shard one, since keys sharing a prefix hash apart and
    /// shard 0 alone would look complete while missing the rest: use
    /// [`Client::watch_cache_shard`] once per shard.
    ///
    /// Fails without sending anything when the broker did not advertise
    /// [`felix_wire::FEATURE_CACHE_WATCH`]: an unrecognised message type is
    /// fatal to a broker's control loop, so probing an older broker would cost
    /// the connection.
    ///
    /// [`CacheWatchItem::Lagged`]: crate::CacheWatchItem::Lagged
    pub async fn watch_cache(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        filter: CacheWatchFilter,
        from_offset: Option<u64>,
    ) -> Result<CacheWatch> {
        self.watch_cache_shard(tenant_id, namespace, cache, filter, None, from_offset)
            .await
    }

    /// Watch a key or prefix, receiving current state first: each matching
    /// key's current value — the retained message — then live changes.
    ///
    /// This is what a client joining should not have to poll for: subscribe
    /// and immediately hold the state, then stay current. The confirmation
    /// says how many retained values precede live delivery
    /// ([`CacheWatch::retained_count`]), so joining an empty key is a definite
    /// `Some(0)` rather than a silence indistinguishable from a slow key. A
    /// retained value arrives at the offset of the write that produced it;
    /// everything at [`CacheWatch::resume_offset`] or later is live.
    ///
    /// Fails without sending anything when the broker did not advertise
    /// [`felix_wire::FEATURE_CACHE_WATCH_RETAINED`]: an older watch-capable
    /// broker would ignore the request's retained field and serve a live-only
    /// watch — silently missing exactly the state the caller joined for.
    pub async fn watch_cache_retained(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        filter: CacheWatchFilter,
    ) -> Result<CacheWatch> {
        self.watch_cache_shard_retained(tenant_id, namespace, cache, filter, None)
            .await
    }

    /// [`Client::watch_cache`] against an explicit shard of the cache.
    ///
    /// The shard applies to a prefix watch; a key watch resolves its own shard
    /// by hashing and ignores this.
    pub async fn watch_cache_shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        filter: CacheWatchFilter,
        shard: Option<u32>,
        from_offset: Option<u64>,
    ) -> Result<CacheWatch> {
        self.establish_cache_watch(
            tenant_id,
            namespace,
            cache,
            filter,
            shard,
            from_offset,
            false,
        )
        .await
    }

    /// [`Client::watch_cache_retained`] against an explicit shard of the cache.
    pub async fn watch_cache_shard_retained(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        filter: CacheWatchFilter,
        shard: Option<u32>,
    ) -> Result<CacheWatch> {
        if !felix_wire::supports_feature(
            self.server_features,
            felix_wire::FEATURE_CACHE_WATCH_RETAINED,
        ) {
            return Err(anyhow::anyhow!(
                "this broker does not support retained cache watch"
            ));
        }
        self.establish_cache_watch(tenant_id, namespace, cache, filter, shard, None, true)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn establish_cache_watch(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        filter: CacheWatchFilter,
        shard: Option<u32>,
        from_offset: Option<u64>,
        retained: bool,
    ) -> Result<CacheWatch> {
        if !felix_wire::supports_feature(self.server_features, felix_wire::FEATURE_CACHE_WATCH) {
            return Err(anyhow::anyhow!("this broker does not support cache watch"));
        }
        if tenant_id != self.auth_tenant_id {
            return Err(anyhow::anyhow!(
                "tenant mismatch: client auth is scoped to {}",
                self.auth_tenant_id
            ));
        }
        // A watch is a long-lived read, so it lives on the event connections
        // beside subscriptions, placed the same way.
        let OpenedStream {
            mut send,
            mut recv,
            lease,
            ..
        } = self.open_event_stream().await?;

        let (key, prefix) = filter_fields(&filter);
        let mut frame_scratch = BytesMut::with_capacity(16 * 1024);
        write_message(
            &mut send,
            Message::CacheWatch {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                cache: cache.to_string(),
                key,
                prefix,
                shard,
                from_offset,
                retained,
                subscription_id: None,
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
        let (subscription_id, resume_offset, resnapshot, retained_count) = match response {
            Some(Message::CacheWatchStarted {
                subscription_id,
                resume_offset,
                resnapshot,
                retained_count,
            }) => (subscription_id, resume_offset, resnapshot, retained_count),
            Some(Message::SubscribeCursorError {
                reason,
                requested,
                available,
            }) => {
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
                    "cache watch refused",
                    message,
                    code,
                    retry,
                    detail,
                ));
            }
            other => return Err(anyhow::anyhow!("cache watch failed: {other:?}")),
        };

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
        Ok(CacheWatch::spawn_pump(
            recv,
            lease,
            resume_offset,
            resnapshot,
            retained_count,
            self.runtime_config.client_sub_queue_capacity.max(1),
            self.runtime_config.max_frame_bytes,
        ))
    }
}
