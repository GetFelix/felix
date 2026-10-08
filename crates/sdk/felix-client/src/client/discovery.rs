//! What a broker can tell a [`Client`] about the cluster: its brokers, how
//! many shards a stream or cache has, and who owns each. Each question
//! refuses to be asked of a broker that did not advertise it.

use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_wire::Message;

use super::Client;
use crate::connection::{Credentials, NodeConnections, OpenedStream};
use crate::frame_io::{read_message_with_limit, write_message};

impl Client {
    /// True if this broker answers [`Client::topology`].
    pub fn supports_topology(&self) -> bool {
        felix_wire::supports_feature(self.server_features, felix_wire::FEATURE_TOPOLOGY)
    }

    /// Whether this broker answers acknowledged publishes after the write when
    /// [`crate::ClientConfig::ack_on_commit`] asks it to.
    pub fn supports_ack_on_commit(&self) -> bool {
        felix_wire::supports_feature(self.server_features, felix_wire::FEATURE_ACK_ON_COMMIT)
    }

    /// Whether this broker answers [`Client::stream_shards`].
    pub fn supports_stream_shards(&self) -> bool {
        felix_wire::supports_feature(self.server_features, felix_wire::FEATURE_STREAM_SHARDS)
    }

    /// Whether this broker answers [`Client::cache_shards`].
    pub fn supports_cache_shards(&self) -> bool {
        felix_wire::supports_feature(self.server_features, felix_wire::FEATURE_CACHE_SHARDS)
    }

    /// Whether this broker answers [`Client::shard_owners`].
    pub fn supports_shard_owners(&self) -> bool {
        felix_wire::supports_feature(self.server_features, felix_wire::FEATURE_SHARD_OWNERS)
    }

    /// Ask the broker which brokers a client may connect to.
    ///
    /// Returns an empty list when the cluster has told this broker of no
    /// client-reachable address -- a single-node deployment, or one whose
    /// brokers do not advertise where clients reach them. That is not an error:
    /// it means discovery has nothing to add to what the caller already has.
    ///
    /// Fails rather than returning empty when the broker predates the request.
    /// Sending it anyway would be a protocol error the broker's control loop
    /// treats as fatal, so the caller has to be told the difference between "no
    /// brokers to report" and "cannot ask".
    pub async fn topology(&self) -> Result<Vec<felix_wire::BrokerEndpoint>> {
        if !self.supports_topology() {
            anyhow::bail!("broker does not support topology discovery");
        }
        // A stream of its own rather than one of the publish pool's: those are
        // pipelined, and a request/response exchange in the middle of one would
        // have to be matched against acks it has nothing to do with.
        let OpenedStream {
            mut send,
            mut recv,
            lease: _lease,
            ..
        } = self.open_event_stream().await?;
        write_message(&mut send, Message::Topology)
            .await
            .context("send topology request")?;
        let mut scratch = BytesMut::with_capacity(64 * 1024);
        let answer =
            read_message_with_limit(&mut recv, &mut scratch, self.runtime_config.max_frame_bytes)
                .await?;
        let _ = send.finish();
        match answer {
            Some(Message::TopologyView { brokers }) => Ok(brokers),
            Some(Message::Error {
                message,
                code,
                retry,
                detail,
            }) => Err(crate::error::refused(
                "topology rejected",
                message,
                code,
                retry,
                detail,
            )),
            Some(other) => Err(anyhow::anyhow!("unexpected topology response: {other:?}")),
            None => Err(anyhow::anyhow!("topology response missing")),
        }
    }

    /// How many shards a stream was placed with.
    ///
    /// A subscription reads one shard, so this is what a client needs before it
    /// can consume a whole stream. The answer comes from the broker's routing
    /// snapshot and can be stale in the way any routing answer can.
    ///
    /// `0` means this broker knows nothing of the stream — which is *not* the
    /// same as one shard, and is why it is not silently rounded up.
    ///
    /// Fails rather than guessing when the broker predates the request, for the
    /// same reason [`Client::topology`] does: sending it anyway is a protocol
    /// error the broker's control loop treats as fatal.
    pub async fn stream_shards(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<u32> {
        let (shards, _) = self.stream_routing(tenant_id, namespace, stream).await?;
        Ok(shards)
    }

    /// [`Client::stream_shards`], with how the stream maps routing keys onto
    /// its shards. A broker that does not say is describing a modulo stream,
    /// the only mapping that existed before it could.
    pub async fn stream_routing(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<(u32, felix_wire::routing::ShardRouting)> {
        if !self.supports_stream_shards() {
            anyhow::bail!("broker does not report stream shard counts");
        }
        if tenant_id != self.auth_tenant_id {
            return Err(anyhow::anyhow!(
                "tenant mismatch: client auth is scoped to {}",
                self.auth_tenant_id
            ));
        }
        let request_id = self
            .cache_request_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ask_stream_routing(
            &self.event_node,
            &self.credentials,
            request_id,
            self.runtime_config.max_frame_bytes,
            tenant_id,
            namespace,
            stream,
        )
        .await
    }

    /// How many shards a cache was placed with.
    ///
    /// A prefix watch reads one shard, so this is what a client needs before it
    /// can watch a prefix across a whole cache. `0` means this broker knows
    /// nothing of the cache. Fails without sending anything when the broker
    /// did not advertise [`felix_wire::FEATURE_CACHE_SHARDS`].
    pub async fn cache_shards(&self, tenant_id: &str, namespace: &str, cache: &str) -> Result<u32> {
        if !self.supports_cache_shards() {
            anyhow::bail!("broker does not report cache shard counts");
        }
        if tenant_id != self.auth_tenant_id {
            return Err(anyhow::anyhow!(
                "tenant mismatch: client auth is scoped to {}",
                self.auth_tenant_id
            ));
        }
        let OpenedStream {
            mut send,
            mut recv,
            lease: _lease,
            ..
        } = self.open_event_stream().await?;
        let request_id = self
            .cache_request_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        write_message(
            &mut send,
            Message::CacheShards {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                cache: cache.to_string(),
                request_id,
            },
        )
        .await
        .context("send cache shards request")?;
        let mut scratch = BytesMut::with_capacity(4 * 1024);
        let answer =
            read_message_with_limit(&mut recv, &mut scratch, self.runtime_config.max_frame_bytes)
                .await?;
        let _ = send.finish();
        match answer {
            Some(Message::CacheShardsView { shards, .. }) => Ok(shards),
            Some(Message::Error {
                message,
                code,
                retry,
                detail,
            }) => Err(crate::error::refused(
                "cache shards rejected",
                message,
                code,
                retry,
                detail,
            )),
            Some(other) => Err(anyhow::anyhow!(
                "unexpected cache shards response: {other:?}"
            )),
            None => Err(anyhow::anyhow!("cache shards response missing")),
        }
    }

    /// Which broker owns each shard of a stream or cache, as this broker's
    /// routing snapshot sees it: one [`ShardOwner`](crate::ShardOwner) per
    /// shard, in shard order.
    ///
    /// Empty means this broker knows nothing of it. A broker not in a cluster
    /// answers one shard with no node id, since it serves everything itself.
    /// A shard nobody can serve right now has no node id and says why in
    /// `unavailable`. The answer is a snapshot and can be stale the way a
    /// redirect can; a request sent on it is still routed correctly.
    ///
    /// Fails without sending anything when the broker did not advertise
    /// [`felix_wire::FEATURE_SHARD_OWNERS`].
    pub async fn shard_owners(
        &self,
        tenant_id: &str,
        namespace: &str,
        name: &str,
        kind: crate::ShardKind,
    ) -> Result<Vec<crate::ShardOwner>> {
        if !self.supports_shard_owners() {
            anyhow::bail!("broker does not report shard owners");
        }
        if tenant_id != self.auth_tenant_id {
            return Err(anyhow::anyhow!(
                "tenant mismatch: client auth is scoped to {}",
                self.auth_tenant_id
            ));
        }
        let OpenedStream {
            mut send,
            mut recv,
            lease: _lease,
            ..
        } = self.open_event_stream().await?;
        let request_id = self
            .cache_request_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        write_message(
            &mut send,
            Message::ShardOwners {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                name: name.to_string(),
                kind,
                request_id,
            },
        )
        .await
        .context("send shard owners request")?;
        let mut scratch = BytesMut::with_capacity(4 * 1024);
        let answer =
            read_message_with_limit(&mut recv, &mut scratch, self.runtime_config.max_frame_bytes)
                .await?;
        let _ = send.finish();
        match answer {
            Some(Message::ShardOwnersView { owners, .. }) => Ok(owners),
            Some(Message::Error {
                message,
                code,
                retry,
                detail,
            }) => Err(crate::error::refused(
                "shard owners rejected",
                message,
                code,
                retry,
                detail,
            )),
            Some(other) => Err(anyhow::anyhow!(
                "unexpected shard owners response: {other:?}"
            )),
            None => Err(anyhow::anyhow!("shard owners response missing")),
        }
    }
}

/// Ask a broker for a stream's width and mapping. Takes the event connections
/// rather than the client, so a publisher can ask too.
pub(super) async fn ask_stream_routing(
    event_node: &NodeConnections,
    credentials: &Credentials,
    request_id: u64,
    max_frame_bytes: usize,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
) -> Result<(u32, felix_wire::routing::ShardRouting)> {
    let OpenedStream {
        mut send,
        mut recv,
        lease: _lease,
        ..
    } = event_node.open_as(credentials).await?;
    write_message(
        &mut send,
        Message::StreamShards {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            request_id,
        },
    )
    .await
    .context("send stream shards request")?;
    let mut scratch = BytesMut::with_capacity(4 * 1024);
    let answer = read_message_with_limit(&mut recv, &mut scratch, max_frame_bytes).await?;
    let _ = send.finish();
    match answer {
        Some(Message::StreamShardsView {
            shards, routing, ..
        }) => Ok((shards, routing.unwrap_or_default())),
        Some(Message::Error {
            message,
            code,
            retry,
            detail,
        }) => Err(crate::error::refused(
            "stream shards rejected",
            message,
            code,
            retry,
            detail,
        )),
        Some(other) => Err(anyhow::anyhow!(
            "unexpected stream shards response: {other:?}"
        )),
        None => Err(anyhow::anyhow!("stream shards response missing")),
    }
}
