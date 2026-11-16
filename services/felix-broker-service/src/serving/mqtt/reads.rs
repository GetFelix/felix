//! How an MQTT subscription reads a mounted stream: committed records of a
//! shard by offset, the same bound a QUIC subscriber's history is read under.
//!
//! The bound is the shard's read bound, which the engine sets for every shard
//! it runs: the commit mark on a `Quorum` stream, and on a Raft shard the
//! commit index (`docs/per-shard-raft.md`). A record past it is never read, so
//! a failover cannot take back a message a device was sent. A shard this
//! broker cannot say the bound of is `Unavailable` and read again. Reading a
//! shard another broker owns is #1693.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use felix_broker::{Broker, BrokerError};
use felix_mqtt::{ReadError, ReadPage, Reads, StoredMessage};

use crate::shards::ShardKind;
use crate::shards::routing::IngressRouter;

/// Most records one read returns.
const READ_RECORDS: usize = 1024;

/// [`Reads`] for this broker.
pub struct BrokerReads {
    broker: Arc<Broker>,
    ingress: Option<Arc<IngressRouter>>,
}

impl BrokerReads {
    /// `ingress` is the cluster view; without it the broker is a single node
    /// and every stream has one shard, as [`super::BrokerMounts`] assumes.
    pub fn new(broker: Arc<Broker>, ingress: Option<Arc<IngressRouter>>) -> Self {
        Self { broker, ingress }
    }
}

fn read_error(err: &BrokerError) -> ReadError {
    match err {
        BrokerError::CursorTooOld { oldest, .. } => ReadError::TooOld { oldest: *oldest },
        BrokerError::StreamNotDurable { .. } => ReadError::Gone,
        _ => ReadError::Unavailable,
    }
}

#[async_trait]
impl Reads for BrokerReads {
    fn shard_count(&self, tenant_id: &str, namespace: &str, stream: &str) -> u32 {
        self.ingress
            .as_ref()
            .and_then(|ingress| {
                ingress.placed_shards_for(ShardKind::Stream, tenant_id, namespace, stream)
            })
            .unwrap_or(1)
            .max(1)
    }

    async fn tail(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<u64, ReadError> {
        self.broker
            .cursor_tail(tenant_id, namespace, stream, shard)
            .await
            .map(|cursor| cursor.next_seq())
            .map_err(|err| read_error(&err))
    }

    async fn read(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        from: u64,
        max_bytes: usize,
        wait: Duration,
    ) -> Result<ReadPage, ReadError> {
        let handle = self
            .broker
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await
            .map_err(|err| read_error(&err))?;
        let appended = handle.appended();
        // Registered before the read, so an append or a commit that lands
        // after it wakes the wait below.
        let moved = appended.notified();
        tokio::pin!(moved);
        moved.as_mut().enable();
        let mut page = self
            .page(tenant_id, namespace, stream, shard, from, max_bytes)
            .await?;
        if page.messages.is_empty() && page.next == from {
            let _ = tokio::time::timeout(wait, moved).await;
            page = self
                .page(tenant_id, namespace, stream, shard, from, max_bytes)
                .await?;
        }
        Ok(page)
    }
}

impl BrokerReads {
    async fn page(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        from: u64,
        max_bytes: usize,
    ) -> Result<ReadPage, ReadError> {
        let page = self
            .broker
            .read_range(
                tenant_id,
                namespace,
                stream,
                shard,
                from,
                None,
                READ_RECORDS,
                max_bytes,
            )
            .await
            .map_err(|err| read_error(&err))?;
        Ok(ReadPage {
            messages: page
                .records
                .into_iter()
                .map(|record| StoredMessage {
                    offset: record.offset,
                    payload: record.payload,
                    headers: record.headers.map(|headers| *headers),
                })
                .collect(),
            next: page.next_offset,
        })
    }
}

#[cfg(test)]
mod tests;
