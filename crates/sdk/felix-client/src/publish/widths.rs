//! How many shards each stream has, so a keyed publish can name its shard.
//!
//! A publish goes on its shard's own stream only when the client knows the
//! shard, and for a keyed publish that takes the stream's width and mapping.
//! Each is asked of the broker once per stream and kept for the client's
//! life, failures included. What is kept decides which writer a key's
//! publishes go to, so it must never change while the client lives: an
//! answer that changed would move a shard to a second writer, and its
//! publishes could reach the broker out of order. A stream whose width could
//! not be learned is kept as one shard, which puts all of its keyed publishes
//! on one writer, the same as a client that knew nothing of shards.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use ahash::RandomState;
use anyhow::Result;
use felix_wire::routing::ShardRouting;
use hashbrown::HashMap;
use tokio::sync::OnceCell;

use super::routing::{StreamKey, StreamKeyRef};

/// Asks the broker for a stream's width and mapping
/// ([`crate::Client::stream_routing`]).
pub(crate) type LearnWidth = Arc<
    dyn Fn(
            String,
            String,
            String,
        ) -> Pin<Box<dyn Future<Output = Result<(u32, ShardRouting)>> + Send>>
        + Send
        + Sync,
>;

/// A stream's shard count and mapping, set once.
type Width = Arc<OnceCell<(u32, ShardRouting)>>;

/// The widths of the streams one client has made keyed publishes to.
///
/// Not bounded: an entry is a stream name, and evicting one would let it be
/// learned again with a different answer.
pub(crate) struct StreamWidths {
    learn: LearnWidth,
    known: RwLock<HashMap<StreamKey, Width, RandomState>>,
}

impl StreamWidths {
    pub(crate) fn new(learn: LearnWidth) -> Self {
        Self {
            learn,
            known: RwLock::new(HashMap::with_hasher(RandomState::new())),
        }
    }

    /// The shard the broker routes `key` to on this stream.
    pub(crate) async fn shard_of(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        key: &[u8],
    ) -> u32 {
        let cell = self.cell(tenant_id, namespace, stream);
        // Concurrent first publishes share one question, and whichever answer
        // lands first is the one every later publish uses.
        let (shards, routing) = *cell
            .get_or_init(|| async {
                match (self.learn)(
                    tenant_id.to_string(),
                    namespace.to_string(),
                    stream.to_string(),
                )
                .await
                {
                    Ok((shards, routing)) if shards > 0 => (shards, routing),
                    Ok(_) => (1, ShardRouting::default()),
                    Err(err) => {
                        tracing::debug!(
                            stream,
                            error = %err,
                            "could not learn the stream's width; its keyed publishes share one writer",
                        );
                        (1, ShardRouting::default())
                    }
                }
            })
            .await;
        felix_wire::routing::shard_for_routing(routing, shards, Some(key))
    }

    fn cell(&self, tenant_id: &str, namespace: &str, stream: &str) -> Width {
        let key = StreamKeyRef::new(tenant_id, namespace, stream);
        if let Some(cell) = self.known.read().expect("stream widths").get(&key) {
            return Arc::clone(cell);
        }
        let mut known = self.known.write().expect("stream widths");
        Arc::clone(
            known
                .entry(StreamKey::new(tenant_id, namespace, stream))
                .or_default(),
        )
    }
}
