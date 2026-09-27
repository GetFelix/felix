//! Resolving a stream shard to its live state, opening it on first use.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::Broker;
use super::keys::{StreamKeyRef, TopicKey, TopicKeyRef};
use super::metadata::{ConsistencyLevel, StreamMetadata};
use crate::durable::StreamLog;
use crate::error::{BrokerError, Result};
use crate::stream::StreamState;

/// Byte ceiling on the replay ring refill at startup.
///
/// The record count is already capped by the ring's capacity; this bounds the
/// payload bytes a single pathological stream can pull in while a broker is
/// starting, so one stream of very large records cannot stall registration of
/// every other stream behind it.
const HYDRATE_MAX_BYTES: usize = 64 * 1024 * 1024;

impl Broker {
    /// Resolve a stream shard to a handle, opening the shard if this broker
    /// has not served it before.
    pub async fn resolve_stream_handle(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<StreamHandle> {
        let state = self
            .get_stream_state(tenant_id, namespace, stream, shard)
            .await?;
        if !state.active.load(Ordering::Acquire) {
            return Err(BrokerError::StreamHandleInactive(state.handle_id));
        }
        Ok(StreamHandle { state })
    }

    /// End every subscription this broker serves on one stream shard.
    ///
    /// Each subscriber still receives what was already queued for it; its feed
    /// then ends, which is how a client learns to resubscribe elsewhere once
    /// the shard has moved. With a `handoff`, each feed also records where the
    /// shard went and the exact offset to resume from, which
    /// [`crate::SubscriptionReceiver::moved`] reports once the feed is drained.
    /// Returns how many ended.
    pub async fn end_subscriptions(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        handoff: Option<crate::ShardHandoff>,
    ) -> usize {
        self.topics
            .read()
            .await
            .get(&TopicKeyRef::new(tenant_id, namespace, stream, shard))
            .map_or(0, |state| state.end_subscribers(handoff))
    }

    /// Drop the batches one stream shard holds back from readers, and its
    /// replay ring. For a shard this broker has stopped leading: what it
    /// held was never committed under its leadership. Returns how many
    /// batches were dropped.
    pub async fn discard_uncommitted(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> usize {
        self.topics
            .read()
            .await
            .get(&TopicKeyRef::new(tenant_id, namespace, stream, shard))
            .map_or(0, |state| state.discard_held())
    }

    pub(super) async fn get_stream_state(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> std::result::Result<Arc<StreamState>, BrokerError> {
        #[cfg(feature = "perf_debug")]
        let lock_wait_start = std::time::Instant::now();
        let found = {
            let guard = self.topics.read().await;
            #[cfg(feature = "perf_debug")]
            {
                let wait_ns = lock_wait_start.elapsed().as_nanos() as u64;
                metrics::histogram!("felix_perf_topics_read_lock_wait_ns").record(wait_ns as f64);
            }
            guard
                .get(&TopicKeyRef::new(tenant_id, namespace, stream, shard))
                .cloned()
        };
        if let Some(state) = found {
            return Ok(state);
        }
        // A shard this broker has not opened yet. Registration opens shard 0;
        // any other arrives here the first time the broker is asked to serve
        // it, because ownership is decided by the control plane long after the
        // stream was registered and a broker may hold any subset.
        self.open_stream_shard(tenant_id, namespace, stream, shard)
            .await
    }

    /// Build, hydrate and install the state for one shard.
    ///
    /// Hydration happens **before** the state is visible, for the same reason
    /// registration does it in that order: a durable shard that is publishable
    /// with an empty replay ring answers `CursorTooOld` for every pre-existing
    /// cursor, and nothing upstream would know it was never initialised.
    async fn open_stream_shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> std::result::Result<Arc<StreamState>, BrokerError> {
        let stream_key = StreamKeyRef::new(tenant_id, namespace, stream);
        let metadata = self.streams.read().await.get(&stream_key).cloned().ok_or(
            BrokerError::StreamNotFound {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            },
        )?;
        if shard >= metadata.shards.max(1) {
            // Asking for a shard the stream does not have is a routing bug, and
            // opening a log for it would create a directory nothing will ever
            // read.
            return Err(BrokerError::StreamNotFound {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: format!("{stream} (shard {shard} of {})", metadata.shards),
            });
        }

        let topic = TopicKey::new(tenant_id, namespace, stream, shard);
        let durable = self
            .open_durable_log(tenant_id, namespace, stream, shard, &metadata)
            .await?;
        let handle_id = self.next_stream_handle.fetch_add(1, Ordering::Relaxed);
        let state = Arc::new(StreamState::new(
            handle_id,
            self.topic_capacity,
            self.subscriber_queue_policy,
            durable,
            metadata.consistency,
        ));
        self.bind_reads(&state, &topic);
        self.hydrate_durable_stream(&state).await?;

        let mut topics = self.topics.write().await;
        // No retry loop: a caller that lost the race takes the winner's state
        // rather than building another, so there is nothing to go round for.
        if let Some(existing) = topics.get(&topic) {
            return Ok(Arc::clone(existing));
        }
        // `close_stream_shard` closed the log while this was hydrating, and
        // clears `topics` only after that, so it may already have passed.
        // Installing this would leave the shard unwritable until the next
        // close; the caller retries into a fresh open instead.
        if state.durable.as_ref().is_some_and(StreamLog::is_closed) {
            return Err(BrokerError::Storage(format!(
                "stream {stream} shard {shard} was closed while opening"
            )));
        }
        topics.insert(topic, Arc::clone(&state));
        Ok(state)
    }

    /// Let go of everything this broker holds open for a stream shard it no
    /// longer has any role in: its logs (records, group cursors, dead
    /// letters) and its in-memory state.
    ///
    /// Anything still holding a handle fails with a storage error from here
    /// on; the next request for the shard opens it afresh from disk. Readers
    /// still attached are ended. Every part is attempted, and the first
    /// failure is returned.
    pub async fn close_stream_shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<()> {
        // Logs before `topics`: an open racing this either installs its state
        // before the removal below, or sees its log closed and backs off.
        let mut closed = Ok(());
        if let Some(storage) = &self.durable_storage {
            closed = closed.and(
                storage
                    .close_stream(tenant_id, namespace, stream, shard)
                    .await,
            );
        }
        if let Some(groups) = &self.consumer_groups {
            closed = closed.and(
                groups
                    .close_shard(tenant_id, namespace, stream, shard)
                    .await,
            );
        }
        if let Some(reader) = &self.group_reader {
            closed = closed.and(
                reader
                    .dead_letters()
                    .close_shard(tenant_id, namespace, stream, shard)
                    .await,
            );
            reader
                .reset_shard(tenant_id, namespace, stream, shard)
                .await;
        }
        let removed = self
            .topics
            .write()
            .await
            .remove(&TopicKeyRef::new(tenant_id, namespace, stream, shard));
        if let Some(state) = removed {
            state.deactivate();
            state.end_subscribers(None);
        }
        closed
    }

    /// [`Self::close_stream_shard`] for a cache shard: its log and the
    /// counters that ride its replica set.
    pub async fn close_cache_shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> Result<()> {
        let mut closed = self
            .cache
            .close_shard(tenant_id, namespace, cache, shard)
            .await
            .map_err(|err| BrokerError::Storage(err.to_string()));
        if let Some(counters) = &self.counters {
            closed = closed.and(
                counters
                    .close_shard(tenant_id, namespace, cache, shard)
                    .await
                    .map_err(|err| BrokerError::Storage(err.to_string())),
            );
        }
        closed
    }

    /// Open the disk log for a stream, or `None` when it is not durable.
    pub(super) async fn open_durable_log(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        metadata: &StreamMetadata,
    ) -> Result<Option<StreamLog>> {
        if !metadata.durable {
            return Ok(None);
        }
        let Some(storage) = &self.durable_storage else {
            return Err(BrokerError::DurableStorageNotConfigured {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            });
        };
        // One log per shard. Shard 0 keeps the directory a single-shard stream
        // has always had, so nothing needs migrating; every other shard gets its
        // own, which is what the replication driver has always opened.
        Ok(Some(
            storage.open_stream(tenant_id, namespace, stream, shard)?,
        ))
    }

    /// Refill a durable stream's replay ring from disk before it is published.
    ///
    /// A durable stream that recovered records keeps counting cursors from
    /// where the log left off, so a subscriber's pre-restart cursor still
    /// resolves to the same records.
    ///
    /// The ring must end at the tail. A single read is bounded by the storage
    /// layer's per-read record and byte caps, so asking for the ring's worth of
    /// history can come back short — and keeping that *earliest* prefix while
    /// still advancing `next_seq` to the real tail leaves a hole. A cursor
    /// pointing into the hole is then above the ring's oldest entry, so it is
    /// accepted rather than rejected, and replay answers with silence: the
    /// subscriber reads "you are caught up" while records are missing. Losing
    /// history loudly is fine; losing it quietly is not.
    ///
    /// So this pages forward to the tail and keeps the newest `log_capacity`
    /// records, dropping older ones as it goes. Memory stays bounded by the
    /// ring, and whatever the ring ends up holding is contiguous with
    /// `next_seq`. If the budget cannot cover even part of the window the ring
    /// is left empty, and every pre-restart cursor gets `CursorTooOld` — the
    /// honest answer.
    pub(super) async fn hydrate_durable_stream(&self, state: &Arc<StreamState>) -> Result<()> {
        let Some(log) = &state.durable else {
            return Ok(());
        };
        let tail = log.tail_offset().await?;
        let capacity = self.log_capacity;
        // A `Quorum` ring holds only committed records, and a tail past the
        // commit offset may be a dead generation's that a new leader replaces.
        // A ring stopping at the commit offset would leave a hole before the
        // tail, so it starts empty and readers are served from disk.
        if state.consistency() == ConsistencyLevel::Quorum && log.commit_offset() < tail {
            state.hydrate(Vec::new(), tail, capacity);
            return Ok(());
        }
        let mut cursor = tail.saturating_sub(capacity as u64);
        let mut window: VecDeque<felix_storage::log::LogRecord> = VecDeque::new();
        let mut bytes = 0usize;

        while cursor < tail {
            let page = log.read_from(cursor, HYDRATE_MAX_BYTES).await?;
            let Some(last) = page.last() else {
                // The tail moved out from under the read, or the range was
                // trimmed. Stop with what is in hand rather than spinning.
                break;
            };
            cursor = last.offset + 1;
            for record in page {
                bytes += record.payload.len();
                window.push_back(record);
                // Keep the *newest* end of the window under both bounds.
                while window.len() > capacity || (bytes > HYDRATE_MAX_BYTES && window.len() > 1) {
                    if let Some(dropped) = window.pop_front() {
                        bytes -= dropped.payload.len();
                    }
                }
            }
        }

        // Only hand over a window that actually reaches the tail. Anything else
        // would reintroduce the hole this method exists to avoid.
        let reaches_tail = window.back().is_some_and(|last| last.offset + 1 == tail);
        let contiguous = if reaches_tail {
            window.into_iter().collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        state.hydrate(contiguous, tail, capacity);
        Ok(())
    }
}

/// A resolved stream shard, held by the publish path so it does not look the
/// stream up again.
#[derive(Clone, Debug)]
pub struct StreamHandle {
    pub(super) state: Arc<StreamState>,
}

impl StreamHandle {
    /// What an acknowledgement of a publish through this handle means.
    pub fn consistency(&self) -> ConsistencyLevel {
        self.state.consistency()
    }

    /// Whether a publish through this handle waits on a device flush.
    ///
    /// The transport uses it to decide which publishes are worth splitting into
    /// a claim and a completion: only a durable stream has a flush to overlap.
    pub fn is_durable(&self) -> bool {
        self.state.durable.is_some()
    }

    pub fn id(&self) -> u64 {
        self.state.handle_id
    }

    pub fn is_active(&self) -> bool {
        self.state.active.load(Ordering::Acquire)
    }

    /// The shard's disk log, for a durable stream.
    ///
    /// For readers that address records by offset. Everything
    /// [`StreamLog::read_from`] returns is at or below the log's tail, which is
    /// the same bound a resumed subscription replays to.
    pub fn log(&self) -> Option<&StreamLog> {
        self.state.durable.as_ref()
    }

    /// Notified after every durable publish to this shard commits.
    ///
    /// To wait for new records without missing one, create the
    /// `notified_owned()` future and `enable()` it *before* reading the tail.
    pub fn appended(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.state.appended)
    }

    /// How far readers of this shard may see; see [`crate::ReadBound`].
    pub fn read_bound(&self) -> crate::ReadBound {
        self.state.read_bound()
    }

    /// Look at the committed mark again and release the held batches it now
    /// covers. For whoever moves the mark; cheap when nothing is held.
    pub fn release_committed(&self) {
        if self.state.held.kick() {
            super::publish::spawn_release(self);
        }
    }
}
