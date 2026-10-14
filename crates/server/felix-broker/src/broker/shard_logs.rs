//! A shard's logs as replication sees them: finding each one, and telling a
//! stream that its log grew or was rebuilt underneath it.

use std::sync::Arc;

use super::Broker;
use crate::durable::StreamLog;
use crate::error::Result;

/// Which of a shard's logs a request is about.
///
/// A stream shard has two: the records themselves, and the consumer-group
/// cursors kept beside them. Both have to reach a replica, or a promoted leader
/// serves the records and has no idea where any group had got to — it starts
/// them at the beginning and redelivers everything already finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogKind {
    /// A stream's records.
    Stream,
    /// A cache's records.
    Cache,
    /// The consumer-group cursors belonging to a stream shard.
    GroupCursors,
    /// The dead-letter list belonging to a stream shard: the offsets its
    /// groups gave up on. Beside the cursors for the same reason the cursors
    /// are beside the records — a promoted leader that serves the stream but
    /// has lost which records its groups abandoned would silently redrive
    /// nothing and list nothing.
    GroupDeadLetters,
    /// The counter log belonging to a cache shard: signed deltas folded into
    /// running sums. Rides the cache shard's replica set the way group state
    /// rides a stream shard's, so a promoted replica resumes the true sum.
    Counters,
}

impl Broker {
    /// Open no stream's logs until the stream is registered, and have a shard
    /// opened before then wait for it: once streams carry incarnations, a log
    /// opened before the catalog names one would be the wrong one. One-way,
    /// and recorded on disk, so a restart requires it from the start.
    ///
    /// Called the first time this broker sees a stream with an incarnation
    /// (registering one, replication naming one, an assignment naming one),
    /// and at start when its data shows it has before; see
    /// [`Broker::require_registration_if_in_play`].
    pub fn require_registered_streams(&self) {
        if self.registration_required() {
            return;
        }
        // The storage first: until it requires registration an unregistered
        // stream still opens at 0, and nothing may see this broker's flag set
        // before then.
        if let Some(storage) = &self.durable_storage {
            storage.require_registration();
        }
        if self
            .registration_required
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        if let Some(storage) = &self.durable_storage
            && let Err(err) = storage.provider().record_incarnations_in_play()
        {
            tracing::error!(error = %err, "could not record that streams carry incarnations");
        }
    }

    /// [`Broker::require_registered_streams`] when this broker's data shows
    /// it has seen a stream with an incarnation. Call at start, before
    /// anything can open a shard. Until then a stream's logs open as they
    /// always did, before the catalog has registered it.
    pub fn require_registration_if_in_play(&self) -> Result<()> {
        let Some(storage) = &self.durable_storage else {
            return Ok(());
        };
        if storage
            .provider()
            .incarnations_in_play()
            .map_err(crate::error::BrokerError::from)?
        {
            self.require_registered_streams();
        }
        Ok(())
    }

    /// Ask the catalog sync to poll now rather than at its next tick: a
    /// follower refused records for a stream it has not registered yet.
    pub fn request_catalog_refresh(&self) {
        self.catalog_wanted.notify_one();
    }

    /// Notified by [`Broker::request_catalog_refresh`]; the catalog sync waits
    /// on it beside its timer.
    pub fn catalog_wanted(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.catalog_wanted)
    }

    /// Whether a stream's logs open only once it is registered.
    pub fn registration_required(&self) -> bool {
        self.registration_required
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Signalled after every durable append.
    ///
    /// Replication waits on it so shipping starts when a record lands rather
    /// than on its next tick — which is most of a `Quorum` publish's latency
    /// when the tick is seconds and the shipping is milliseconds.
    pub fn appended(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.appended)
    }

    /// The log for one shard, whichever kind of thing it belongs to.
    ///
    /// The single place replication resolves a shard's log. A stream's lives in
    /// the durable-storage provider and a cache's belongs to the cache store,
    /// and getting that wrong writes a stream log for a cache — an empty
    /// directory nothing reads, while the real records go unreplicated.
    ///
    /// Not cached by the caller: a cache shard's log is replaced when the shard
    /// is closed and reopened, so a handle held across that goes stale.
    pub async fn shard_log(
        &self,
        kind: LogKind,
        tenant_id: &str,
        namespace: &str,
        name: &str,
        shard: u32,
    ) -> Option<StreamLog> {
        if !self.may_open(kind, tenant_id, namespace, name) {
            return None;
        }
        match kind {
            LogKind::Cache => self
                .cache
                .shard_log(tenant_id, namespace, name, shard)
                .await
                .map(StreamLog::from_log),
            LogKind::GroupCursors => self
                .consumer_groups
                .as_ref()?
                .shard_log(tenant_id, namespace, name, shard)
                .await
                .ok()
                .map(StreamLog::from_log),
            LogKind::GroupDeadLetters => self
                .group_reader
                .as_ref()?
                .dead_letters()
                .shard_log(tenant_id, namespace, name, shard)
                .await
                .ok()
                .map(StreamLog::from_log),
            LogKind::Counters => self
                .counters
                .as_ref()?
                .shard_log(tenant_id, namespace, name, shard)
                .await
                .ok()
                .map(StreamLog::from_log),
            LogKind::Stream => self
                .durable_storage
                .as_ref()?
                .open_stream(tenant_id, namespace, name, shard)
                .ok(),
        }
    }

    /// [`Broker::shard_log`], creating the log at `base_offset` when this broker
    /// has never held the shard. The base it comes back with is the authority.
    pub async fn shard_log_at(
        &self,
        kind: LogKind,
        tenant_id: &str,
        namespace: &str,
        name: &str,
        shard: u32,
        base_offset: u64,
    ) -> Option<StreamLog> {
        if !self.may_open(kind, tenant_id, namespace, name) {
            return None;
        }
        match kind {
            LogKind::Cache => self
                .cache
                .shard_log_at(tenant_id, namespace, name, shard, base_offset)
                .await
                .map(StreamLog::from_log),
            LogKind::GroupCursors => self
                .consumer_groups
                .as_ref()?
                .shard_log_at(tenant_id, namespace, name, shard, base_offset)
                .await
                .ok()
                .map(StreamLog::from_log),
            LogKind::GroupDeadLetters => self
                .group_reader
                .as_ref()?
                .dead_letters()
                .shard_log_at(tenant_id, namespace, name, shard, base_offset)
                .await
                .ok()
                .map(StreamLog::from_log),
            LogKind::Counters => self
                .counters
                .as_ref()?
                .shard_log_at(tenant_id, namespace, name, shard, base_offset)
                .await
                .ok()
                .map(StreamLog::from_log),
            LogKind::Stream => self
                .durable_storage
                .as_ref()?
                .open_stream_at(tenant_id, namespace, name, shard, base_offset)
                .ok(),
        }
    }

    /// [`Broker::shard_log`], or [`Broker::shard_log_at`] with a base, for a
    /// caller acting for a named incarnation of the stream: a replication
    /// request, or an assignment. With `Some`, a stream's own and group logs
    /// open only when that incarnation is the registered one, checked by the
    /// open itself, so a delete and create landing after the caller's own
    /// check cannot hand it (or create) the new stream's log. A cache's logs
    /// have no incarnation and ignore it.
    #[allow(clippy::too_many_arguments)]
    pub async fn shard_log_for(
        &self,
        kind: LogKind,
        tenant_id: &str,
        namespace: &str,
        name: &str,
        shard: u32,
        base_offset: Option<u64>,
        incarnation: Option<u64>,
    ) -> Option<StreamLog> {
        let any = || async {
            match base_offset {
                Some(base) => {
                    self.shard_log_at(kind, tenant_id, namespace, name, shard, base)
                        .await
                }
                None => {
                    self.shard_log(kind, tenant_id, namespace, name, shard)
                        .await
                }
            }
        };
        let Some(incarnation) = incarnation else {
            return any().await;
        };
        if !self.may_open(kind, tenant_id, namespace, name) {
            return None;
        }
        match kind {
            LogKind::Cache | LogKind::Counters => any().await,
            LogKind::Stream => self
                .durable_storage
                .as_ref()?
                .open_stream_for(tenant_id, namespace, name, shard, base_offset, incarnation)
                .ok(),
            LogKind::GroupCursors => self
                .consumer_groups
                .as_ref()?
                .shard_log_for(tenant_id, namespace, name, shard, base_offset, incarnation)
                .await
                .ok()
                .map(StreamLog::from_log),
            LogKind::GroupDeadLetters => self
                .group_reader
                .as_ref()?
                .dead_letters()
                .shard_log_for(tenant_id, namespace, name, shard, base_offset, incarnation)
                .await
                .ok()
                .map(StreamLog::from_log),
        }
    }

    /// Whether a log of `kind` for `name` may be opened here now. On a broker
    /// that learns its streams from the control plane, a stream's logs only
    /// once it knows which incarnation to open: before that, records shipped
    /// to it would land in a log the registration then deletes.
    fn may_open(&self, kind: LogKind, tenant_id: &str, namespace: &str, name: &str) -> bool {
        if !self.registration_required() {
            return true;
        }
        match kind {
            LogKind::Cache | LogKind::Counters => true,
            LogKind::Stream | LogKind::GroupCursors | LogKind::GroupDeadLetters => self
                .stream_incarnation(tenant_id, namespace, name)
                .is_some(),
        }
    }

    /// Tell a stream that records reached its log without passing through it.
    ///
    /// Replication writes the shard's log directly, so a follower's in-memory
    /// view of the stream stays at zero while its disk fills. Left that way, the
    /// first publish this broker accepts after being promoted waits on commit
    /// turns that were never taken, and never returns.
    pub async fn adopt_replicated(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        durable_offset: u64,
    ) -> Result<()> {
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await?;
        handle.state.advance_to(durable_offset);
        Ok(())
    }

    /// Append a generation-start record for `generation` to a durable
    /// stream's log and move the stream past it. Returns its offset.
    ///
    /// For a promoted leader, before it serves. Writing through the
    /// [`StreamLog`] alone leaves the stream's commit order waiting on the
    /// record's offset, and the first publish after it would report a drop to
    /// every subscriber reading offsets. This keeps both straight, counting any
    /// generation-start records inherited just before this one into the skip.
    pub async fn append_generation_start(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        generation: u64,
    ) -> Result<u64> {
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await?;
        let Some(log) = &handle.state.durable else {
            return Err(crate::error::BrokerError::StreamNotDurable {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            });
        };
        let offset = log.append_generation_start(generation).await?;
        let run = offset + 1 - log.event_end(0, offset + 1).await?;
        handle.state.skip_generation_start(offset, run);
        self.appended.notify_one();
        Ok(offset)
    }

    /// The stream's log was rebuilt from `base_offset`; forget the tail the
    /// old copy had. The counterpart of [`Self::adopt_replicated`] for a
    /// follower that discarded its records rather than storing more.
    pub async fn reset_replicated(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        base_offset: u64,
    ) -> Result<()> {
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await?;
        handle.state.reset_to(base_offset);
        Ok(())
    }

    /// One of a shard's logs was cut back to `tail` or rebuilt from it; forget
    /// the state derived from the records that went. A stream's tail, or the
    /// index over a cache, a counter shard or a group log; a group log also
    /// has its shard's cursors checked again before it is next served.
    pub async fn reset_log(
        &self,
        kind: LogKind,
        tenant_id: &str,
        namespace: &str,
        name: &str,
        shard: u32,
        tail: u64,
    ) -> Result<()> {
        match kind {
            LogKind::Stream => {
                self.reset_replicated(tenant_id, namespace, name, shard, tail)
                    .await
            }
            LogKind::Cache => Ok(self
                .cache
                .forget_index(tenant_id, namespace, name, shard)
                .await?),
            LogKind::Counters => match &self.counters {
                Some(counters) => Ok(counters
                    .forget_index(tenant_id, namespace, name, shard)
                    .await?),
                None => Ok(()),
            },
            LogKind::GroupCursors | LogKind::GroupDeadLetters => {
                // Its cursors are checked against what the logs now hold
                // before the shard is next served.
                if let Some(reader) = &self.group_reader {
                    reader.invalidate(tenant_id, namespace, name, shard);
                    if kind == LogKind::GroupCursors {
                        if let Some(groups) = &self.consumer_groups {
                            groups
                                .forget_index(tenant_id, namespace, name, shard)
                                .await?;
                        }
                    } else {
                        reader
                            .dead_letters()
                            .forget_index(tenant_id, namespace, name, shard)
                            .await?;
                    }
                }
                Ok(())
            }
        }
    }
}
