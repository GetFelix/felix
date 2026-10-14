//! Which incarnation of a stream this broker's logs belong to, and deleting
//! one.
//!
//! A stream deleted and created again under the same name is a new stream
//! with directories of its own (see `docs/durable-storage.md`, "Deleting a
//! stream"). Each stream's incarnation is kept in a marker beside the logs, so
//! a restart, or a catalog record that leaves the incarnation out, cannot
//! point the stream at another incarnation's data. A delete marks the marker
//! retiring before it moves anything, so one that stops part-way is finished
//! by the next registration, the next delete or the next start.

use felix_storage::{MarkerRead, StreamMarker};

use super::Broker;
use super::keys::StreamKey;
use crate::error::{BrokerError, Result};

impl Broker {
    /// Point every store at `incarnation` of a stream, or with `None` at
    /// nothing, so its logs cannot be opened while it is being deleted.
    ///
    /// Registration does this; a tool working on a stopped broker's data, which
    /// registers nothing, calls it to say which incarnation it means.
    pub fn set_stream_incarnation(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        incarnation: Option<u64>,
    ) {
        if let Some(storage) = &self.durable_storage {
            storage.set_stream_incarnation(tenant_id, namespace, stream, incarnation);
        }
        if let Some(groups) = &self.consumer_groups {
            groups.set_incarnation(tenant_id, namespace, stream, incarnation);
        }
        if let Some(reader) = &self.group_reader {
            reader
                .dead_letters()
                .set_incarnation(tenant_id, namespace, stream, incarnation);
        }
    }

    /// The incarnation a stream's logs open at on this broker: the registered
    /// one, or the one a tool set. `None` for a stream it does not know or is
    /// deleting.
    pub fn stream_incarnation(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Option<u64> {
        if let Some(storage) = &self.durable_storage {
            return storage.stream_incarnation(tenant_id, namespace, stream);
        }
        if let Some(groups) = &self.consumer_groups {
            return groups.incarnation(tenant_id, namespace, stream);
        }
        None
    }

    /// Keep a deleted stream's data while it is at incarnation 0 and this
    /// broker has not seen a stream with an incarnation, as brokers did before
    /// incarnations. For a broker in a cluster: the control plane starts a
    /// stream created again at incarnation 0 at the generations the deleted
    /// one had, so a new, empty log would be judged by the quorum marks and
    /// follower positions this broker and its peers kept for the old one, and
    /// acknowledge writes no follower holds.
    ///
    /// The stream's logs open again only if the name is registered again, and
    /// its marker records that it was kept. The data goes when the name is
    /// registered at an incarnation: incarnations are given only once the
    /// fleet finalized them, and nothing is created at 0 after that, so the
    /// name's incarnation 0 is gone for good.
    pub fn keep_unincarnated_streams_on_delete(&self) {
        self.keeps_unincarnated
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Read the catalog positions this broker's markers recorded, so that
    /// after a restart a catalog served from an earlier point cannot replace
    /// an incarnation registered here. Call once at start, before the catalog
    /// registers anything.
    pub fn load_catalog_positions(&self) -> Result<()> {
        let Some(storage) = &self.durable_storage else {
            return Ok(());
        };
        for marker in storage
            .provider()
            .stream_markers()
            .map_err(BrokerError::from)?
        {
            if let Some(seen_at) = marker.seen_at {
                self.note_catalog_position(
                    &marker.tenant,
                    &marker.namespace,
                    &marker.stream,
                    seen_at,
                );
            }
        }
        Ok(())
    }

    /// Record that this broker's view of a stream includes every catalog
    /// change before `position`. See [`Broker::register_stream_at`].
    pub fn note_catalog_position(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        position: u64,
    ) {
        let key = StreamKey::new(tenant_id, namespace, stream);
        let mut positions = self.catalog_positions.lock();
        let held = positions.entry(key).or_default();
        *held = (*held).max(position);
    }

    /// The furthest catalog position recorded for any stream, to tell when
    /// the control plane's stream feed has gone back behind it.
    pub fn catalog_position(&self) -> Option<u64> {
        self.catalog_positions.lock().values().copied().max()
    }

    /// Forget every catalog position: the control plane's stream feed went
    /// back and stayed back, as a restore to an earlier point does, and a
    /// position from before can no longer tell a change served again from a
    /// new one. Until then, registrations from behind them are refused.
    pub fn reset_catalog_positions(&self) {
        self.catalog_positions.lock().clear();
    }

    /// Whether `position` is before what this broker applied for `key`.
    pub(super) fn is_behind(&self, key: &StreamKey, position: Option<u64>) -> bool {
        position.is_some_and(|position| {
            self.catalog_positions
                .lock()
                .get(key)
                .is_some_and(|applied| position < *applied)
        })
    }

    /// Finish every delete that stopped part-way, as a crash leaves them. Call
    /// once at start, before the catalog registers anything. A delete that
    /// fails again is logged and left for later; only failing to list them is
    /// an error.
    pub async fn finish_pending_deletes(&self) -> Result<()> {
        let Some(storage) = &self.durable_storage else {
            return Ok(());
        };
        let pending = storage
            .provider()
            .retiring_streams()
            .map_err(BrokerError::from)?;
        for marker in pending {
            let key = StreamKey::new(
                marker.tenant.clone(),
                marker.namespace.clone(),
                marker.stream.clone(),
            );
            // Logged by `retire_everywhere`, and finished by the next delete
            // or registration of the name; a failure must not stop the start.
            let _ = self.finish_retiring(&key, &marker).await;
        }
        Ok(())
    }

    /// The incarnation to register `metadata` of `key` at, given the one
    /// registered now (`previous`) and the catalog's `position` for it. `None`
    /// when the registration is refused and the stream left as it is.
    ///
    /// Another incarnation held here stops serving. Its data is deleted only
    /// when it is incarnation 0 and the new one is not, which proves it gone:
    /// nothing is created at 0 once incarnations are given. Otherwise it stays
    /// until the catalog's delete naming it arrives; inferring a delete from a
    /// registration could take a live stream's data, if the registration was
    /// served from a catalog behind the one this broker read before.
    ///
    /// An incoming 0 is a record that may have left the field out (a control
    /// plane before incarnations, or one rolled back); an incarnated stream is
    /// never recreated at 0. So it is never taken as a new stream:
    ///
    /// - for a stream registered here at another incarnation it is refused;
    /// - for one whose marker names an incarnation, that one is registered;
    /// - for one this broker cannot place (no marker, or one that does not
    ///   parse) while an incarnated stream of the name is on disk, it is
    ///   refused, and the stream is not served until the catalog names its
    ///   incarnation. Guessing could serve a deleted stream's records.
    pub(super) async fn settle_incarnation(
        &self,
        key: &StreamKey,
        previous: Option<u64>,
        metadata: &crate::broker::metadata::StreamMetadata,
        position: Option<u64>,
    ) -> Result<Option<u64>> {
        let incoming = metadata.incarnation;
        let (marker, damaged) = match self.stream_marker(key)? {
            MarkerRead::Valid(pending) if pending.retiring => {
                self.finish_retiring(key, &pending).await?;
                (None, false)
            }
            MarkerRead::Valid(marker) => (Some(marker), false),
            MarkerRead::Absent => (None, false),
            MarkerRead::Unreadable => (None, true),
        };
        if incoming != 0 {
            self.require_registered_streams();
        }
        let held = previous.or(marker.as_ref().map(|marker| marker.incarnation));
        if incoming == 0 {
            let refuse = match held {
                Some(held) => held != 0 && previous.is_some(),
                None => self
                    .incarnations_on_disk(key)?
                    .iter()
                    .any(|held| *held != 0),
            };
            if held.is_some_and(|held| held != 0) || refuse {
                tracing::error!(
                    tenant_id = %key.tenant_id,
                    namespace = %key.namespace,
                    stream = %key.stream,
                    held,
                    refused = refuse,
                    "the catalog sent a stream without its incarnation; keeping what this broker holds",
                );
                metrics::counter!("felix_broker_stream_incarnation_missing_total").increment(1);
            }
            if refuse {
                return Ok(None);
            }
        }
        let incarnation = match held {
            Some(held) if incoming == 0 => held,
            _ => incoming,
        };
        if held != Some(incarnation) && self.is_behind(key, position) {
            tracing::warn!(
                tenant_id = %key.tenant_id,
                namespace = %key.namespace,
                stream = %key.stream,
                held,
                incarnation,
                position,
                "ignoring a stream change older than one already applied",
            );
            return Ok(None);
        }
        if let Some(held) = held
            && held != incarnation
        {
            // Incarnation 0 cannot come back once one is given, so its data
            // goes. Any other stays until the catalog's delete of it.
            let gone = held == 0;
            if previous.is_some() {
                self.drop_stream(key, Some(held), gone).await?;
            } else if gone {
                let shards = marker
                    .as_ref()
                    .and_then(|marker| marker.shards)
                    .unwrap_or(0);
                self.delete_incarnation(key, held, shards).await?;
            }
            if !gone {
                tracing::warn!(
                    tenant_id = %key.tenant_id,
                    namespace = %key.namespace,
                    stream = %key.stream,
                    held,
                    incarnation,
                    "another incarnation registered; keeping the one held here until the catalog deletes it",
                );
            }
        }
        // Only a durable stream keeps one: an ephemeral one writes nothing to
        // disk until a consumer group does. A kept stream's marker is taken
        // over by the stream registered at its incarnation.
        let was_kept = marker.as_ref().is_some_and(|marker| marker.kept);
        if metadata.durable {
            self.write_marker(
                key,
                StreamMarkerState {
                    incarnation,
                    retiring: false,
                    kept: false,
                    shards: metadata.shards,
                    seen_at: position,
                },
            )?;
        } else if was_kept && held == Some(incarnation) {
            self.remove_marker(key)?;
        }
        self.set_stream_incarnation(
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            Some(incarnation),
        );
        // Data from before incarnations under an incarnated stream's name can
        // only be a deleted stream's, for the same reason. Not when the marker
        // is damaged: what it held is unknown, and nothing is deleted on its
        // account.
        if incarnation != 0 && held.is_none() && !damaged {
            self.retire_everywhere(key, 0, &[]).await?;
        }
        if let Some(position) = position {
            self.note_catalog_position(&key.tenant_id, &key.namespace, &key.stream, position);
        }
        Ok(Some(incarnation))
    }

    /// Delete one incarnation of a stream that is no longer registered.
    /// `shards` bounds the search for its consumer groups, whose dead letters
    /// the layout before 0.6 kept per group. The delete is recorded in the
    /// stream's marker first, durable or not, since an ephemeral stream's
    /// group state is on disk too, so one that stops part-way is finished.
    pub(super) async fn delete_incarnation(
        &self,
        key: &StreamKey,
        incarnation: u64,
        shards: u32,
    ) -> Result<()> {
        if incarnation == 0
            && self
                .keeps_unincarnated
                .load(std::sync::atomic::Ordering::Relaxed)
            && !self.registration_required()
        {
            tracing::info!(
                tenant_id = %key.tenant_id,
                namespace = %key.namespace,
                stream = %key.stream,
                "keeping a deleted stream's data until its name is given an incarnation",
            );
            // Nothing opens it until the name is registered again.
            self.set_stream_incarnation(&key.tenant_id, &key.namespace, &key.stream, None);
            let seen_at = match self.stream_marker(key)? {
                MarkerRead::Valid(marker) => marker.seen_at,
                _ => None,
            };
            return self.write_marker(
                key,
                StreamMarkerState {
                    incarnation: 0,
                    retiring: false,
                    kept: true,
                    shards,
                    seen_at,
                },
            );
        }
        // Read before the stream stops opening: they are in its cursor logs.
        let legacy_groups = self.legacy_groups(key, incarnation, shards).await;
        // Nothing opens the stream from here on, so nothing recreates what is
        // about to be deleted.
        self.set_stream_incarnation(&key.tenant_id, &key.namespace, &key.stream, None);
        self.write_marker(
            key,
            StreamMarkerState {
                incarnation,
                retiring: true,
                kept: false,
                shards,
                seen_at: None,
            },
        )?;
        self.retire_everywhere(key, incarnation, &legacy_groups)
            .await
    }

    /// Delete what this broker kept of `incarnation` of `key` after it stopped
    /// serving it without a delete, on the catalog's delete naming it. The
    /// registered incarnation's marker and logs are not touched.
    pub(super) async fn delete_kept_incarnation(
        &self,
        key: &StreamKey,
        incarnation: u64,
    ) -> Result<()> {
        // Safe to repeat, and finds nothing where nothing was kept.
        self.retire_everywhere(key, incarnation, &[]).await?;
        // Its marker, when nothing registered since wrote one of its own.
        if let MarkerRead::Valid(marker) = self.stream_marker(key)?
            && marker.incarnation == incarnation
        {
            self.remove_marker(key)?;
        }
        Ok(())
    }

    /// Finish a delete of `key` that stopped part-way, if there is one.
    pub(super) async fn finish_delete(&self, key: &StreamKey) -> Result<()> {
        if let MarkerRead::Valid(marker) = self.stream_marker(key)?
            && marker.retiring
        {
            self.finish_retiring(key, &marker).await?;
        }
        Ok(())
    }

    /// Stop serving one incarnation of `key` and close its logs, keeping its
    /// data.
    pub(super) async fn close_incarnation(&self, key: &StreamKey, incarnation: u64) -> Result<()> {
        let (tenant_id, namespace, stream) = (&key.tenant_id, &key.namespace, &key.stream);
        if let Some(storage) = &self.durable_storage {
            storage
                .close_incarnation(tenant_id, namespace, stream, incarnation)
                .await?;
        }
        if let Some(groups) = &self.consumer_groups {
            groups
                .close_incarnation(tenant_id, namespace, stream, incarnation)
                .await?;
        }
        if let Some(reader) = &self.group_reader {
            reader
                .dead_letters()
                .close_incarnation(tenant_id, namespace, stream, incarnation)
                .await?;
        }
        Ok(())
    }

    /// Finish the delete a retiring marker records, finding the per-group
    /// dead letters of the layout before 0.6 from its shard count.
    async fn finish_retiring(&self, key: &StreamKey, marker: &StreamMarker) -> Result<()> {
        // The cursors are read to find them, so the group stores open the
        // incarnation for that and stop again before anything moves.
        let shards = marker.shards.unwrap_or(0);
        let legacy_groups = if marker.incarnation == 0 && shards > 0 {
            self.set_group_incarnation(key, Some(0));
            let groups = self.legacy_groups(key, 0, shards).await;
            self.set_group_incarnation(key, None);
            groups
        } else {
            Vec::new()
        };
        self.retire_everywhere(key, marker.incarnation, &legacy_groups)
            .await
    }

    /// Every group with a position on shards `0..shards` of incarnation 0 of
    /// `key`, whose dead letters the layout before 0.6 kept per group.
    async fn legacy_groups(
        &self,
        key: &StreamKey,
        incarnation: u64,
        shards: u32,
    ) -> Vec<(u32, String)> {
        match (&self.consumer_groups, incarnation) {
            (Some(groups), 0) => groups
                .legacy_groups(&key.tenant_id, &key.namespace, &key.stream, shards)
                .await
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    fn set_group_incarnation(&self, key: &StreamKey, incarnation: Option<u64>) {
        if let Some(groups) = &self.consumer_groups {
            groups.set_incarnation(&key.tenant_id, &key.namespace, &key.stream, incarnation);
        }
        if let Some(reader) = &self.group_reader {
            reader.dead_letters().set_incarnation(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                incarnation,
            );
        }
    }

    /// Move one incarnation of a stream out of service in every store, and
    /// then drop its marker. Every store is tried even when one fails, and the
    /// first failure returned; the marker then stays, so the delete is
    /// finished later. Safe to repeat.
    async fn retire_everywhere(
        &self,
        key: &StreamKey,
        incarnation: u64,
        legacy_groups: &[(u32, String)],
    ) -> Result<()> {
        let (tenant_id, namespace, stream) = (&key.tenant_id, &key.namespace, &key.stream);
        let mut first_error = None;
        let mut note = |outcome: Result<()>| {
            if let Err(err) = outcome {
                tracing::error!(
                    tenant_id = %tenant_id,
                    namespace = %namespace,
                    stream = %stream,
                    incarnation,
                    error = %err,
                    "could not delete a stream's data; it will be retried",
                );
                first_error.get_or_insert(err);
            }
        };
        // The per-group dead letters first: they are found through the
        // cursors, so the cursors stay until they are gone.
        let mut legacy_done = true;
        if let Some(reader) = &self.group_reader {
            let legacy = reader
                .dead_letters()
                .retire_legacy(tenant_id, namespace, stream, legacy_groups)
                .await;
            legacy_done = legacy.is_ok();
            note(legacy);
        }
        if let Some(storage) = &self.durable_storage {
            note(
                storage
                    .retire_stream(tenant_id, namespace, stream, incarnation)
                    .await,
            );
        }
        if let Some(groups) = &self.consumer_groups
            && legacy_done
        {
            note(
                groups
                    .retire(tenant_id, namespace, stream, incarnation)
                    .await,
            );
        }
        if let Some(reader) = &self.group_reader {
            note(
                reader
                    .dead_letters()
                    .retire(tenant_id, namespace, stream, incarnation)
                    .await,
            );
        }
        if let Some(err) = first_error {
            return Err(err);
        }
        // Only a marker for the incarnation just deleted: one written since
        // for a newer incarnation stays.
        if let MarkerRead::Valid(marker) = self.stream_marker(key)?
            && marker.retiring
            && marker.incarnation == incarnation
            && let Some(storage) = &self.durable_storage
        {
            storage
                .provider()
                .remove_stream_marker(tenant_id, namespace, stream)
                .map_err(BrokerError::from)?;
        }
        Ok(())
    }

    fn stream_marker(&self, key: &StreamKey) -> Result<MarkerRead> {
        let Some(storage) = &self.durable_storage else {
            return Ok(MarkerRead::Absent);
        };
        storage
            .provider()
            .stream_marker(&key.tenant_id, &key.namespace, &key.stream)
            .map_err(BrokerError::from)
    }

    fn incarnations_on_disk(&self, key: &StreamKey) -> Result<std::collections::BTreeSet<u64>> {
        let Some(storage) = &self.durable_storage else {
            return Ok(Default::default());
        };
        storage
            .provider()
            .stream_incarnations_on_disk(&key.tenant_id, &key.namespace, &key.stream)
            .map_err(BrokerError::from)
    }

    fn write_marker(&self, key: &StreamKey, state: StreamMarkerState) -> Result<()> {
        let Some(storage) = &self.durable_storage else {
            return Ok(());
        };
        storage
            .provider()
            .write_stream_marker(&StreamMarker {
                tenant: key.tenant_id.clone(),
                namespace: key.namespace.clone(),
                stream: key.stream.clone(),
                incarnation: state.incarnation,
                retiring: state.retiring,
                kept: state.kept,
                shards: Some(state.shards),
                seen_at: state.seen_at,
            })
            .map_err(BrokerError::from)
    }

    fn remove_marker(&self, key: &StreamKey) -> Result<()> {
        let Some(storage) = &self.durable_storage else {
            return Ok(());
        };
        storage
            .provider()
            .remove_stream_marker(&key.tenant_id, &key.namespace, &key.stream)
            .map_err(BrokerError::from)
    }
}

/// A marker's contents apart from the stream's name.
struct StreamMarkerState {
    incarnation: u64,
    retiring: bool,
    kept: bool,
    shards: u32,
    seen_at: Option<u64>,
}
