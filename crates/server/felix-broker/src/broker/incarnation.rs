//! Which incarnation of a stream this broker's logs belong to, and deleting
//! one.
//!
//! A stream deleted and created again under the same name is a new stream
//! with directories of its own (see `docs/durable-storage.md`, "Deleting a
//! stream"). A marker beside the logs records the incarnation registered here
//! and how far the control plane said the name's incarnations are deleted.
//!
//! Two rules keep a stale catalog from costing data. A name never moves to an
//! incarnation below one held here or one the catalog deleted: the control
//! plane gives a name increasing incarnations, so a lower one can only come
//! from a view older than one already applied. And data goes only on the
//! control plane's word that its incarnation is deleted (a `Deleted` change
//! naming it, or a tombstone in the snapshot), or on an operator's removal. A
//! later incarnation replacing the one held here closes the old one and keeps
//! its data until that word arrives.
//!
//! Registering and deleting one name must not overlap: each reads the marker,
//! decides, and writes it back, with nothing locking the name in between. The
//! broker service calls them only from its one catalog-sync task, which starts
//! after `finish_pending_deletes`. A caller that runs them concurrently for
//! one name has to serialize them itself.

use felix_storage::{MarkerRead, StreamMarker};

use super::Broker;
use super::keys::StreamKey;
use crate::error::Result;

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
        self.set_group_incarnation(&StreamKey::new(tenant_id, namespace, stream), incarnation);
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

    /// Make the stream's marker name `incarnation` and nothing deleted, as a
    /// restore to a backup point does: the control plane was put back to the
    /// point, so the incarnation the point names is the stream's again, even
    /// below one this broker registered or saw deleted since. For an
    /// operator's tool on a stopped broker; nothing is deleted.
    pub async fn restore_stream_marker(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        incarnation: u64,
    ) -> Result<()> {
        let key = StreamKey::new(tenant_id, namespace, stream);
        let shards = match self.stream_marker(&key).await? {
            MarkerRead::Valid(marker) => marker.shards,
            _ => None,
        };
        self.write_marker(&StreamMarker {
            tenant: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: key.stream.clone(),
            incarnation,
            retiring: false,
            shards,
            deleted: None,
        })
        .await?;
        if let Some(storage) = &self.durable_storage {
            storage
                .on_provider(move |provider| {
                    provider.clear_stream_floor(&key.tenant_id, &key.namespace, &key.stream)
                })
                .await?;
        }
        Ok(())
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
            .on_provider(|provider| provider.retiring_streams())
            .await?;
        for marker in pending {
            let key = StreamKey::new(
                marker.tenant.clone(),
                marker.namespace.clone(),
                marker.stream.clone(),
            );
            // Logged where it failed, and finished by the next delete or
            // registration of the name; a failure must not stop the start.
            let _ = self.finish_delete(&key, &marker).await;
        }
        Ok(())
    }

    /// The control plane's word that every incarnation of a stream up to and
    /// including `through` is deleted: a `Deleted` change naming `through`, or
    /// a tombstone in its snapshot. The registered stream goes if it is one of
    /// them, and so does every one of them this broker has data for. Safe to
    /// repeat; one already applied costs a marker read.
    ///
    /// Returns whether a registered stream was removed.
    pub async fn delete_stream_through(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        through: u64,
    ) -> Result<bool> {
        let key = StreamKey::new(tenant_id, namespace, stream);
        let marker = match self.stream_marker(&key).await? {
            MarkerRead::Valid(marker) => Some(marker),
            MarkerRead::Absent | MarkerRead::Unreadable => None,
        };
        let registered = self
            .streams
            .read()
            .await
            .get(&key)
            .map(|existing| (existing.incarnation, existing.shards));
        let covered = registered
            .map(|(incarnation, _)| incarnation)
            .filter(|incarnation| *incarnation <= through);
        if covered.is_none()
            && let Some(marker) = &marker
            && !marker.retiring
            && marker.deleted.is_some_and(|deleted| deleted >= through)
        {
            return Ok(false);
        }
        // Recorded first: from here no registration of these incarnations is
        // taken, and a delete cut short is finished from the marker.
        let pending = StreamMarker {
            tenant: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: key.stream.clone(),
            incarnation: registered
                .map(|(incarnation, _)| incarnation)
                .or(marker.as_ref().map(|marker| marker.incarnation))
                .unwrap_or(through),
            retiring: true,
            shards: max_shards(
                marker.as_ref().and_then(|marker| marker.shards),
                registered.map(|(_, shards)| shards),
            ),
            deleted: Some(
                marker
                    .as_ref()
                    .and_then(|marker| marker.deleted)
                    .map_or(through, |deleted| deleted.max(through)),
            ),
        };
        self.write_marker(&pending).await?;
        if let Some(incarnation) = covered {
            self.drop_stream(&key).await;
            // Nothing opens the stream from here on, so nothing recreates
            // what is about to be deleted.
            self.set_stream_incarnation(tenant_id, namespace, stream, None);
            self.close_incarnation(&key, incarnation).await?;
        }
        self.finish_delete(&key, &pending).await?;
        Ok(covered.is_some())
    }

    /// [`Self::delete_stream_through`] for each of a catalog snapshot's
    /// tombstones. Most name streams this broker never had, as on a broker
    /// joining the fleet; their floors go into one file with one flush
    /// instead of a delete and two marker writes each.
    pub async fn apply_tombstones(&self, tombstones: &[(&str, &str, &str, u64)]) -> Result<()> {
        let mut floors = Vec::new();
        for &tombstone in tombstones {
            let (tenant_id, namespace, stream, through) = tombstone;
            let key = StreamKey::new(tenant_id, namespace, stream);
            match self.tombstone_work(&key, through).await? {
                TombstoneWork::Done => {}
                TombstoneWork::Floor => floors.push(tombstone),
                TombstoneWork::Delete => {
                    self.delete_stream_through(tenant_id, namespace, stream, through)
                        .await?;
                }
            }
        }
        if let (Some(storage), false) = (&self.durable_storage, floors.is_empty()) {
            let floors: Vec<(String, String, String, u64)> = floors
                .into_iter()
                .map(|(tenant_id, namespace, stream, through)| {
                    (tenant_id.into(), namespace.into(), stream.into(), through)
                })
                .collect();
            storage
                .on_provider(move |provider| {
                    let floors: Vec<_> = floors
                        .iter()
                        .map(|(t, n, s, through)| (t.as_str(), n.as_str(), s.as_str(), *through))
                        .collect();
                    provider.record_stream_floors(&floors)
                })
                .await?;
        }
        Ok(())
    }

    /// What a tombstone through `through` needs here. Only a floor when
    /// nothing registered is covered, no delete is in progress and no store
    /// has a directory of a covered incarnation.
    async fn tombstone_work(&self, key: &StreamKey, through: u64) -> Result<TombstoneWork> {
        if self.durable_storage.is_none() {
            return Ok(TombstoneWork::Delete);
        }
        let marker = match self.stream_marker(key).await? {
            MarkerRead::Valid(marker) if marker.retiring => return Ok(TombstoneWork::Delete),
            MarkerRead::Valid(marker) => Some(marker),
            MarkerRead::Absent | MarkerRead::Unreadable => None,
        };
        let registered = self
            .streams
            .read()
            .await
            .get(key)
            .map(|existing| existing.incarnation);
        if registered.is_some_and(|incarnation| incarnation <= through) {
            return Ok(TombstoneWork::Delete);
        }
        let covered = |held: &u64| *held <= through;
        if self.incarnations_on_disk(key).await?.iter().any(covered) {
            return Ok(TombstoneWork::Delete);
        }
        let (tenant_id, namespace, stream) = (&key.tenant_id, &key.namespace, &key.stream);
        if let Some(groups) = &self.consumer_groups
            && groups
                .dirs_on_disk(tenant_id, namespace, stream)
                .await?
                .iter()
                .any(|(_, held)| covered(held))
        {
            return Ok(TombstoneWork::Delete);
        }
        if let Some(reader) = &self.group_reader
            && reader
                .dead_letters()
                .dirs_on_disk(tenant_id, namespace, stream)
                .await?
                .iter()
                .any(|(_, held)| covered(held))
        {
            return Ok(TombstoneWork::Delete);
        }
        let recorded = marker
            .and_then(|marker| marker.deleted)
            .max(self.stream_floor(key).await?);
        Ok(if recorded.is_some_and(|deleted| deleted >= through) {
            TombstoneWork::Done
        } else {
            TombstoneWork::Floor
        })
    }

    /// The control plane's delete of a stream that names no incarnation: a
    /// stream from before incarnations, deleted before the fleet finalized
    /// them. Its data stays, as it did before incarnations, because the name
    /// may still be created again at incarnation 0 and reopen it: a new,
    /// empty log there would be judged by quorum marks and follower positions
    /// kept for the old one. A tombstone for incarnation 0, which only comes
    /// after the finalize, deletes it.
    ///
    /// An incarnated stream is never removed by such a delete.
    pub(super) async fn close_unincarnated(&self, key: &StreamKey) -> Result<bool> {
        let registered = self
            .streams
            .read()
            .await
            .get(key)
            .map(|existing| existing.incarnation);
        match registered {
            None => Ok(false),
            Some(0) => {
                self.drop_stream(key).await;
                self.set_stream_incarnation(&key.tenant_id, &key.namespace, &key.stream, None);
                self.close_incarnation(key, 0).await?;
                Ok(true)
            }
            Some(incarnation) => {
                tracing::warn!(
                    tenant_id = %key.tenant_id,
                    namespace = %key.namespace,
                    stream = %key.stream,
                    incarnation,
                    "a delete naming no incarnation; keeping the incarnated stream registered",
                );
                Ok(false)
            }
        }
    }

    /// Delete the registered stream for an operator or a tool: the caller is
    /// the evidence. An incarnated stream is recorded as deleted, as a
    /// tombstone would; one at incarnation 0 is not, so a broker that
    /// registers its streams itself can create the name again.
    pub(super) async fn remove_registered(&self, key: &StreamKey) -> Result<bool> {
        let marker = match self.stream_marker(key).await? {
            MarkerRead::Valid(marker) => Some(marker),
            _ => None,
        };
        let registered = self
            .streams
            .read()
            .await
            .get(key)
            .map(|existing| (existing.incarnation, existing.shards));
        let Some((incarnation, shards)) = registered else {
            // A delete that stopped part-way, asked again.
            if let Some(marker) = marker.filter(|marker| marker.retiring) {
                self.finish_delete(key, &marker).await?;
            }
            return Ok(false);
        };
        if incarnation != 0 {
            return self
                .delete_stream_through(&key.tenant_id, &key.namespace, &key.stream, incarnation)
                .await;
        }
        let pending = StreamMarker {
            tenant: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: key.stream.clone(),
            incarnation: 0,
            retiring: true,
            shards: max_shards(marker.and_then(|marker| marker.shards), Some(shards)),
            deleted: None,
        };
        self.write_marker(&pending).await?;
        self.drop_stream(key).await;
        self.set_stream_incarnation(&key.tenant_id, &key.namespace, &key.stream, None);
        self.close_incarnation(key, 0).await?;
        self.finish_delete(key, &pending).await?;
        Ok(true)
    }

    /// The incarnation to register `metadata` of `key` at, given the one
    /// registered now (`previous`). `None` when the registration is refused
    /// and the stream left as it is.
    ///
    /// Refused: an incarnation the catalog deleted, one below the incarnation
    /// held here, and a record without the incarnation (0) for a name this
    /// broker has seen incarnated. Each can only come from a catalog view
    /// older than one already applied (a control-plane member behind, or a
    /// restart seeded from one), and taking it could serve a deleted stream's
    /// records. A later incarnation replaces the held one, which stops serving
    /// and keeps its data until the catalog deletes it.
    pub(super) async fn settle_incarnation(
        &self,
        key: &StreamKey,
        previous: Option<u64>,
        metadata: &crate::broker::metadata::StreamMetadata,
    ) -> Result<Option<u64>> {
        let incoming = metadata.incarnation;
        let (marker, damaged) = match self.stream_marker(key).await? {
            MarkerRead::Valid(pending) if pending.retiring => {
                self.finish_delete(key, &pending).await?;
                match self.stream_marker(key).await? {
                    MarkerRead::Valid(marker) => (Some(marker), false),
                    _ => (None, false),
                }
            }
            MarkerRead::Valid(marker) => (Some(marker), false),
            MarkerRead::Absent => (None, false),
            MarkerRead::Unreadable => (None, true),
        };
        if incoming != 0 {
            self.require_registered_streams();
        }
        let deleted = marker
            .as_ref()
            .and_then(|marker| marker.deleted)
            .max(self.stream_floor(key).await?);
        let held = previous.or(marker
            .as_ref()
            .map(|marker| marker.incarnation)
            .filter(|held| deleted.is_none_or(|deleted| *held > deleted)));
        let refusal = if deleted.is_some_and(|deleted| incoming <= deleted) {
            Some("the catalog deleted this incarnation")
        } else if incoming == 0 {
            let incarnated = match held {
                Some(held) => held != 0,
                None => {
                    damaged
                        || self
                            .incarnations_on_disk(key)
                            .await?
                            .iter()
                            .any(|held| *held != 0)
                }
            };
            incarnated.then_some("the catalog sent a stream without its incarnation")
        } else if held.is_some_and(|held| incoming < held) {
            Some("a later incarnation is held here")
        } else {
            None
        };
        if let Some(reason) = refusal {
            tracing::warn!(
                tenant_id = %key.tenant_id,
                namespace = %key.namespace,
                stream = %key.stream,
                incoming,
                held,
                deleted,
                reason,
                "refusing a stream registration from an older catalog; keeping what this broker holds",
            );
            metrics::counter!("felix_broker_stream_incarnation_refused_total").increment(1);
            return Ok(None);
        }
        if let Some(held) = held
            && held != incoming
        {
            // Stops serving; its data stays until the catalog deletes it.
            if previous.is_some() {
                self.drop_stream(key).await;
            }
            self.close_incarnation(key, held).await?;
            tracing::info!(
                tenant_id = %key.tenant_id,
                namespace = %key.namespace,
                stream = %key.stream,
                held,
                incarnation = incoming,
                "a later incarnation registered; keeping the earlier one's data until the catalog deletes it",
            );
        }
        let wanted = StreamMarker {
            tenant: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: key.stream.clone(),
            incarnation: incoming,
            retiring: false,
            shards: max_shards(
                marker.as_ref().and_then(|marker| marker.shards),
                Some(metadata.shards),
            ),
            deleted,
        };
        // An ephemeral stream from before incarnations keeps nothing a marker
        // would protect, and writes nothing to disk until a group does.
        let needed = metadata.durable || incoming != 0 || marker.is_some();
        if needed && marker.as_ref() != Some(&wanted) {
            self.write_marker(&wanted).await?;
        }
        self.set_stream_incarnation(&key.tenant_id, &key.namespace, &key.stream, Some(incoming));
        Ok(Some(incoming))
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

    /// Delete what `marker` records: every incarnation up to its `deleted`,
    /// or with none, its own incarnation. Every store is tried even when one
    /// fails, and the first failure returned; the marker then stays retiring,
    /// so the delete is finished later. Safe to repeat.
    async fn finish_delete(&self, key: &StreamKey, marker: &StreamMarker) -> Result<()> {
        let through = marker.deleted.unwrap_or(marker.incarnation);
        let (tenant_id, namespace, stream) = (&key.tenant_id, &key.namespace, &key.stream);
        let mut first_error = None;
        let mut note = |outcome: Result<()>| {
            if let Err(err) = outcome {
                tracing::error!(
                    tenant_id = %tenant_id,
                    namespace = %namespace,
                    stream = %stream,
                    through,
                    error = %err,
                    "could not delete a stream's data; it will be retried",
                );
                first_error.get_or_insert(err);
            }
        };
        // The per-group dead letters of the layout before 0.6 first: they
        // are found through the cursors, so the cursors stay until they are
        // gone.
        let legacy = self.retire_legacy_dead_letters(key, marker).await;
        let legacy_done = legacy.is_ok();
        note(legacy);
        if let Some(storage) = &self.durable_storage {
            note(
                self.retire_through(key, through, Store::Records(storage))
                    .await,
            );
        }
        if let Some(groups) = &self.consumer_groups
            && legacy_done
        {
            note(
                self.retire_through(key, through, Store::Cursors(groups))
                    .await,
            );
        }
        if let Some(reader) = &self.group_reader {
            note(
                self.retire_through(key, through, Store::DeadLetters(reader.dead_letters()))
                    .await,
            );
        }
        if let Some(err) = first_error {
            return Err(err);
        }
        // Only the marker this delete wrote: a registration since rewrote it.
        // A registration overlapping this delete would also land here, its
        // stream retired and no floor recorded; the module docs say why none
        // does.
        if let MarkerRead::Valid(current) = self.stream_marker(key).await?
            && current == *marker
        {
            if marker.deleted.is_some() {
                self.write_marker(&StreamMarker {
                    retiring: false,
                    ..marker.clone()
                })
                .await?;
            } else {
                self.remove_marker(key).await?;
            }
        }
        Ok(())
    }

    /// Retire every incarnation of `key` up to `through` that `store` has
    /// directories for.
    async fn retire_through(&self, key: &StreamKey, through: u64, store: Store<'_>) -> Result<()> {
        let (tenant_id, namespace, stream) = (&key.tenant_id, &key.namespace, &key.stream);
        let held: std::collections::BTreeSet<u64> = match store {
            Store::Records(_) => self.incarnations_on_disk(key).await?,
            Store::Cursors(groups) => groups
                .dirs_on_disk(tenant_id, namespace, stream)
                .await?
                .into_iter()
                .map(|(_, incarnation)| incarnation)
                .collect(),
            Store::DeadLetters(letters) => letters
                .dirs_on_disk(tenant_id, namespace, stream)
                .await?
                .into_iter()
                .map(|(_, incarnation)| incarnation)
                .collect(),
        };
        for incarnation in held.into_iter().filter(|held| *held <= through) {
            match store {
                Store::Records(storage) => {
                    storage
                        .retire_stream(tenant_id, namespace, stream, incarnation)
                        .await?
                }
                Store::Cursors(groups) => {
                    groups
                        .retire(tenant_id, namespace, stream, incarnation)
                        .await?
                }
                Store::DeadLetters(letters) => {
                    letters
                        .retire(tenant_id, namespace, stream, incarnation)
                        .await?
                }
            }
        }
        Ok(())
    }

    /// Delete the dead letters the layout before 0.6 kept per group, for a
    /// delete that covers incarnation 0. They are found from the groups with
    /// a position in incarnation 0's cursors.
    async fn retire_legacy_dead_letters(
        &self,
        key: &StreamKey,
        marker: &StreamMarker,
    ) -> Result<()> {
        let (Some(groups), Some(reader)) = (&self.consumer_groups, &self.group_reader) else {
            return Ok(());
        };
        let on_disk = groups
            .dirs_on_disk(&key.tenant_id, &key.namespace, &key.stream)
            .await?;
        let Some(shards) = on_disk
            .iter()
            .filter(|(_, incarnation)| *incarnation == 0)
            .map(|(shard, _)| shard + 1)
            .max()
        else {
            return Ok(());
        };
        let shards = shards.max(marker.shards.unwrap_or(0));
        let found = if self
            .stream_incarnation(&key.tenant_id, &key.namespace, &key.stream)
            .is_some()
        {
            // A tombstone for 0 while a later incarnation is registered:
            // switching the group stores to 0 would close the live stream's.
            groups
                .legacy_groups_beside(&key.tenant_id, &key.namespace, &key.stream, shards)
                .await
        } else {
            // The group stores open incarnation 0 to read them, and stop
            // again before anything moves.
            self.set_group_incarnation(key, Some(0));
            let found = groups
                .legacy_groups(&key.tenant_id, &key.namespace, &key.stream, shards)
                .await;
            self.set_group_incarnation(key, None);
            found
        };
        reader
            .dead_letters()
            .retire_legacy(&key.tenant_id, &key.namespace, &key.stream, &found?)
            .await
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

    async fn stream_marker(&self, key: &StreamKey) -> Result<MarkerRead> {
        let Some(storage) = &self.durable_storage else {
            return Ok(MarkerRead::Absent);
        };
        let key = key.clone();
        storage
            .on_provider(move |provider| {
                provider.stream_marker(&key.tenant_id, &key.namespace, &key.stream)
            })
            .await
    }

    async fn stream_floor(&self, key: &StreamKey) -> Result<Option<u64>> {
        let Some(storage) = &self.durable_storage else {
            return Ok(None);
        };
        let key = key.clone();
        storage
            .on_provider(move |provider| {
                provider.stream_floor(&key.tenant_id, &key.namespace, &key.stream)
            })
            .await
    }

    async fn incarnations_on_disk(
        &self,
        key: &StreamKey,
    ) -> Result<std::collections::BTreeSet<u64>> {
        let Some(storage) = &self.durable_storage else {
            return Ok(Default::default());
        };
        let key = key.clone();
        storage
            .on_provider(move |provider| {
                provider.stream_incarnations_on_disk(&key.tenant_id, &key.namespace, &key.stream)
            })
            .await
    }

    async fn write_marker(&self, marker: &StreamMarker) -> Result<()> {
        let Some(storage) = &self.durable_storage else {
            return Ok(());
        };
        let marker = marker.clone();
        storage
            .on_provider(move |provider| provider.write_stream_marker(&marker))
            .await
    }

    async fn remove_marker(&self, key: &StreamKey) -> Result<()> {
        let Some(storage) = &self.durable_storage else {
            return Ok(());
        };
        let key = key.clone();
        storage
            .on_provider(move |provider| {
                provider.remove_stream_marker(&key.tenant_id, &key.namespace, &key.stream)
            })
            .await
    }
}

/// What one catalog tombstone needs from this broker.
enum TombstoneWork {
    /// Its floor is already recorded.
    Done,
    /// Nothing here to delete: record the floor.
    Floor,
    /// A full delete.
    Delete,
}

/// One of the stores a stream keeps directories in.
#[derive(Clone, Copy)]
enum Store<'a> {
    Records(&'a crate::DurableStorage),
    Cursors(&'a crate::queue::ConsumerGroups),
    DeadLetters(&'a crate::queue::DeadLetters),
}

fn max_shards(a: Option<u32>, b: Option<u32>) -> Option<u32> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    }
}
