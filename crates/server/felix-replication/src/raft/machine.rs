//! The state machine of a shard group. A stream shard's state is its log,
//! so this keeps only what openraft asks every state machine for: the last
//! applied entry and the membership in force. A cache shard's group also
//! folds its entries into a [`CacheState`].
//!
//! All of it is kept in memory. A member starts from the snapshot in the
//! group's [`SnapshotStore`] and openraft applies the log above it again.
//! A stream shard's snapshot has no data; a cache shard's is its
//! projection as of the snapshot's last entry.

use std::io;
use std::sync::Arc;

use bytes::Bytes;

use felix_storage::raft_log::{LogPoint, SnapshotId, SnapshotStore, StoredSnapshot};
use futures::{Stream, StreamExt};
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::{EntryPayload, OptionalSend};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use super::cache::{CacheCommand, CacheState};
use super::codec::{StoredMembership, invalid};
use super::commit::Commits;
use super::types::{Applied, ShardTypes};
use crate::ShardKind;

/// A cache group's projection, shared by its state machine, which writes
/// it, and the member's read and answer paths.
pub(crate) type Cache = Arc<parking_lot::RwLock<CacheState>>;

#[derive(Clone, Default)]
struct State {
    last_applied: Option<LogIdOf<ShardTypes>>,
    membership: StoredMembershipOf<ShardTypes>,
}

pub(crate) struct Machine {
    snapshots: SnapshotStore,
    state: State,
    /// What this member has applied, and so knows committed.
    commits: Commits,
    /// `commits` as the read bound and the commit waits see it, moved once
    /// per run of applied entries.
    published: watch::Sender<Commits>,
    /// `None` in a stream shard's group.
    cache: Option<Cache>,
    /// Held by a test to keep this member from applying.
    #[cfg(test)]
    pub(crate) hold: Arc<tokio::sync::Mutex<()>>,
}

impl Machine {
    /// The state machine over the snapshot `snapshots` holds, if any, and
    /// what it has applied as that changes.
    pub(crate) async fn open(
        snapshots: SnapshotStore,
        kind: ShardKind,
    ) -> io::Result<(Self, watch::Receiver<Commits>, Option<Cache>)> {
        let stored = snapshots.current().await?;
        let state = match &stored {
            Some(stored) => StoredMeta::state(&stored.meta)?,
            None => State::default(),
        };
        let cache = match kind {
            ShardKind::Stream => None,
            ShardKind::Cache => {
                let data = stored.map(|stored| stored.data).unwrap_or_default();
                Some(Arc::new(parking_lot::RwLock::new(CacheState::restore(
                    &data,
                    state.last_applied.is_some(),
                )?)))
            }
        };
        let commits = Commits::resumed(state.last_applied.as_ref().map(point));
        let (published, applied) = watch::channel(commits.clone());
        let machine = Self {
            snapshots,
            state,
            commits,
            published,
            cache: cache.clone(),
            #[cfg(test)]
            hold: Arc::default(),
        };
        Ok((machine, applied, cache))
    }

    fn publish(&self) {
        self.published.send_if_modified(|published| {
            let moved = *published != self.commits;
            if moved {
                published.clone_from(&self.commits);
            }
            moved
        });
    }
}

fn point(id: &LogIdOf<ShardTypes>) -> LogPoint {
    let (term, index) = term_index(id);
    LogPoint { term, index }
}

/// A snapshot's metadata as stored, in JSON.
#[derive(Serialize, Deserialize)]
struct StoredMeta {
    /// `(term, index)` of the last applied entry.
    last_applied: Option<(u64, u64)>,
    /// `(term, index)` of the entry that holds the membership.
    membership_at: Option<(u64, u64)>,
    membership: StoredMembership,
}

fn log_id((term, index): (u64, u64)) -> LogIdOf<ShardTypes> {
    LogIdOf::<ShardTypes>::new(term.into(), index)
}

fn term_index(id: &LogIdOf<ShardTypes>) -> (u64, u64) {
    (**id.committed_leader_id(), id.index)
}

impl StoredMeta {
    fn of(meta: &SnapshotMetaOf<ShardTypes>) -> Self {
        Self {
            last_applied: meta.last_log_id.as_ref().map(term_index),
            membership_at: meta.last_membership.log_id().as_ref().map(term_index),
            membership: StoredMembership::of(meta.last_membership.membership()),
        }
    }

    fn state(bytes: &[u8]) -> io::Result<State> {
        let meta: Self = serde_json::from_slice(bytes)
            .map_err(|err| invalid(format!("a snapshot's metadata does not parse: {err}")))?;
        Ok(State {
            last_applied: meta.last_applied.map(log_id),
            membership: StoredMembershipOf::<ShardTypes>::new(
                meta.membership_at.map(log_id),
                meta.membership.membership()?,
            ),
        })
    }
}

async fn store(
    snapshots: &SnapshotStore,
    meta: &SnapshotMetaOf<ShardTypes>,
    data: Bytes,
) -> io::Result<()> {
    let stored = StoredSnapshot {
        last: meta.last_log_id.as_ref().map(|id| {
            let (term, index) = term_index(id);
            SnapshotId { term, index }
        }),
        meta: serde_json::to_vec(&StoredMeta::of(meta))
            .map_err(io::Error::other)?
            .into(),
        data,
    };
    // False means a later snapshot is already stored, which is fine.
    snapshots.install(stored).await.map(|_| ())
}

fn meta(state: &State) -> SnapshotMetaOf<ShardTypes> {
    SnapshotMetaOf::<ShardTypes> {
        last_log_id: state.last_applied,
        last_membership: state.membership.clone(),
    }
}

pub(crate) struct Builder {
    snapshots: SnapshotStore,
    state: State,
    /// The projection as of `state.last_applied`.
    data: Bytes,
}

impl RaftSnapshotBuilder<ShardTypes> for Builder {
    type SnapshotData = Vec<u8>;

    async fn build_snapshot(&mut self) -> io::Result<SnapshotOf<ShardTypes, Vec<u8>>> {
        let meta = meta(&self.state);
        store(&self.snapshots, &meta, self.data.clone()).await?;
        Ok(SnapshotOf::<ShardTypes, Vec<u8>> {
            meta,
            snapshot: self.data.to_vec(),
        })
    }
}

impl RaftStateMachine<ShardTypes> for Machine {
    type SnapshotData = Vec<u8>;
    type SnapshotBuilder = Builder;

    async fn applied_state(
        &mut self,
    ) -> io::Result<(Option<LogIdOf<ShardTypes>>, StoredMembershipOf<ShardTypes>)> {
        Ok((self.state.last_applied, self.state.membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> io::Result<()>
    where
        Strm: Stream<Item = io::Result<EntryResponder<ShardTypes>>> + Unpin + OptionalSend,
    {
        #[cfg(test)]
        let _held = Arc::clone(&self.hold).lock_owned().await;
        let mut applied = Ok(());
        while let Some(next) = entries.next().await {
            let (entry, responder) = match next {
                Ok(next) => next,
                Err(err) => {
                    applied = Err(err);
                    break;
                }
            };
            if let (Some(cache), EntryPayload::Normal(command)) = (&self.cache, &entry.payload) {
                // An entry this build cannot read stops the member. Skipping
                // it would leave this member's state unlike the others'.
                let command = match CacheCommand::decode(&command.payload) {
                    Ok(decoded) => (command.timestamp_micros, decoded),
                    Err(err) => {
                        applied = Err(err);
                        break;
                    }
                };
                // Before the commit table moves, so a write that waits on
                // its commit finds its answer.
                cache
                    .write()
                    .apply(entry.log_id.index, command.0, &command.1);
            }
            let record = matches!(entry.payload, EntryPayload::Normal(_));
            if let EntryPayload::Membership(membership) = entry.payload {
                self.state.membership =
                    StoredMembershipOf::<ShardTypes>::new(Some(entry.log_id), membership);
            }
            self.commits.advance(point(&entry.log_id));
            if !record {
                self.commits.holds_no_record(entry.log_id.index);
            }
            self.state.last_applied = Some(entry.log_id);
            if let Some(responder) = responder {
                responder.send(Applied);
            }
        }
        // Once for the run, whatever ended it: a waiter is woken per append,
        // not per record.
        self.publish();
        applied
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // The data and the id it is as of are taken in one step, here,
        // where no entry is being applied. Taken when the snapshot is built,
        // the data could hold entries past the id, and a member that
        // restored it would apply them twice.
        Builder {
            snapshots: self.snapshots.clone(),
            state: self.state.clone(),
            data: self
                .cache
                .as_ref()
                .map(|cache| cache.read().snapshot())
                .unwrap_or_default(),
        }
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<ShardTypes>,
        snapshot: Vec<u8>,
    ) -> io::Result<()> {
        let data = Bytes::from(snapshot);
        // Read before it is stored: a snapshot that does not parse must not
        // become the one this member restarts from.
        let restored = match &self.cache {
            Some(_) => Some(CacheState::restore(&data, meta.last_log_id.is_some())?),
            None => None,
        };
        store(&self.snapshots, meta, data).await?;
        if let (Some(cache), Some(restored)) = (&self.cache, restored) {
            *cache.write() = restored;
        }
        self.state = State {
            last_applied: meta.last_log_id,
            membership: meta.last_membership.clone(),
        };
        // Only the snapshot's last entry is known by term now.
        let resumed = Commits::resumed(meta.last_log_id.as_ref().map(point));
        if resumed.read_bound() > self.commits.read_bound() {
            self.commits = resumed;
            self.publish();
        }
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> io::Result<Option<SnapshotOf<ShardTypes, Vec<u8>>>> {
        let Some(stored) = self.snapshots.current().await? else {
            return Ok(None);
        };
        let state = StoredMeta::state(&stored.meta)?;
        Ok(Some(SnapshotOf::<ShardTypes, Vec<u8>> {
            meta: meta(&state),
            snapshot: stored.data.to_vec(),
        }))
    }
}
