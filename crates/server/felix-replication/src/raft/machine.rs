//! The state machine of a shard group. A shard's state is its log, so this
//! keeps only what openraft asks every state machine for: the last applied
//! entry and the membership in force.
//!
//! Both are kept in memory and rebuilt from the log on start. The snapshot
//! is the same two values in the group's [`SnapshotStore`], with no data.

use std::io;

use felix_storage::raft_log::{SnapshotId, SnapshotStore, StoredSnapshot};
use futures::{Stream, StreamExt};
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::{EntryPayload, OptionalSend};
use serde::{Deserialize, Serialize};

use super::codec::{StoredMembership, invalid};
use super::types::{Applied, ShardTypes};

#[derive(Clone, Default)]
struct State {
    last_applied: Option<LogIdOf<ShardTypes>>,
    membership: StoredMembershipOf<ShardTypes>,
}

pub(crate) struct Machine {
    snapshots: SnapshotStore,
    state: State,
}

impl Machine {
    /// The state machine over the snapshot `snapshots` holds, if any.
    pub(crate) async fn open(snapshots: SnapshotStore) -> io::Result<Self> {
        let state = match snapshots.current().await? {
            Some(stored) => StoredMeta::state(&stored.meta)?,
            None => State::default(),
        };
        Ok(Self { snapshots, state })
    }
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

async fn store(snapshots: &SnapshotStore, meta: &SnapshotMetaOf<ShardTypes>) -> io::Result<()> {
    let stored = StoredSnapshot {
        last: meta.last_log_id.as_ref().map(|id| {
            let (term, index) = term_index(id);
            SnapshotId { term, index }
        }),
        meta: serde_json::to_vec(&StoredMeta::of(meta))
            .map_err(io::Error::other)?
            .into(),
        data: bytes::Bytes::new(),
    };
    // False means a later snapshot is already stored, which is fine.
    snapshots.install(stored).await.map(|_| ())
}

fn meta(state: &State) -> SnapshotMetaOf<ShardTypes> {
    SnapshotMetaOf::<ShardTypes> {
        last_log_id: state.last_applied.clone(),
        last_membership: state.membership.clone(),
    }
}

pub(crate) struct Builder {
    snapshots: SnapshotStore,
    state: State,
}

impl RaftSnapshotBuilder<ShardTypes> for Builder {
    type SnapshotData = Vec<u8>;

    async fn build_snapshot(&mut self) -> io::Result<SnapshotOf<ShardTypes, Vec<u8>>> {
        let meta = meta(&self.state);
        store(&self.snapshots, &meta).await?;
        Ok(SnapshotOf::<ShardTypes, Vec<u8>> {
            meta,
            snapshot: Vec::new(),
        })
    }
}

impl RaftStateMachine<ShardTypes> for Machine {
    type SnapshotData = Vec<u8>;
    type SnapshotBuilder = Builder;

    async fn applied_state(
        &mut self,
    ) -> io::Result<(Option<LogIdOf<ShardTypes>>, StoredMembershipOf<ShardTypes>)> {
        Ok((
            self.state.last_applied.clone(),
            self.state.membership.clone(),
        ))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> io::Result<()>
    where
        Strm: Stream<Item = io::Result<EntryResponder<ShardTypes>>> + Unpin + OptionalSend,
    {
        while let Some(next) = entries.next().await {
            let (entry, responder) = next?;
            if let EntryPayload::Membership(membership) = entry.payload {
                self.state.membership =
                    StoredMembershipOf::<ShardTypes>::new(Some(entry.log_id.clone()), membership);
            }
            self.state.last_applied = Some(entry.log_id);
            if let Some(responder) = responder {
                responder.send(Applied);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        Builder {
            snapshots: self.snapshots.clone(),
            state: self.state.clone(),
        }
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<ShardTypes>,
        _snapshot: Vec<u8>,
    ) -> io::Result<()> {
        store(&self.snapshots, meta).await?;
        self.state = State {
            last_applied: meta.last_log_id.clone(),
            membership: meta.last_membership.clone(),
        };
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
            snapshot: Vec::new(),
        }))
    }
}
