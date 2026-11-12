//! A state machine kept in memory, with its snapshot in a
//! [`SnapshotStore`], as the control plane's will be. It applies commands
//! by keeping them.

use std::io;

use futures_util::{Stream, StreamExt};
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::{EntryPayload, OptionalSend};
use serde::{Deserialize, Serialize};

use super::Types;
use crate::raft_log::{SnapshotId, SnapshotStore, StoredSnapshot};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(super) struct State {
    pub(super) last_applied: Option<LogIdOf<Types>>,
    pub(super) membership: StoredMembershipOf<Types>,
    pub(super) commands: Vec<String>,
}

pub(super) struct Machine {
    pub(super) snapshots: SnapshotStore,
    pub(super) state: State,
    /// Store a snapshot after every apply, so that what is applied is
    /// always what the stored snapshot covers.
    pub(super) snapshot_each_apply: bool,
}

impl Machine {
    /// An empty state machine over the snapshot in `dir`. openraft installs
    /// that snapshot at startup.
    pub(super) fn open(dir: &std::path::Path) -> Self {
        Self {
            snapshots: SnapshotStore::open(dir).expect("snapshots"),
            state: State::default(),
            snapshot_each_apply: false,
        }
    }

    fn meta(state: &State) -> SnapshotMetaOf<Types> {
        SnapshotMetaOf::<Types> {
            last_log_id: state.last_applied,
            last_membership: state.membership.clone(),
        }
    }
}

fn json<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(io::Error::other)
}

fn stored(meta: &SnapshotMetaOf<Types>, data: Vec<u8>) -> io::Result<StoredSnapshot> {
    Ok(StoredSnapshot {
        last: meta.last_log_id.map(|id| SnapshotId {
            term: **id.committed_leader_id(),
            index: id.index,
        }),
        meta: json(meta)?.into(),
        data: data.into(),
    })
}

pub(super) struct Builder {
    snapshots: SnapshotStore,
    state: State,
}

impl RaftSnapshotBuilder<Types> for Builder {
    type SnapshotData = Vec<u8>;

    async fn build_snapshot(&mut self) -> io::Result<SnapshotOf<Types, Vec<u8>>> {
        let meta = Machine::meta(&self.state);
        let data = json(&self.state)?;
        self.snapshots.install(stored(&meta, data.clone())?).await?;
        Ok(SnapshotOf::<Types, Vec<u8>> {
            meta,
            snapshot: data,
        })
    }
}

impl RaftStateMachine<Types> for Machine {
    type SnapshotData = Vec<u8>;
    type SnapshotBuilder = Builder;

    async fn applied_state(
        &mut self,
    ) -> io::Result<(Option<LogIdOf<Types>>, StoredMembershipOf<Types>)> {
        Ok((self.state.last_applied, self.state.membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> io::Result<()>
    where
        Strm: Stream<Item = io::Result<EntryResponder<Types>>> + Unpin + OptionalSend,
    {
        while let Some(next) = entries.next().await {
            let (entry, responder) = next?;
            self.state.last_applied = Some(entry.log_id);
            let response = match entry.payload {
                EntryPayload::Blank => String::new(),
                EntryPayload::Normal(command) => {
                    self.state.commands.push(command.clone());
                    command
                }
                EntryPayload::Membership(membership) => {
                    self.state.membership =
                        StoredMembershipOf::<Types>::new(Some(entry.log_id), membership);
                    String::new()
                }
            };
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        if self.snapshot_each_apply {
            let meta = Self::meta(&self.state);
            let data = json(&self.state)?;
            self.snapshots.install(stored(&meta, data)?).await?;
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
        meta: &SnapshotMetaOf<Types>,
        snapshot: Vec<u8>,
    ) -> io::Result<()> {
        let state: State = serde_json::from_slice(&snapshot).map_err(io::Error::other)?;
        self.snapshots.install(stored(meta, snapshot)?).await?;
        self.state = state;
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> io::Result<Option<SnapshotOf<Types, Vec<u8>>>> {
        let Some(stored) = self.snapshots.current().await? else {
            return Ok(None);
        };
        Ok(Some(SnapshotOf::<Types, Vec<u8>> {
            meta: serde_json::from_slice(&stored.meta).map_err(io::Error::other)?,
            snapshot: stored.data.to_vec(),
        }))
    }
}
