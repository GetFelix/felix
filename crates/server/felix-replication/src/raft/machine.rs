//! The state machine of a shard group. A shard's state is its log, so this
//! keeps only what openraft asks every state machine for: the last applied
//! entry and the membership in force.
//!
//! Both are kept in memory and rebuilt from the log on start. The snapshot
//! is the same two values in the group's [`SnapshotStore`], with no data.

use std::io;

use felix_storage::raft_log::{LogPoint, SnapshotId, SnapshotStore, StoredSnapshot};
use futures::{Stream, StreamExt};
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::{EntryPayload, OptionalSend};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use super::codec::{StoredMembership, invalid};
use super::commit::Commits;
use super::types::{Applied, ShardTypes};

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
}

impl Machine {
    /// The state machine over the snapshot `snapshots` holds, if any, and
    /// what it has applied as that changes.
    pub(crate) async fn open(
        snapshots: SnapshotStore,
    ) -> io::Result<(Self, watch::Receiver<Commits>)> {
        let state = match snapshots.current().await? {
            Some(stored) => StoredMeta::state(&stored.meta)?,
            None => State::default(),
        };
        let commits = Commits::resumed(state.last_applied.as_ref().map(point));
        let (published, applied) = watch::channel(commits.clone());
        let machine = Self {
            snapshots,
            state,
            commits,
            published,
        };
        Ok((machine, applied))
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

/// The snapshot a voter of a switch from the hybrid starts from: everything
/// up to the base's entry applied, and `voters` as the membership, held at
/// that entry.
pub(crate) fn base_snapshot(
    base: LogPoint,
    voters: &std::collections::BTreeSet<super::types::MemberId>,
) -> io::Result<StoredSnapshot> {
    let at = Some((base.term, base.index));
    let meta = StoredMeta {
        last_applied: at,
        membership_at: at,
        membership: StoredMembership {
            version: super::codec::MEMBERSHIP_VERSION,
            voters: vec![voters.clone()],
            learners: std::collections::BTreeSet::new(),
            target: Some(super::membership::TargetVersion::default()),
        },
    };
    Ok(StoredSnapshot {
        last: Some(SnapshotId {
            term: base.term,
            index: base.index,
        }),
        meta: serde_json::to_vec(&meta).map_err(io::Error::other)?.into(),
        data: bytes::Bytes::new(),
    })
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
        Ok((self.state.last_applied, self.state.membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> io::Result<()>
    where
        Strm: Stream<Item = io::Result<EntryResponder<ShardTypes>>> + Unpin + OptionalSend,
    {
        let mut applied = Ok(());
        while let Some(next) = entries.next().await {
            let (entry, responder) = match next {
                Ok(next) => next,
                Err(err) => {
                    applied = Err(err);
                    break;
                }
            };
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
            snapshot: Vec::new(),
        }))
    }
}
