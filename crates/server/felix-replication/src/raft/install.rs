//! A cache group member's side of a snapshot that arrives in pieces: the
//! size cap and the one install a member holds at a time. The design is in
//! `docs/replication-engine.md`, "Sending a snapshot".

use std::cmp::Ordering;
use std::time::Duration;

use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, VoteOf};

use super::types::ShardTypes;

/// The largest snapshot a member takes. A cache shard whose state is over
/// this cannot bring a member that is behind its purge back.
pub(crate) const MAX_SNAPSHOT_BYTES: u64 = 1 << 30;
/// A snapshot whose next piece has not come for this long is dropped.
pub(crate) const IDLE: Duration = felix_storage::raft_log::transfer::IDLE;

/// The one snapshot a member is receiving. A sender that starts again, or
/// one with a newer vote, replaces what is held, so a leader that was
/// deposed part way cannot hold off its successor.
#[derive(Default)]
pub(crate) struct Incoming {
    slot: parking_lot::Mutex<Slot>,
}

#[derive(Default)]
struct Slot {
    held: Option<Held>,
    /// Whether a task is waiting to drop what is held once it goes idle.
    /// Set and cleared under the same lock as `held`, so a first piece that
    /// arrives as the task ends starts the next one.
    watched: bool,
}

struct Held {
    vote: VoteOf<ShardTypes>,
    upto: Option<LogIdOf<ShardTypes>>,
    data_len: u64,
    data: Vec<u8>,
    last: tokio::time::Instant,
}

pub(crate) enum Piece {
    More,
    Whole(Vec<u8>),
}

impl Incoming {
    /// Keep one piece. Refuses a snapshot over the cap at its first piece,
    /// a first piece from a sender ranked below the one being received,
    /// and a piece that does not follow the last one kept.
    pub(crate) fn take(
        &self,
        vote: &VoteOf<ShardTypes>,
        meta: &SnapshotMetaOf<ShardTypes>,
        data_len: u64,
        offset: u64,
        piece: &[u8],
    ) -> Result<Piece, String> {
        if data_len > MAX_SNAPSHOT_BYTES {
            return Err(format!(
                "a snapshot of {data_len} bytes is over this member's cap of {MAX_SNAPSHOT_BYTES}"
            ));
        }
        // `offset` is the sender's, so it can be anything.
        let ends = offset.checked_add(piece.len() as u64);
        if ends.is_none_or(|ends| ends > data_len) {
            return Err(format!(
                "a snapshot piece ends past the {data_len} bytes it declared"
            ));
        }
        let mut slot = self.slot.lock();
        let held = &mut slot.held;
        if offset == 0 {
            // A vote that cannot be compared with the holder's waits too.
            let outranked = |held: &Held| {
                !matches!(
                    vote.partial_cmp(&held.vote),
                    Some(Ordering::Greater | Ordering::Equal)
                )
            };
            if held.as_ref().is_some_and(outranked) {
                return Err("another snapshot install is in progress".into());
            }
            *held = Some(Held {
                vote: vote.clone(),
                upto: meta.last_log_id,
                data_len,
                data: Vec::new(),
                last: tokio::time::Instant::now(),
            });
        }
        let Some(taking) = held.as_mut().filter(|held| {
            held.vote == *vote
                && held.upto == meta.last_log_id
                && held.data_len == data_len
                && held.data.len() as u64 == offset
        }) else {
            return Err(format!(
                "a snapshot piece at {offset} does not follow what this member holds"
            ));
        };
        taking.data.extend_from_slice(piece);
        taking.last = tokio::time::Instant::now();
        if (taking.data.len() as u64) < data_len {
            return Ok(Piece::More);
        }
        Ok(Piece::Whole(
            held.take().map(|held| held.data).unwrap_or_default(),
        ))
    }

    /// True if the caller is to start the task that drops an idle
    /// snapshot: none is running. The task runs until [`Self::settled`].
    pub(crate) fn watch(&self) -> bool {
        let mut slot = self.slot.lock();
        !std::mem::replace(&mut slot.watched, true)
    }

    /// Drop a snapshot whose sender went away part way. True once nothing
    /// is held, and the watching task is then to end.
    pub(crate) fn settled(&self) -> bool {
        let mut slot = self.slot.lock();
        if slot
            .held
            .as_ref()
            .is_some_and(|held| held.last.elapsed() >= IDLE)
        {
            slot.held = None;
        }
        let settled = slot.held.is_none();
        if settled {
            slot.watched = false;
        }
        settled
    }

    #[cfg(test)]
    pub(crate) fn held(&self) -> Option<usize> {
        self.slot.lock().held.as_ref().map(|held| held.data.len())
    }
}

#[cfg(test)]
mod tests;
