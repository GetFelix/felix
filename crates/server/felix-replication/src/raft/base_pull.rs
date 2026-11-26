//! A learner's pull of a re-formed stream's base: the committed entries
//! below the gap, which the gap snapshot does not carry. The learner asks
//! a voter in chunks ([`serve`] is the voter's side, which only reads and
//! bounds), checks each entry with [`felix_storage::raft_copy::BaseCheck`]
//! and stages it ([`pull`]). Issue #3005.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use felix_storage::log::LogConfig;
use felix_storage::raft_copy::{BaseCheck, EpochBase, EpochLogId, Staging, discard_staged};
use felix_storage::raft_log::LogReader;
use openraft::type_config::alias::EntryOf;

use super::codec::ShardCodec;
use super::network::{Ask, Beat, RaftReply, RaftRequest, RaftTransport, Reply, Request};
use super::types::{GroupId, MemberId, ShardTypes};
use super::wire::entry_bytes;
use tokio_util::sync::CancellationToken;

/// The most entries one chunk carries, whatever the learner asks.
pub(crate) const MAX_BASE_ENTRIES: u32 = 4096;
/// The most record bytes one chunk carries, whatever the learner asks. A
/// chunk always carries its first entry, which may be a whole
/// `MAX_RECORD_BYTES` record.
pub(crate) const MAX_BASE_BYTES: u32 = 8 * 1024 * 1024;

/// One chunk of the base, starting at the asked index.
#[derive(Debug)]
pub(crate) struct BaseChunk {
    /// The lowest index the voter serves: the higher of its log's first
    /// index and the trim floor in force at the base. Both are committed,
    /// so nothing below it is a record the group still serves. Above the
    /// asked start, the chunk is empty and the learner restarts here,
    /// keeping this as its floor so it never serves below it.
    pub(crate) floor: u64,
    pub(crate) entries: Vec<EntryOf<ShardTypes>>,
}

/// Read the base entries from `start` that fit `max_bytes` and `max_count`,
/// each held to its own cap. Refused when the copy has no base or `start`
/// is past it: nothing past the base is ever served, since the gap holds
/// nothing and the epoch's entries come by append. `floor` gives the
/// chunk's floor for a base ending at the index it is passed; it is read
/// again after the read, so a purge that moves it under the read answers
/// an empty chunk at the new floor instead of entries it may have cut.
pub(crate) async fn serve(
    reader: &mut LogReader<ShardTypes, ShardCodec>,
    floor: impl Fn(u64) -> u64,
    base: Option<EpochBase>,
    start: u64,
    max_bytes: u32,
    max_count: u32,
) -> Result<BaseChunk, String> {
    let Some(last) = base.and_then(|base| base.base_last) else {
        return Err("this copy holds no re-form base".into());
    };
    if start > last.index {
        return Err(format!(
            "{start} is past the base, which ends at {}",
            last.index
        ));
    }
    let empty = |floor| BaseChunk {
        floor,
        entries: Vec::new(),
    };
    let moved = || Some(floor(last.index)).filter(|floor| *floor > start);
    if let Some(floor) = moved() {
        return Ok(empty(floor));
    }
    let count = u64::from(max_count.clamp(1, MAX_BASE_ENTRIES));
    let budget = max_bytes.clamp(1, MAX_BASE_BYTES) as usize;
    let end = last.index.min(start + count - 1);
    let mut entries: Vec<EntryOf<ShardTypes>> = Vec::new();
    let mut bytes = 0;
    let mut next = start;
    'pages: while next <= end {
        // One page a read, so a chunk never holds much past its budget.
        let page = reader
            .base_page(next, end + 1)
            .await
            .map_err(|err: io::Error| err.to_string())?;
        if page.is_empty() {
            if let Some(floor) = moved() {
                return Ok(empty(floor));
            }
            return Err(format!("the log holds no entry at {next}, inside the base"));
        }
        for entry in page {
            let index = entry.log_id.index;
            if index != next {
                if let Some(floor) = moved() {
                    return Ok(empty(floor));
                }
                return Err(format!("the log skips from {next} to {index}"));
            }
            let size = entry_bytes(&entry);
            if !entries.is_empty() && bytes + size > budget {
                break 'pages;
            }
            bytes += size;
            entries.push(entry);
            next += 1;
            if bytes >= budget {
                break 'pages;
            }
        }
    }
    if let Some(floor) = moved() {
        return Ok(empty(floor));
    }
    Ok(BaseChunk {
        floor: floor(last.index),
        entries,
    })
}

/// Where a learner's pull asks for chunks of the base. The real one is the
/// peer transport, which refuses a voter that has not offered base pulls.
#[async_trait]
pub(crate) trait ChunkSource: Send + Sync {
    async fn chunk(
        &self,
        voter: &MemberId,
        start: u64,
        max_bytes: u32,
        max_count: u32,
    ) -> Result<BaseChunk, String>;
}

/// How a pull ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Pulled {
    /// The staging log holds the whole of `floor..=base`, synced and marked.
    /// The copy adopts it the next time it opens.
    Staged(EpochLogId),
    /// The voters' floor is past the base: there is nothing below the gap to
    /// hold.
    BelowFloor { floor: u64 },
    /// No voter served the whole base within the rounds allowed. Nothing is
    /// staged.
    GaveUp,
}

/// How hard a pull tries.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PullPace {
    /// Full passes over the voters before the pull gives up.
    pub(crate) rounds: u32,
    /// The wait after the first pass that served nothing; it doubles each
    /// pass up to `max_backoff`.
    pub(crate) backoff: Duration,
    pub(crate) max_backoff: Duration,
}

impl Default for PullPace {
    fn default() -> Self {
        Self {
            rounds: 8,
            backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(10),
        }
    }
}

/// Pull `floor..=base` from `voters` into the staging log of `copy`, one
/// chunk in memory at a time. The whole pull comes from one voter: any
/// refusal, error or mismatch discards what is staged and starts over with
/// the next, so the staging log never mixes two voters' entries. A voter's
/// floor above the next index restarts the pull there.
pub(crate) async fn pull(
    source: &dyn ChunkSource,
    voters: &[MemberId],
    base: &EpochBase,
    floor: u64,
    copy: &Path,
    config: &LogConfig,
    pace: PullPace,
) -> io::Result<Pulled> {
    let Some(last) = base.base_last else {
        return Ok(Pulled::BelowFloor { floor });
    };
    let mut backoff = pace.backoff;
    for round in 0..pace.rounds {
        if round > 0 {
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(pace.max_backoff);
        }
        for voter in voters {
            match pull_from(source, voter, base, floor, copy, config).await {
                Ok(staged @ Pulled::Staged(_)) => return Ok(staged),
                Ok(pulled) => {
                    discard_staged(copy)?;
                    return Ok(pulled);
                }
                Err(err) => {
                    tracing::debug!(%voter, base = last.index, "base pull abandoned: {err}");
                }
            }
            // A voter that served part of another history leaves nothing
            // behind for the next.
            discard_staged(copy)?;
        }
    }
    Ok(Pulled::GaveUp)
}

/// One voter's attempt. `Err` means try the next voter.
async fn pull_from(
    source: &dyn ChunkSource,
    voter: &MemberId,
    base: &EpochBase,
    floor: u64,
    copy: &Path,
    config: &LogConfig,
) -> Result<Pulled, String> {
    let (Some(last), Some(mut check)) = (base.base_last, BaseCheck::new(base, floor)) else {
        return Ok(Pulled::BelowFloor { floor });
    };
    // Begun at the first chunk, so a voter that serves nothing stages
    // nothing.
    let mut staging: Option<Staging> = None;
    loop {
        let chunk = source
            .chunk(voter, check.next(), MAX_BASE_BYTES, MAX_BASE_ENTRIES)
            .await?;
        if chunk.floor > last.index {
            return Ok(Pulled::BelowFloor { floor: chunk.floor });
        }
        if check.raise_floor(chunk.floor) {
            if let Some(staging) = staging.as_mut() {
                staging
                    .restart(chunk.floor)
                    .await
                    .map_err(|err| err.to_string())?;
            }
            continue;
        }
        if chunk.entries.is_empty() {
            return Err(format!("an empty chunk at {}", check.next()));
        }
        for entry in &chunk.entries {
            check
                .take(entry.log_id.index, entry.log_id.leader_id.term)
                .map_err(|mismatch| format!("{mismatch:?}"))?;
        }
        let mut filling = match staging.take() {
            Some(filling) => filling,
            None => {
                let start = chunk.entries[0].log_id.index;
                Staging::begin(copy, start, config)
                    .await
                    .map_err(|err| err.to_string())?
            }
        };
        filling
            .append::<ShardTypes, ShardCodec>(&ShardCodec, &chunk.entries, wall_clock_micros())
            .await
            .map_err(|err| err.to_string())?;
        drop(chunk);
        if check.complete() {
            filling.finish(last).await.map_err(|err| err.to_string())?;
            return Ok(Pulled::Staged(last));
        }
        staging = Some(filling);
    }
}

/// [`ChunkSource`] over the Raft transport, which refuses a voter that has
/// not offered base pulls.
pub(crate) struct TransportSource {
    pub(crate) transport: Arc<dyn RaftTransport>,
    pub(crate) group: GroupId,
    pub(crate) from: MemberId,
    pub(crate) deadline: Duration,
}

#[async_trait]
impl ChunkSource for TransportSource {
    async fn chunk(
        &self,
        voter: &MemberId,
        start: u64,
        max_bytes: u32,
        max_count: u32,
    ) -> Result<BaseChunk, String> {
        let request = RaftRequest {
            group: self.group.clone(),
            to: voter.clone(),
            beat: Beat::default(),
            report: Ask::Nothing,
            deadline: self.deadline,
            body: Request::BasePull {
                from: self.from.clone(),
                start,
                max_bytes,
                max_count,
            },
        };
        match tokio::time::timeout(self.deadline, self.transport.send(request)).await {
            Ok(Ok(RaftReply(Reply::Base { floor, entries }))) => Ok(BaseChunk { floor, entries }),
            Ok(Ok(_)) => Err("it answered something other than a base chunk".into()),
            Ok(Err(refused)) => Err(format!("{refused:?}")),
            Err(_) => Err("it did not answer in time".into()),
        }
    }
}

/// A group's pull, at most one at a time, and whether one staged the base.
#[derive(Debug, Default)]
pub(crate) struct PullState {
    running: AtomicBool,
    staged: AtomicBool,
    /// Set once the member took a snapshot or entries: it is no longer
    /// blank, so a staged base must never replace its log.
    abandoned: AtomicBool,
    cancel: CancellationToken,
}

impl PullState {
    pub(crate) fn staged(&self) -> bool {
        self.staged.load(Ordering::Acquire) && !self.abandoned.load(Ordering::Acquire)
    }

    /// The member caught up another way: stop any pull and never start or
    /// signal one again. A staging log left behind is discarded by the
    /// next open, which adopts only into a copy that holds nothing.
    pub(crate) fn abandon(&self) {
        if !self.abandoned.swap(true, Ordering::AcqRel) {
            self.cancel.cancel();
        }
    }

    fn abandoned(&self) -> bool {
        self.abandoned.load(Ordering::Acquire)
    }
}

/// Run [`pull`] on its own task unless one is running or has staged the
/// base. A pull that gives up or fails clears the way for the next refused
/// snapshot to start another.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn(
    runtime: &tokio::runtime::Handle,
    state: Arc<PullState>,
    source: impl ChunkSource + 'static,
    voters: Vec<MemberId>,
    copy: PathBuf,
    config: LogConfig,
    floor: u64,
    stop: CancellationToken,
    label: String,
) {
    if state.abandoned() || state.staged() || state.running.swap(true, Ordering::AcqRel) {
        return;
    }
    runtime.spawn(async move {
        let pulled = tokio::select! {
            biased;
            () = stop.cancelled() => None,
            () = state.cancel.cancelled() => None,
            pulled = async {
                let base = {
                    let copy = copy.clone();
                    felix_common::blocking::spawn(move || felix_storage::raft_copy::latest_base(&copy))
                        .await
                        .map_err(io::Error::other)??
                };
                let Some(base) = base else {
                    return Ok(Pulled::GaveUp);
                };
                pull(&source, &voters, &base, floor, &copy, &config, PullPace::default()).await
            } => Some(pulled),
        };
        match pulled {
            Some(Ok(Pulled::Staged(last))) => {
                state.staged.store(true, Ordering::Release);
                // Checked after the store, so an abandon on either side of
                // it leaves nothing signalled.
                if state.abandoned() {
                    state.staged.store(false, Ordering::Release);
                    discard_quietly(&copy, &label).await;
                } else {
                    tracing::info!(member = %label, base = last.index, "staged a re-formed stream's base; the member reopens to adopt it");
                }
            }
            Some(Ok(Pulled::BelowFloor { floor })) => {
                tracing::warn!(member = %label, floor, "the voters' floor is past the re-form base; nothing to pull");
            }
            Some(Ok(Pulled::GaveUp)) => {
                tracing::warn!(member = %label, voters = voters.len(), "no voter served the re-form base; the next refused snapshot tries again");
            }
            Some(Err(err)) => {
                tracing::warn!(member = %label, error = %err, "a base pull failed; the next refused snapshot tries again");
            }
            None => discard_quietly(&copy, &label).await,
        }
        state.running.store(false, Ordering::Release);
    });
}

/// Drop a staging log the member will never adopt.
async fn discard_quietly(copy: &Path, label: &str) {
    let copy = copy.to_path_buf();
    let discarded = felix_common::blocking::spawn(move || discard_staged(&copy)).await;
    if let Err(err) = discarded.map_err(io::Error::other).and_then(|done| done) {
        tracing::warn!(member = %label, error = %err, "could not discard a staged base; the next open does");
    }
}

fn wall_clock_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_micros()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests;
