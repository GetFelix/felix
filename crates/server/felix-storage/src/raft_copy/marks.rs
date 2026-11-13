//! The mark log at the data root, `raft-marks`: per shard, the copy id its
//! marks belong to, its vote mark and its log mark.
//!
//! A header (magic, version, the root sequence at the last rewrite, crc) and
//! then one frame per synced write: magic, body length, body, crc. A body is
//! the write's sequence and the full marks of every shard it changed, so the
//! last record of a shard is its marks and the highest sequence is the root
//! sequence. Writers queue their changes and one of them writes everything
//! queued with one append and one fsync. The sequence rises once per synced
//! write, and the run witness hears it before any writer returns.
//!
//! A frame that does not decode at the end of the file was never synced, so
//! nobody was answered for it, and the open cuts it. One followed by a frame
//! that decodes is damage, and the open fails. When the file is well past
//! the size of the live marks it is rewritten whole through a temporary.

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::watch;

use super::{ShardMarks, VoteMark, log_below};
use crate::StorageId;
use crate::raft_log::{LogPoint, LogPositions, state_file};
use crate::run_state::witness;

pub(crate) const FILE_NAME: &str = "raft-marks";
/// `"FLMK"`.
const MAGIC: u32 = 0x464C_4D4B;
const VERSION: u16 = 1;
/// magic(4) + version(2) + sequence(8) + crc(4).
const HEADER_LEN: usize = 18;
/// `"FLMF"`.
const FRAME_MAGIC: u32 = 0x464C_4D46;
/// magic(4) + body length(4), then the body and a crc(4) over the length
/// and the body.
const FRAME_OVERHEAD: usize = 12;
/// Bounds what a damaged length can claim.
const MAX_BODY: usize = 1 << 30;
const FLAG_LEADER: u8 = 1;
const FLAG_COMMITTED: u8 = 2;
const FLAG_LOG: u8 = 4;
/// No rewrite below this size, so a small root never rewrites.
const COMPACT_FLOOR: u64 = 1 << 20;

/// The mark log of one data root. Cheap to clone; every clone is the same
/// log. Opened only by the gate.
#[derive(Clone)]
pub struct MarkLog {
    inner: Arc<Inner>,
}

struct Inner {
    root: PathBuf,
    storage_id: parking_lot::Mutex<StorageId>,
    witness: Option<PathBuf>,
    /// The marks as synced.
    live: parking_lot::RwLock<HashMap<Arc<str>, ShardMarks>>,
    /// The root sequence as synced.
    sequence: AtomicU64,
    queue: parking_lot::Mutex<Queue>,
    writer: tokio::sync::Mutex<Writer>,
    /// Shards whose log mark is written in batches. Locked before `queue`.
    batched: parking_lot::Mutex<HashMap<Arc<str>, Source>>,
    batcher: AtomicBool,
    /// Set once a write fails, for callers that cannot wait on the writer.
    failed: parking_lot::Mutex<Option<Arc<str>>>,
    compact_floor: AtomicU64,
    #[cfg(test)]
    stall: tokio::sync::RwLock<()>,
    #[cfg(test)]
    fail_next: AtomicBool,
}

#[derive(Default)]
struct Queue {
    updates: Vec<Queued>,
    named: HashSet<Arc<str>>,
    /// The write that will take what is queued now.
    next: u64,
}

struct Queued {
    shard: Arc<str>,
    copy: u64,
    update: Update,
}

#[derive(Debug, Clone)]
enum Update {
    Fresh,
    Vote(VoteMark),
    Log(Option<LogPoint>),
    /// Lower the log mark to this id if it is above it.
    LowerLog(Option<LogPoint>),
}

struct Writer {
    file: Option<crate::fs::File>,
    len: u64,
    compact_at: u64,
    /// How many writes have been taken from the queue.
    taken: u64,
    /// Set by a failed write or rewrite. Writes from `failed_from` on
    /// failed.
    failed: Option<Arc<str>>,
    failed_from: Option<u64>,
}

impl Writer {
    fn failure(&self) -> io::Error {
        io::Error::other(
            self.failed
                .as_deref()
                .unwrap_or("the mark write failed")
                .to_string(),
        )
    }
}

struct Source {
    copy: u64,
    positions: watch::Receiver<LogPositions>,
    /// Truncations in progress: the batch leaves the shard alone.
    held: usize,
}

impl MarkLog {
    /// Open the mark log under `root`, creating it if there is none.
    pub(crate) fn open(
        root: &Path,
        storage_id: StorageId,
        witness: Option<PathBuf>,
    ) -> io::Result<Self> {
        state_file::remove_temporary(root, FILE_NAME)?;
        let path = root.join(FILE_NAME);
        let bytes = match state_file::load(root, FILE_NAME)? {
            Some(bytes) => bytes,
            None => {
                let header = encode_header(0);
                state_file::store(root, FILE_NAME, &[&header])?;
                header.to_vec()
            }
        };
        let read = read(&bytes).map_err(|why| damaged(&path, &why))?;
        let file = crate::fs::OpenOptions::new().append(true).open(&path)?;
        if read.valid < bytes.len() {
            tracing::warn!(
                path = %path.display(),
                cut = bytes.len() - read.valid,
                "cut a torn write from the end of the mark log"
            );
            file.set_len(read.valid as u64)?;
            crate::io::sync_all(&file)?;
        }
        let len = read.valid as u64;
        Ok(Self {
            inner: Arc::new(Inner {
                root: root.to_path_buf(),
                storage_id: parking_lot::Mutex::new(storage_id),
                witness,
                live: parking_lot::RwLock::new(read.live),
                sequence: AtomicU64::new(read.sequence),
                queue: parking_lot::Mutex::default(),
                writer: tokio::sync::Mutex::new(Writer {
                    file: Some(file),
                    len,
                    compact_at: COMPACT_FLOOR.max(len.saturating_mul(4)),
                    taken: 0,
                    failed: None,
                    failed_from: None,
                }),
                batched: parking_lot::Mutex::default(),
                batcher: AtomicBool::new(false),
                failed: parking_lot::Mutex::default(),
                compact_floor: AtomicU64::new(COMPACT_FLOOR),
                #[cfg(test)]
                stall: tokio::sync::RwLock::default(),
                #[cfg(test)]
                fail_next: AtomicBool::new(false),
            }),
        })
    }

    /// The root sequence: how many synced mark writes this root has seen.
    pub fn sequence(&self) -> u64 {
        self.inner.sequence.load(Ordering::Acquire)
    }

    /// The synced marks of `shard`, if the root has any.
    pub fn marks(&self, shard: &str) -> Option<ShardMarks> {
        self.inner.live.read().get(shard).cloned()
    }

    /// The storage id the witness records the sequence under.
    pub fn storage_id(&self) -> StorageId {
        *self.inner.storage_id.lock()
    }

    pub(crate) fn set_storage_id(&self, id: StorageId) {
        *self.inner.storage_id.lock() = id;
    }

    pub(crate) fn root(&self) -> &Path {
        &self.inner.root
    }

    /// Tell the witness the root's sequence under its current storage id.
    pub(crate) fn record_witness(&self) {
        witness::record_sequence(
            &self.inner.root,
            self.inner.witness.as_deref(),
            self.storage_id(),
            self.sequence(),
        );
    }

    /// Why the log stopped, once a write failed. Every later write fails
    /// with it.
    pub fn failure(&self) -> Option<io::Error> {
        self.inner
            .failed
            .lock()
            .as_deref()
            .map(|why| io::Error::other(why.to_string()))
    }

    /// Marks for `shard` as copy `copy`.
    pub(crate) fn for_copy(&self, shard: Arc<str>, copy: u64) -> CopyMarks {
        CopyMarks {
            log: self.clone(),
            shard,
            copy,
        }
    }

    /// Write fresh marks for a new copy: never voted, no entry. Synced
    /// before this returns.
    pub(crate) async fn fresh(&self, shard: Arc<str>, copy: u64) -> io::Result<()> {
        self.submit(vec![Queued {
            shard,
            copy,
            update: Update::Fresh,
        }])
        .await
    }

    /// Write the flushed position of every batched shard whose position
    /// moved, in one synced write.
    pub async fn mark_batched(&self) -> io::Result<()> {
        let ticket = {
            let mut sources = self.inner.batched.lock();
            // A source whose log is gone is dropped once its last position
            // is marked.
            let live = self.inner.live.read();
            sources.retain(|shard, source| {
                source.positions.has_changed().is_ok()
                    || live.get(shard).is_some_and(|marks| {
                        let flushed = source.positions.borrow().flushed;
                        marks.copy == source.copy && flushed.is_some() && marks.log != flushed
                    })
            });
            // A position of `None` is a log with no append since it opened,
            // not an empty one: a truncation lowers the mark itself.
            let updates: Vec<Queued> = sources
                .iter()
                .filter_map(|(shard, source)| {
                    let flushed = source.positions.borrow().flushed?;
                    let marks = live.get(shard)?;
                    (source.held == 0 && marks.copy == source.copy && marks.log != Some(flushed))
                        .then(|| Queued {
                            shard: Arc::clone(shard),
                            copy: source.copy,
                            update: Update::Log(Some(flushed)),
                        })
                })
                .collect();
            drop(live);
            // Under the sources' lock, so a truncation that holds a shard
            // after this read queues its lower mark after this one.
            self.enqueue(updates)?
        };
        match ticket {
            Some(ticket) => self.wait(ticket).await,
            None => Ok(()),
        }
    }

    /// Run [`Self::mark_batched`] every `interval` until the log is
    /// dropped or a batch fails. Started once per log, so the first batched
    /// shard's interval sets it for the whole root; later calls do nothing.
    /// A failed batch stops the log, and from then on the batched shards'
    /// appends fail too (see [`Self::failure`]), so no leader keeps acking
    /// past a mark that stopped moving.
    pub(crate) fn start_batcher(&self, interval: Duration) {
        if self.inner.batcher.swap(true, Ordering::AcqRel) {
            return;
        }
        let weak: Weak<Inner> = Arc::downgrade(&self.inner);
        tokio::spawn(async move {
            let mut ticks = tokio::time::interval(interval);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticks.tick().await;
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                let log = MarkLog { inner };
                if let Err(err) = log.mark_batched().await {
                    tracing::error!(error = %err, "the batched log marks were not written");
                    log.stop(err).await;
                    return;
                }
            }
        });
    }

    /// Make the next write rewrite the file once it is this long. Tests use
    /// it to reach a rewrite without a megabyte of marks.
    #[cfg(test)]
    pub(crate) async fn set_compaction_floor(&self, floor: u64) {
        self.inner.compact_floor.store(floor, Ordering::Relaxed);
        self.inner.writer.lock().await.compact_at = floor;
    }

    /// Hold the writer, as a write in flight does.
    #[cfg(test)]
    pub(crate) async fn hold_writer(&self) -> impl Drop + '_ {
        self.inner.writer.lock().await
    }

    /// Stop the next writes after they leave the queue, before they reach
    /// the file, until the guard drops.
    #[cfg(test)]
    pub(crate) async fn stall_writes(&self) -> impl Drop + '_ {
        self.inner.stall.write().await
    }

    /// Fail the next write as a failed fsync would.
    #[cfg(test)]
    pub(crate) fn fail_next_write(&self) {
        self.inner.fail_next.store(true, Ordering::Relaxed);
    }

    /// How many writes have left the queue.
    #[cfg(test)]
    pub(crate) fn writes_taken(&self) -> u64 {
        self.inner.queue.lock().next
    }

    /// Changes queued for the next write.
    #[cfg(test)]
    pub(crate) fn queued(&self) -> usize {
        self.inner.queue.lock().updates.len()
    }

    #[cfg(test)]
    pub(crate) async fn file_len(&self) -> u64 {
        self.inner.writer.lock().await.len
    }

    async fn submit(&self, updates: Vec<Queued>) -> io::Result<()> {
        match self.enqueue(updates)? {
            Some(ticket) => self.wait(ticket).await,
            None => Ok(()),
        }
    }

    /// Queue `updates`, or `None` when each is already synced and nothing
    /// queued names its shard.
    fn enqueue(&self, updates: Vec<Queued>) -> io::Result<Option<u64>> {
        let mut queue = self.inner.queue.lock();
        {
            let live = self.inner.live.read();
            // A lowering always waits its turn at the writer: a write in
            // flight has left the queue but not yet reached `live`, and may
            // raise the mark above the cut this lowering is for.
            if updates.iter().all(|queued| {
                !queue.named.contains(&queued.shard)
                    && !matches!(queued.update, Update::LowerLog(_))
                    && is_noop(&live, queued)
            }) {
                return Ok(None);
            }
            for queued in &updates {
                let fresh_queued = queue.updates.iter().any(|earlier| {
                    earlier.shard == queued.shard
                        && earlier.copy == queued.copy
                        && matches!(earlier.update, Update::Fresh)
                });
                let belongs = matches!(queued.update, Update::Fresh)
                    || fresh_queued
                    || live
                        .get(&queued.shard)
                        .is_some_and(|marks| marks.copy == queued.copy);
                if !belongs {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "{}: the marks of {} do not belong to copy {:016x}",
                            self.inner.root.display(),
                            queued.shard,
                            queued.copy
                        ),
                    ));
                }
            }
        }
        for queued in updates {
            queue.named.insert(Arc::clone(&queued.shard));
            queue.updates.push(queued);
        }
        Ok(Some(queue.next))
    }

    /// Return once write `ticket` is synced, writing it if nobody has.
    ///
    /// The work runs in its own task. A caller that takes the batch answers
    /// for everyone in it, so dropping its future part way would answer the
    /// others before their write is synced and leave the writer without its
    /// file.
    async fn wait(&self, ticket: u64) -> io::Result<()> {
        let log = self.clone();
        tokio::spawn(async move { log.take_and_write(ticket).await })
            .await
            .map_err(io::Error::other)?
    }

    async fn take_and_write(&self, ticket: u64) -> io::Result<()> {
        let inner = &self.inner;
        let mut writer = inner.writer.lock().await;
        if ticket < writer.taken {
            // Another caller wrote it.
            return match writer.failed_from {
                Some(failed) if ticket >= failed => Err(writer.failure()),
                _ => Ok(()),
            };
        }
        if writer.failed.is_some() {
            return Err(writer.failure());
        }
        let batch = {
            let mut queue = inner.queue.lock();
            debug_assert_eq!(queue.next, ticket);
            queue.next += 1;
            queue.named.clear();
            std::mem::take(&mut queue.updates)
        };
        writer.taken = ticket + 1;
        #[cfg(test)]
        drop(inner.stall.read().await);
        let written = self.write(&mut writer, batch).await;
        if written.is_err() {
            writer.failed_from = Some(ticket);
        }
        written
    }

    async fn write(&self, writer: &mut Writer, batch: Vec<Queued>) -> io::Result<()> {
        let inner = &self.inner;
        let mut order: Vec<Arc<str>> = Vec::new();
        let mut changed: HashMap<Arc<str>, ShardMarks> = HashMap::new();
        {
            let live = inner.live.read();
            for Queued {
                shard,
                copy,
                update,
            } in batch
            {
                let current = match changed.get(&shard) {
                    Some(marks) => Some(marks.clone()),
                    None => live.get(&shard).cloned(),
                };
                let Some(next) = apply(current.as_ref(), copy, &update) else {
                    tracing::warn!(%shard, copy, ?update, "a mark for a copy the shard no longer holds was dropped");
                    continue;
                };
                if current.as_ref() == Some(&next) {
                    continue;
                }
                if !changed.contains_key(&shard) {
                    order.push(Arc::clone(&shard));
                }
                changed.insert(shard, next);
            }
        }
        if changed.is_empty() {
            return Ok(());
        }
        let sequence = self.sequence() + 1;
        let records: Vec<(&Arc<str>, &ShardMarks)> =
            order.iter().map(|shard| (shard, &changed[shard])).collect();
        let frame = encode_frame(sequence, &records).map_err(|err| self.fail(writer, err))?;
        #[cfg(test)]
        if inner.fail_next.swap(false, Ordering::Relaxed) {
            return Err(self.fail(writer, io::Error::other("an injected fsync failure")));
        }
        let file = writer
            .file
            .take()
            .expect("a writer that has not failed has its file");
        let root = inner.root.clone();
        let witness_dir = inner.witness.clone();
        let id = self.storage_id();
        let frame_len = frame.len() as u64;
        let written = felix_common::blocking::spawn(move || {
            let result =
                crate::io::write_all(&file, &frame).and_then(|()| crate::io::sync_data(&file));
            if result.is_ok() {
                // After the sync and before any writer returns.
                witness::record_sequence(&root, witness_dir.as_deref(), id, sequence);
            }
            (file, result)
        })
        .await
        .map_err(io::Error::other);
        let (file, result) = match written {
            Ok(written) => written,
            Err(err) => return Err(self.fail(writer, err)),
        };
        writer.file = Some(file);
        if let Err(err) = result {
            return Err(self.fail(writer, err));
        }
        writer.len += frame_len;
        inner.live.write().extend(changed);
        inner.sequence.store(sequence, Ordering::Release);
        if writer.len >= writer.compact_at {
            self.compact(writer).await?;
        }
        Ok(())
    }

    /// Rewrite the file as its header and one frame of the live marks.
    async fn compact(&self, writer: &mut Writer) -> io::Result<()> {
        let inner = &self.inner;
        let sequence = self.sequence();
        let bytes = {
            let live = inner.live.read();
            let records: Vec<(&Arc<str>, &ShardMarks)> = live.iter().collect();
            let mut bytes = encode_header(sequence).to_vec();
            if !records.is_empty() {
                bytes.extend(encode_frame(sequence, &records)?);
            }
            bytes
        };
        let root = inner.root.clone();
        let len = bytes.len() as u64;
        // The old handle names a file the rename unlinks, so it is dropped
        // either way and a failure stops the log.
        writer.file = None;
        let reopened = felix_common::blocking::spawn(move || {
            state_file::store(&root, FILE_NAME, &[&bytes])?;
            crate::fs::OpenOptions::new()
                .append(true)
                .open(root.join(FILE_NAME))
        })
        .await
        .map_err(io::Error::other)
        .and_then(|reopened| reopened);
        match reopened {
            Ok(file) => {
                writer.file = Some(file);
                writer.len = len;
                writer.compact_at = inner
                    .compact_floor
                    .load(Ordering::Relaxed)
                    .max(len.saturating_mul(4));
                Ok(())
            }
            Err(err) => Err(self.fail(writer, err)),
        }
    }

    /// Stop every later write: after a failed write or fsync what is on
    /// disk is unknown, and a retry could report a mark synced that is not.
    fn fail(&self, writer: &mut Writer, err: io::Error) -> io::Error {
        let message: Arc<str> = Arc::from(format!(
            "{}: a mark write failed, so no later one is made: {err}",
            self.inner.root.join(FILE_NAME).display()
        ));
        writer.failed = Some(Arc::clone(&message));
        self.inner
            .failed
            .lock()
            .get_or_insert_with(|| Arc::clone(&message));
        io::Error::new(err.kind(), message.to_string())
    }

    /// Stop every later write after `err`, if a failed write has not
    /// already.
    async fn stop(&self, err: io::Error) {
        let mut writer = self.inner.writer.lock().await;
        if writer.failed.is_none() {
            let _ = self.fail(&mut writer, err);
        }
    }
}

/// One shard's marks, as the copy `copy` writes them.
#[derive(Clone)]
pub struct CopyMarks {
    log: MarkLog,
    shard: Arc<str>,
    copy: u64,
}

impl CopyMarks {
    pub fn copy(&self) -> u64 {
        self.copy
    }

    pub fn shard(&self) -> &str {
        &self.shard
    }

    pub fn mark_log(&self) -> &MarkLog {
        &self.log
    }

    /// Raise the vote mark to `vote`. Synced, with the witness told, before
    /// this returns.
    pub async fn vote(&self, vote: VoteMark) -> io::Result<()> {
        self.submit(Update::Vote(vote)).await
    }

    /// Set the log mark to `point`, which must be on disk. Synced before
    /// this returns.
    pub async fn log(&self, point: Option<LogPoint>) -> io::Result<()> {
        self.submit(Update::Log(point)).await
    }

    /// Lower the log mark to `point` if it is above it, before a truncation
    /// cuts the log there, so a crash after the cut leaves no mark above the
    /// log.
    pub(crate) async fn lower_log(&self, point: Option<LogPoint>) -> io::Result<()> {
        self.submit(Update::LowerLog(point)).await
    }

    /// Have the batch write this shard's flushed position from `positions`
    /// every `interval`.
    pub(crate) fn batch_from(&self, positions: watch::Receiver<LogPositions>, interval: Duration) {
        self.log.inner.batched.lock().insert(
            Arc::clone(&self.shard),
            Source {
                copy: self.copy,
                positions,
                held: 0,
            },
        );
        self.log.start_batcher(interval);
    }

    /// Keep the batch off this shard until the guard drops.
    pub(crate) fn hold_batched(&self) -> HeldBatch {
        let mut sources = self.log.inner.batched.lock();
        if let Some(source) = sources.get_mut(&self.shard) {
            source.held += 1;
        }
        HeldBatch(self.clone())
    }

    async fn submit(&self, update: Update) -> io::Result<()> {
        self.log
            .submit(vec![Queued {
                shard: Arc::clone(&self.shard),
                copy: self.copy,
                update,
            }])
            .await
    }
}

pub(crate) struct HeldBatch(CopyMarks);

impl Drop for HeldBatch {
    fn drop(&mut self) {
        let mut sources = self.0.log.inner.batched.lock();
        if let Some(source) = sources.get_mut(&self.0.shard) {
            source.held = source.held.saturating_sub(1);
        }
    }
}

/// The marks after `update`, or `None` when they belong to another copy.
fn apply(current: Option<&ShardMarks>, copy: u64, update: &Update) -> Option<ShardMarks> {
    if let Update::Fresh = update {
        return Some(ShardMarks::fresh(copy));
    }
    let mut marks = current.filter(|marks| marks.copy == copy)?.clone();
    match update {
        Update::Fresh => unreachable!("handled above"),
        Update::Vote(vote) => marks.vote = vote.clone(),
        Update::Log(point) => marks.log = *point,
        Update::LowerLog(point) => {
            if log_below(*point, marks.log) {
                marks.log = *point;
            }
        }
    }
    Some(marks)
}

fn is_noop(live: &HashMap<Arc<str>, ShardMarks>, queued: &Queued) -> bool {
    let current = live.get(&queued.shard);
    !matches!(queued.update, Update::Fresh)
        && apply(current, queued.copy, &queued.update).as_ref() == current
}

fn encode_header(sequence: u64) -> [u8; HEADER_LEN] {
    let mut out = [0u8; HEADER_LEN];
    out[0..4].copy_from_slice(&MAGIC.to_be_bytes());
    out[4..6].copy_from_slice(&VERSION.to_be_bytes());
    out[6..14].copy_from_slice(&sequence.to_be_bytes());
    let crc = crate::segment::format::crc32(&[&out[..14]]);
    out[14..].copy_from_slice(&crc.to_be_bytes());
    out
}

fn encode_frame(sequence: u64, records: &[(&Arc<str>, &ShardMarks)]) -> io::Result<Vec<u8>> {
    let too_long = |what: &str| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{what} is too long for a mark"),
        )
    };
    let mut body = Vec::with_capacity(12 + records.len() * 64);
    body.extend_from_slice(&sequence.to_be_bytes());
    body.extend_from_slice(&(records.len() as u32).to_be_bytes());
    for (shard, marks) in records {
        let shard_len = u16::try_from(shard.len()).map_err(|_| too_long("a shard name"))?;
        body.extend_from_slice(&shard_len.to_be_bytes());
        body.extend_from_slice(shard.as_bytes());
        body.extend_from_slice(&marks.copy.to_be_bytes());
        let vote = &marks.vote;
        let mut flags = 0;
        if vote.leader.is_some() {
            flags |= FLAG_LEADER;
        }
        if vote.committed {
            flags |= FLAG_COMMITTED;
        }
        if marks.log.is_some() {
            flags |= FLAG_LOG;
        }
        body.push(flags);
        body.extend_from_slice(&vote.generation.to_be_bytes());
        if let Some(leader) = &vote.leader {
            let len = u16::try_from(leader.len()).map_err(|_| too_long("a leader id"))?;
            body.extend_from_slice(&len.to_be_bytes());
            body.extend_from_slice(leader.as_bytes());
        }
        if let Some(point) = marks.log {
            body.extend_from_slice(&point.term.to_be_bytes());
            body.extend_from_slice(&point.index.to_be_bytes());
        }
    }
    if body.len() > MAX_BODY {
        return Err(too_long("a write"));
    }
    let len = (body.len() as u32).to_be_bytes();
    let crc = crate::segment::format::crc32(&[&len, &body]);
    let mut frame = Vec::with_capacity(FRAME_OVERHEAD + body.len());
    frame.extend_from_slice(&FRAME_MAGIC.to_be_bytes());
    frame.extend_from_slice(&len);
    frame.extend_from_slice(&body);
    frame.extend_from_slice(&crc.to_be_bytes());
    Ok(frame)
}

/// What a frame holds.
struct Frame {
    len: usize,
    sequence: u64,
    records: Vec<(Arc<str>, ShardMarks)>,
}

/// The frame at the start of `bytes`, or `None` when there is no whole one
/// that decodes.
fn decode_frame(bytes: &[u8]) -> Option<Frame> {
    let mut at = Cursor(bytes);
    if at.u32()? != FRAME_MAGIC {
        return None;
    }
    let len = at.u32()? as usize;
    if len > MAX_BODY || bytes.len() < FRAME_OVERHEAD + len {
        return None;
    }
    let body = &bytes[8..8 + len];
    let crc = u32::from_be_bytes(bytes[8 + len..12 + len].try_into().ok()?);
    if crate::segment::format::crc32(&[&bytes[4..8], body]) != crc {
        return None;
    }
    let mut at = Cursor(body);
    let sequence = at.u64()?;
    let count = at.u32()?;
    let mut records = Vec::with_capacity(count.min(1 << 16) as usize);
    for _ in 0..count {
        let shard_len = at.u16()? as usize;
        let shard: Arc<str> = Arc::from(std::str::from_utf8(at.take(shard_len)?).ok()?);
        let copy = at.u64()?;
        let flags = at.u8()?;
        if flags & !(FLAG_LEADER | FLAG_COMMITTED | FLAG_LOG) != 0 {
            return None;
        }
        let generation = at.u64()?;
        let leader = if flags & FLAG_LEADER != 0 {
            let len = at.u16()? as usize;
            Some(Arc::from(std::str::from_utf8(at.take(len)?).ok()?))
        } else {
            None
        };
        let log = if flags & FLAG_LOG != 0 {
            Some(LogPoint {
                term: at.u64()?,
                index: at.u64()?,
            })
        } else {
            None
        };
        records.push((
            shard,
            ShardMarks {
                copy,
                vote: VoteMark {
                    generation,
                    leader,
                    committed: flags & FLAG_COMMITTED != 0,
                },
                log,
            },
        ));
    }
    if !at.0.is_empty() {
        return None;
    }
    Some(Frame {
        len: FRAME_OVERHEAD + len,
        sequence,
        records,
    })
}

struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.0.len() < n {
            return None;
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Some(head)
    }

    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_be_bytes(self.take(2)?.try_into().ok()?))
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }
}

/// What an open reads.
struct Read {
    live: HashMap<Arc<str>, ShardMarks>,
    sequence: u64,
    /// Where the frames that decode end.
    valid: usize,
}

fn read(bytes: &[u8]) -> Result<Read, String> {
    if bytes.len() < HEADER_LEN || bytes[..HEADER_LEN] != encode_header(header_sequence(bytes)) {
        return Err("the header does not decode".into());
    }
    let mut sequence = header_sequence(bytes);
    let mut live = HashMap::new();
    let mut at = HEADER_LEN;
    while at < bytes.len() {
        let Some(frame) = decode_frame(&bytes[at..]) else {
            // A torn last write, unless a whole frame follows.
            if (at + 1..bytes.len()).any(|later| decode_frame(&bytes[later..]).is_some()) {
                return Err(format!(
                    "the write at byte {at} is damaged and a later one is not"
                ));
            }
            break;
        };
        // The first frame after a rewrite carries the header's sequence.
        let first = at == HEADER_LEN;
        if frame.sequence < sequence || (!first && frame.sequence == sequence) {
            return Err(format!(
                "the write at byte {at} has sequence {}, not above {sequence}",
                frame.sequence
            ));
        }
        sequence = frame.sequence;
        live.extend(frame.records);
        at += frame.len;
    }
    Ok(Read {
        live,
        sequence,
        valid: at.min(bytes.len()),
    })
}

fn header_sequence(bytes: &[u8]) -> u64 {
    bytes
        .get(6..14)
        .and_then(|bytes| bytes.try_into().ok())
        .map_or(0, u64::from_be_bytes)
}

fn damaged(path: &Path, why: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("{}: the mark log is damaged: {why}", path.display()),
    )
}
