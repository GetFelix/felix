//! The hybrid side of the switch: the seal that fixes the base every voter
//! starts from.
//!
//! A copy that takes the seal's fence promises, on disk, that it takes the
//! shard's records only from the copy that sealed and acknowledges nothing
//! else of the shard until a later generation replaces the promise
//! ([`DiskLog::promise_seal`]). The seal leader appends the generation-start
//! record at `N`, which names the start `S` and the digests
//! ([`DiskLog::claim_seal`]), and each follower appends that record only
//! after it checked its own copy against it ([`DiskLog::ack_seal`]). The
//! rules are `FollowerAck`, `SealClaim` and `PowerLoss` in
//! `docs/formal/FelixRaftSwitch.tla`; the design is in
//! `docs/per-shard-raft.md`, "Moving a shard from the hybrid to Raft".
//!
//! The promise is a file of its own, `switch-seal`, beside the ballot. It is
//! written after the ballot that accepts the seal's generation, so a crash
//! between them leaves a copy that accepted the generation without the
//! promise. The leader never heard its answer, so it never counted it.

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{DiskLog, SideRange, SwitchBase, digest, side_holds};
use crate::disk_log::GenerationCheck;
use crate::io::sync_dir;
use crate::log::{
    AppendOnlyLog, Epoch, FsyncMode, LogRecord, Offset, ReadRange, RecordMark, WrittenBy,
};
use crate::{Result, StorageError, StorageId};

/// `"FLSP"`.
const MAGIC: u32 = 0x464C_5350;
const VERSION: u16 = 1;
/// magic(4) + version(2) + generation(8) + sealer(16) + claimed(1) + crc(4).
const ENCODED_LEN: usize = 35;
const FILE_NAME: &str = "switch-seal";

/// A copy's promise to a seal: at `generation` it takes the shard's records
/// only from the copy `sealer`, and its own leader, if it is `sealer`,
/// appends the seal's record at most once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealPromise {
    pub generation: u64,
    pub sealer: StorageId,
    /// The sealer claimed its record at `N` here. One record per generation:
    /// a sealer that lost its tail, or restarted, does not claim again at
    /// this generation; the control plane seals again at a later one.
    pub claimed: bool,
}

impl SealPromise {
    /// Whether a hybrid batch from the copy `sender` may be stored: only the
    /// sealer's catch-up. Ballots and rebuild writes are refused outright
    /// while a promise holds.
    pub fn admits_batch(&self, sender: StorageId) -> bool {
        sender == self.sealer
    }

    fn encode(&self) -> [u8; ENCODED_LEN] {
        let mut out = [0u8; ENCODED_LEN];
        out[0..4].copy_from_slice(&MAGIC.to_be_bytes());
        out[4..6].copy_from_slice(&VERSION.to_be_bytes());
        out[6..14].copy_from_slice(&self.generation.to_be_bytes());
        out[14..30].copy_from_slice(&self.sealer.to_bytes());
        out[30] = u8::from(self.claimed);
        let crc = crate::segment::format::crc32(&[&out[..31]]);
        out[31..].copy_from_slice(&crc.to_be_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != ENCODED_LEN {
            return None;
        }
        let crc = u32::from_be_bytes(bytes[31..].try_into().ok()?);
        if crate::segment::format::crc32(&[&bytes[..31]]) != crc
            || u32::from_be_bytes(bytes[0..4].try_into().ok()?) != MAGIC
            || u16::from_be_bytes(bytes[4..6].try_into().ok()?) != VERSION
        {
            return None;
        }
        let claimed = match bytes[30] {
            0 => false,
            1 => true,
            _ => return None,
        };
        Some(Self {
            generation: u64::from_be_bytes(bytes[6..14].try_into().ok()?),
            sealer: StorageId::from_bytes(bytes[14..30].try_into().ok()?),
            claimed,
        })
    }
}

/// Why a seal step was refused. Each leaves the log as it was, except
/// [`SealRefused::Claimed`] after a claim that got as far as its mark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SealRefused {
    /// No promise to this seal holds: none was taken, or a later
    /// generation replaced it.
    NoPromise,
    /// The promise is to another generation or another sealing copy.
    PromisedOther { generation: u64, sealer: StorageId },
    /// The sealer already claimed its record at this generation.
    Claimed { generation: u64 },
    /// A log is not fsync on commit, so an acknowledgement could outrun the
    /// disk and a voter could not start a Raft log on it.
    NotOnCommit { label: String },
    /// The record is not a seal's generation-start record.
    NotASeal,
    /// The log does not end right below the seal's record.
    EndsAt { tail: Offset, offset: Offset },
    /// The log starts above the seal's start.
    StartsAbove { first: Offset, start: Offset },
    /// The records from the seal's start differ from the leader's.
    DigestDiffers,
    /// A side log differs from the leader's.
    SideDiffers { label: String },
}

impl DiskLog {
    /// Take the seal's fence: accept `generation` from `leader`, whose copy
    /// is `sealer`, and promise the seal. Both are on disk when this returns
    /// [`GenerationCheck::Raised`] or [`GenerationCheck::Current`], and only
    /// then may the caller answer. Any other check, or a refusal, writes
    /// nothing.
    ///
    /// A promise names one sealing copy per generation: the same leader
    /// under another copy id, as after a wipe, is refused, so a copy that
    /// held its records for one sealer never takes another's at that
    /// generation.
    pub async fn promise_seal(
        &self,
        generation: u64,
        leader: &str,
        sealer: StorageId,
    ) -> Result<std::result::Result<GenerationCheck, SealRefused>> {
        if let Some(refused) = other_sealer(*self.inner.switch_seal.lock(), generation, sealer) {
            return Ok(Err(refused));
        }
        let check = self.accept_generation(generation, Some(leader)).await?;
        if !matches!(check, GenerationCheck::Raised | GenerationCheck::Current) {
            return Ok(Ok(check));
        }
        let inner = Arc::clone(&self.inner);
        felix_common::blocking::spawn(move || {
            // `close` sets the flag under this lock: once it has, the
            // directory may already be another log's.
            let _persisted = inner.replica_persisted.lock();
            inner.check_open()?;
            inner.check_not_poisoned()?;
            let mut held = inner.switch_seal.lock();
            if let Some(refused) = other_sealer(*held, generation, sealer) {
                return Ok(Err(refused));
            }
            let promise = match *held {
                // Taken again by a retried fence: keep the claim.
                Some(promise) if promise.generation == generation => return Ok(Ok(check)),
                Some(promise) if promise.generation > generation => {
                    return Ok(Ok(GenerationCheck::Superseded {
                        accepted: promise.generation,
                    }));
                }
                _ => SealPromise {
                    generation,
                    sealer,
                    claimed: false,
                },
            };
            inner.poison_on_sync_failure("the switch seal", store(&inner.dir, &promise))?;
            *held = Some(promise);
            Ok(Ok(check))
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }

    /// The seal this copy promised, unless a later generation was accepted
    /// since: an unsealed promotion above it reopens the shard as a hybrid
    /// shard, and nothing else does. There is no timeout.
    ///
    /// Retention and head trims hold while a promise holds, so a copy keeps
    /// the start it answered the seal's fence with. The side logs are not
    /// held: one that moves past its range in the record makes the copy
    /// fail the check and start as a learner.
    pub fn seal_promise(&self) -> Option<SealPromise> {
        let promise = (*self.inner.switch_seal.lock())?;
        (self.accepted_generation() <= promise.generation).then_some(promise)
    }

    /// The seal leader's step: append the seal's generation-start record at
    /// the tail, `N`, naming `start` (the highest log start among the copies
    /// that answered the fence, this one included) and the digests of this
    /// log from `start` to `N` and of the side logs from their starts to
    /// their tails. Returns the base the record names, once the record and
    /// everything below it are on disk.
    ///
    /// The caller has quiesced the shard first: no write admitted before the
    /// seal is still on its way to an offset, so none lands above `N` or is
    /// acknowledged after the seal. `own` is this copy's storage id, which
    /// the promise must name. The claim is marked on disk before the record
    /// is written, so a sealer that crashes or loses its tail never claims
    /// again at the same generation.
    pub async fn claim_seal(
        &self,
        own: StorageId,
        start: Offset,
        dead_letters: (&DiskLog, Offset),
        cursors: (&DiskLog, Offset),
    ) -> Result<std::result::Result<SwitchBase, SealRefused>> {
        let promise = match self.seal_promise() {
            None => return Ok(Err(SealRefused::NoPromise)),
            Some(promise) if promise.sealer != own => {
                return Ok(Err(SealRefused::PromisedOther {
                    generation: promise.generation,
                    sealer: promise.sealer,
                }));
            }
            Some(promise) if promise.claimed => {
                return Ok(Err(SealRefused::Claimed {
                    generation: promise.generation,
                }));
            }
            Some(promise) => promise,
        };
        if let Some(refused) = not_on_commit([self, dead_letters.0, cursors.0]) {
            return Ok(Err(refused));
        }
        if self.base_offset() > start {
            return Ok(Err(SealRefused::StartsAbove {
                first: self.base_offset(),
                start,
            }));
        }
        // Checked before the mark, so a side log that cannot be digested
        // from its start is a refusal, not a generation used up.
        for (log, side_start) in [dead_letters, cursors] {
            if log.base_offset() > side_start && log.tail_offset().await? > side_start {
                return Ok(Err(SealRefused::SideDiffers {
                    label: log.label().to_string(),
                }));
            }
        }
        let side_ranges = (
            SideRange::of(dead_letters.0, dead_letters.1).await?,
            SideRange::of(cursors.0, cursors.1).await?,
        );
        // The side logs' tails count only once they are on disk, as N does.
        dead_letters.0.sync().await?;
        cursors.0.sync().await?;
        self.mark_claimed(promise).await?;
        crash_point(1)?;
        self.sync().await?;
        let offset = self.tail_offset().await?;
        let base = SwitchBase {
            generation: promise.generation,
            offset,
            start,
            digest: digest(self, start, offset).await?,
            dead_letters: side_ranges.0,
            cursors: side_ranges.1,
        };
        let record = base.record(now_micros());
        if !self.append_seal_record(offset, &base, record).await? {
            return Err(invalid(format!(
                "{}: the tail moved from {offset} while the seal was claimed",
                self.label()
            )));
        }
        crash_point(2)?;
        // A side write or a compaction that slipped past the quiesce would
        // leave the record naming ranges the log no longer holds.
        for (log, range) in [
            (dead_letters.0, &base.dead_letters),
            (cursors.0, &base.cursors),
        ] {
            if !side_holds(log, range).await? {
                return Ok(Err(SealRefused::SideDiffers {
                    label: log.label().to_string(),
                }));
            }
        }
        pause_point().await;
        // A later generation accepted while the record was written reopened
        // the shard: nothing may be reported from this seal.
        if !self.promise_still(promise.generation) {
            return Ok(Err(SealRefused::NoPromise));
        }
        Ok(Ok(base))
    }

    /// A follower's step: check this copy against the seal's `record`, sent
    /// by the copy `sender`, then append the record at its offset and return
    /// once it is on disk. Only then may the caller acknowledge it.
    ///
    /// Refused unless the promise names this seal and `sender`, every log is
    /// fsync on commit, this log ends right below the record, starts at or
    /// below the seal's start, and holds the leader's records from there,
    /// and each side log holds the leader's range and nothing past it. A
    /// copy that already holds the record answers again.
    pub async fn ack_seal(
        &self,
        record: &LogRecord,
        sender: StorageId,
        dead_letters: &DiskLog,
        cursors: &DiskLog,
    ) -> Result<std::result::Result<(), SealRefused>> {
        let Some(base) = SwitchBase::from_record(record) else {
            return Ok(Err(SealRefused::NotASeal));
        };
        match self.seal_promise() {
            None => return Ok(Err(SealRefused::NoPromise)),
            Some(promise) if promise.generation != base.generation || promise.sealer != sender => {
                return Ok(Err(SealRefused::PromisedOther {
                    generation: promise.generation,
                    sealer: promise.sealer,
                }));
            }
            Some(_) => {}
        }
        if let Some(refused) = not_on_commit([self, dead_letters, cursors]) {
            return Ok(Err(refused));
        }
        let tail = self.tail_offset().await?;
        // The side logs are checked as they are on disk.
        dead_letters.sync().await?;
        cursors.sync().await?;
        // Answered before, or the record arrived some other way: every check
        // below still runs, so holding N proves nothing on its own.
        let held = tail == base.offset + 1 && self.holds_seal_record(record).await?;
        if !held && tail != base.offset {
            return Ok(Err(SealRefused::EndsAt {
                tail,
                offset: base.offset,
            }));
        }
        let first = self.base_offset();
        if first > base.start {
            return Ok(Err(SealRefused::StartsAbove {
                first,
                start: base.start,
            }));
        }
        if digest(self, base.start, base.offset).await? != base.digest {
            return Ok(Err(SealRefused::DigestDiffers));
        }
        for (log, range) in [(dead_letters, &base.dead_letters), (cursors, &base.cursors)] {
            if !side_holds(log, range).await? {
                return Ok(Err(SealRefused::SideDiffers {
                    label: log.label().to_string(),
                }));
            }
        }
        if held {
            self.sync().await?;
            return Ok(self.lapsed_refusal(base.generation));
        }
        let sent = crate::log::AppendRecord {
            payload: record.payload.clone(),
            timestamp_micros: record.timestamp_micros,
            mark: record.mark,
            publisher: None,
            headers: None,
        };
        if !self.append_seal_record(base.offset, &base, sent).await? {
            return Ok(Err(SealRefused::EndsAt {
                tail: self.tail_offset().await?,
                offset: base.offset,
            }));
        }
        pause_point().await;
        // A later generation accepted while this ran reopened the shard, so
        // the record is no acknowledgement of the seal.
        Ok(self.lapsed_refusal(base.generation))
    }

    /// Append `record` at exactly `offset`, stamped with the seal's
    /// generation, and sync. False when the tail is elsewhere.
    async fn append_seal_record(
        &self,
        offset: Offset,
        base: &SwitchBase,
        record: crate::log::AppendRecord,
    ) -> Result<bool> {
        let writers = WrittenBy::Writers(vec![Epoch {
            generation: base.generation,
            start_offset: offset,
        }]);
        let Some(pending) = self
            .append_pending_at(offset, std::slice::from_ref(&record), writers)
            .await?
        else {
            return Ok(false);
        };
        self.commit(&pending).await?;
        // The acknowledgement and the report rest on N being on disk, not on
        // the commit's policy.
        self.sync().await?;
        if self.durable_offset() <= offset {
            return Err(invalid(format!(
                "{}: the seal's record at {offset} is not on disk after a sync",
                self.label()
            )));
        }
        Ok(true)
    }

    /// The promise to `generation` still holds: no later generation was
    /// accepted since it was read.
    fn promise_still(&self, generation: u64) -> bool {
        self.seal_promise()
            .is_some_and(|promise| promise.generation == generation)
    }

    fn lapsed_refusal(&self, generation: u64) -> std::result::Result<(), SealRefused> {
        if self.promise_still(generation) {
            Ok(())
        } else {
            Err(SealRefused::NoPromise)
        }
    }

    async fn holds_seal_record(&self, record: &LogRecord) -> Result<bool> {
        let held = self
            .read_range_with_trims(ReadRange {
                start: record.offset,
                max_bytes: 1,
            })
            .await?;
        Ok(held.first().is_some_and(|held| {
            held.offset == record.offset
                && held.mark == RecordMark::GenerationStart
                && held.payload == record.payload
        }))
    }

    async fn mark_claimed(&self, promise: SealPromise) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        felix_common::blocking::spawn(move || {
            let _persisted = inner.replica_persisted.lock();
            inner.check_open()?;
            inner.check_not_poisoned()?;
            let mut held = inner.switch_seal.lock();
            if *held != Some(promise) {
                return Err(invalid(format!(
                    "{}: the switch seal changed while it was claimed",
                    inner.label
                )));
            }
            let claimed = SealPromise {
                claimed: true,
                ..promise
            };
            inner.poison_on_sync_failure("the switch seal", store(&inner.dir, &claimed))?;
            *held = Some(claimed);
            Ok(())
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }
}

fn other_sealer(
    held: Option<SealPromise>,
    generation: u64,
    sealer: StorageId,
) -> Option<SealRefused> {
    held.filter(|promise| promise.generation == generation && promise.sealer != sealer)
        .map(|promise| SealRefused::PromisedOther {
            generation,
            sealer: promise.sealer,
        })
}

fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_micros() as u64)
}

fn not_on_commit<const N: usize>(logs: [&DiskLog; N]) -> Option<SealRefused> {
    logs.into_iter()
        .find(|log| log.config().fsync_mode != FsyncMode::OnCommit)
        .map(|log| SealRefused::NotOnCommit {
            label: log.label().to_string(),
        })
}

/// The promise in `dir`, if one was written. One that does not decode fails
/// the open: it cannot be rebuilt, and dropping it would let the copy take
/// records from another leader.
pub(in crate::disk_log) fn load(dir: &Path) -> Result<Option<SealPromise>> {
    match crate::fs::read(path_in(dir)) {
        Ok(bytes) => SealPromise::decode(&bytes).map(Some).ok_or_else(|| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{}: does not decode", path_in(dir).display()),
            ))
        }),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(StorageError::Io(err)),
    }
}

/// Written as the ballot is: a crash leaves the old promise or the new one.
fn store(dir: &Path, promise: &SealPromise) -> Result<()> {
    use std::io::Write;

    let path = path_in(dir);
    let temporary = path.with_extension("tmp");
    {
        let mut file = crate::fs::File::create(&temporary).map_err(StorageError::Io)?;
        file.write_all(&promise.encode())
            .map_err(StorageError::Io)?;
        crate::io::sync_all(&file)?;
    }
    crate::fs::rename(&temporary, &path).map_err(StorageError::Io)?;
    sync_dir(dir)?;
    Ok(())
}

fn path_in(dir: &Path) -> PathBuf {
    dir.join(FILE_NAME)
}

fn invalid(why: String) -> StorageError {
    StorageError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, why))
}

#[cfg(test)]
thread_local! {
    /// The step after which [`DiskLog::claim_seal`] stops as a crash would.
    static CRASH_AFTER: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn crash_point(step: u8) -> Result<()> {
    if CRASH_AFTER.with(|at| at.get()) == step {
        return Err(StorageError::Io(std::io::Error::other(format!(
            "crashed after step {step}"
        ))));
    }
    Ok(())
}

#[cfg(not(test))]
fn crash_point(_: u8) -> Result<()> {
    Ok(())
}

#[cfg(test)]
type Pause = (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);

#[cfg(test)]
thread_local! {
    /// Where a claim or an acknowledgement waits, once its record is on
    /// disk, for a test to accept a later generation: (reached, resume).
    static PAUSE: std::cell::RefCell<Option<Pause>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
async fn pause_point() {
    let pause = PAUSE.with(|pause| pause.borrow_mut().take());
    if let Some((reached, resume)) = pause {
        reached.notify_one();
        resume.notified().await;
    }
}

#[cfg(not(test))]
async fn pause_point() {}
