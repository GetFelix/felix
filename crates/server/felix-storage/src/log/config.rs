//! How a log is configured: segment sizing, the fsync policy, retention and
//! recovery. [`LogConfig::validate`] rejects settings that cannot work, once,
//! when the log is opened.

use std::time::Duration;

use crate::{Result, StorageError};

/// Settings for one log. The defaults suit a broker; tests shrink segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogConfig {
    pub segment_size_bytes: u64,
    pub index_spacing_bytes: u64,
    pub fsync_mode: FsyncMode,
    /// Ceiling on records returned by a single `read_range`, on top of the
    /// caller's byte budget. Payload bytes alone do not bound a response made of
    /// empty records, and an unbounded response is a memory hazard on a path
    /// that will later serve follower catch-up.
    pub max_records_per_read: usize,
    /// Reserve a segment's blocks when it is created. Keeps allocation off the
    /// append path; disable on filesystems where reservations are expensive or
    /// where thin provisioning makes them counter-productive.
    pub preallocate_segments: bool,
    /// Truncate a *complete* trailing record whose checksum does not verify.
    ///
    /// Off by default, and the default is the conservative one. A torn write
    /// and bit rot on an acknowledged record produce identical bytes: a
    /// full-length record that fails its checksum. Recovery cannot tell them
    /// apart, so it refuses to guess and fails to start, naming the segment and
    /// position. Provably *incomplete* writes - a record cut short by end of
    /// file - are still repaired automatically, because nothing could have
    /// acknowledged them.
    ///
    /// Turn this on only where losing the last record is preferable to a broker
    /// that will not start, which is a defensible trade under `FsyncMode::None`
    /// and is not one under `OnCommit`.
    pub repair_checksum_tail: bool,
    /// Percentage of `segment_size_bytes` at which a rollover is started in the
    /// background. **100 disables it, which is the default.**
    ///
    /// Rolling a segment costs several fsyncs, and doing that work when an
    /// append discovers the segment is full puts those flushes on the publish
    /// path. Starting the roll early — building the replacement off the lock
    /// while the current segment still has room — is the obvious way to keep
    /// appends off them, and it is implemented here in full.
    ///
    /// It is off by default because it was measured, and it does not help.
    /// Over five 200k-record runs at 16 MiB segments and 16 publishers, median
    /// p999 was 3.98ms with it disabled and 7.97ms with it enabled, with a much
    /// heavier upper tail (worst run 19.0ms against 5.0ms). At 256 MiB and at
    /// 1 MiB it was neutral. It was never better anywhere.
    ///
    /// The reason is `F_FULLFSYNC`, which is what durability on macOS actually
    /// requires: it flushes the whole device write cache, so it does not
    /// overlap with concurrent writes. Moving a rollover's flushes off the
    /// append lock does not hide them; it just stops confining them, so instead
    /// of blocking behind one lock they land on whichever appends happen to be
    /// in flight. The cost is the same and the tail is worse.
    ///
    /// It is kept, and kept configurable, because that reasoning is specific to
    /// a flush that reaches the device. On a platform where `fdatasync` returns
    /// once the write is in the kernel, the overlap is real and the trade may
    /// invert — but that has not been measured here, so it is not the default.
    ///
    /// Enabling it makes `segment_size_bytes` a target rather than a ceiling;
    /// see `max_overshoot_percent`.
    pub rollover_threshold_percent: u8,
    /// How far past `segment_size_bytes` a segment may grow while its
    /// replacement is being prepared, as a percentage.
    ///
    /// This is the price of keeping rollover off the append path, and it is not
    /// optional slack: preparing a replacement costs two fsyncs — milliseconds —
    /// while the appends that fill the remaining space cost microseconds. With
    /// no room to overshoot, the inline roll always wins that race, every roll
    /// blocks exactly as it did before, and the prepared segment is discarded
    /// unused. Measured, that combination is *slower* than not preparing at all.
    ///
    /// It is bounded rather than open-ended because `segment_size_bytes` also
    /// bounds recovery time: the newest segment is the one scanned in full at
    /// startup. Once a segment reaches this bound its appends roll inline and
    /// pay the latency, which is the right trade against an unboundedly large
    /// segment to re-scan after a crash.
    ///
    /// The overshoot a segment actually takes is roughly `write_rate x
    /// prepare_time`, so it shrinks as segments grow: at 256 MiB and 300 MB/s
    /// an 8ms preparation overshoots by about 1%. Small segments are the case
    /// this cannot rescue — see the note on `segment_size_bytes`.
    pub max_overshoot_percent: u8,
    /// Delete whole sealed segments from the head once the log exceeds this
    /// many bytes. `None` (the default) never deletes anything.
    ///
    /// The active segment is never deleted, so a log settles at roughly
    /// `retention_bytes` and never below `segment_size_bytes` regardless of how
    /// small this is set.
    pub retention_bytes: Option<u64>,
    /// Delete whole sealed segments whose newest record is older than this.
    /// `None` (the default) never deletes anything.
    ///
    /// Age comes from the records' own `timestamp_micros`, not file mtime, so a
    /// restore or a copy does not reset it. The *newest* record in a segment
    /// decides, which is the conservative end: nothing younger than the bound
    /// is ever deleted.
    pub retention_age: Option<Duration>,
    /// How often retention is evaluated. Ignored unless a retention bound is
    /// set.
    ///
    /// Retention is bulk file deletion and must not land on a publish, so it
    /// runs on its own timer rather than being triggered by an append.
    pub retention_check_interval: Duration,
    /// Checksum every record of every segment at open time.
    ///
    /// Off by default: startup would otherwise cost one full pass over all data
    /// on disk. The active segment is always fully scanned regardless, and every
    /// read verifies the records it returns, so bit rot in cold data is still
    /// caught — just when it is read rather than at boot. Turn this on where a
    /// slow, loud startup is preferable to a late surprise.
    pub verify_all_on_open: bool,
    /// Most sealed segments kept open at once, per storage root.
    ///
    /// A sealed segment's file and sparse index are opened by the first read
    /// that reaches it and closed again when this many others have been used
    /// since. The index is about 1/256 of its segment at the default spacing,
    /// so this bounds both descriptors and index memory by what is being read
    /// rather than by what is retained. The active segment of every open log
    /// is always open and does not count.
    pub max_open_sealed_segments: usize,
    /// Copy sealed segments to an object store. `None` (the default) copies
    /// nothing.
    ///
    /// Copies are made on the retention timer. With this set, retention
    /// deletes a segment only once its copy is verified and recorded in the
    /// shard's offload manifest, so a segment past the bounds is never lost
    /// for want of a copy; it waits for one. Without a retention bound
    /// nothing is deleted and the store holds a second copy. Reads below the
    /// local head still report `Trimmed`. See `docs/durable-storage.md`.
    ///
    /// Compaction's trimming honours it too, but the broker sets it only for
    /// stream logs.
    pub offload: Option<OffloadTarget>,
    /// Whether retention is held at the commit offset from the moment the log
    /// opens, before retention first runs. The default keeps what the log's
    /// replica state recorded. See `DiskLog::hold_retention_at_commit`.
    pub retention_hold: RetentionHold,
}

/// What a log does at open with the retention hold its replica state recorded.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RetentionHold {
    /// Keep the recorded hold, as replication last set it.
    #[default]
    AsSaved,
    /// Hold, for a log its broker knows is replicated under `Quorum`.
    Held,
    /// Lift it: for a broker that does not replicate, where nothing would
    /// ever advance the commit offset, or a log not under `Quorum`.
    Lifted,
}

/// Where offloaded segments go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OffloadTarget {
    /// A directory, through `object_store`'s local filesystem backend. Each
    /// object is fsynced, with its directories, before it is recorded.
    LocalDir(std::path::PathBuf),
}

/// The bounds retention deletes a log's oldest segments by. Either, both or
/// neither may be set; neither keeps everything.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Retention {
    /// See [`LogConfig::retention_bytes`].
    pub bytes: Option<u64>,
    /// See [`LogConfig::retention_age`].
    pub age: Option<Duration>,
}

impl Retention {
    /// These bounds, with each one left unset taken from `fallback`.
    pub fn or(self, fallback: Retention) -> Retention {
        Retention {
            bytes: self.bytes.or(fallback.bytes),
            age: self.age.or(fallback.age),
        }
    }

    pub fn is_set(&self) -> bool {
        self.bytes.is_some() || self.age.is_some()
    }
}

impl LogConfig {
    /// The retention bounds this configuration sets.
    pub fn retention(&self) -> Retention {
        Retention {
            bytes: self.retention_bytes,
            age: self.retention_age,
        }
    }

    /// Whether `retention` can be enforced under this configuration.
    pub(crate) fn check_retention(&self, retention: Retention) -> Result<()> {
        // Zero is not "retain nothing" — the active segment is never deleted, so
        // it would be a bound the log can never satisfy, re-evaluated forever.
        if retention.bytes == Some(0) {
            return Err(StorageError::InvalidConfig(
                "retention_bytes must be greater than zero; omit it to disable retention",
            ));
        }
        if retention.age == Some(Duration::ZERO) {
            return Err(StorageError::InvalidConfig(
                "retention_age must be greater than zero; omit it to disable retention",
            ));
        }
        if (retention.is_set() || self.offload.is_some()) && self.retention_check_interval.is_zero()
        {
            return Err(StorageError::InvalidConfig(
                "retention_check_interval must be greater than zero",
            ));
        }
        Ok(())
    }

    /// Reject configurations that cannot produce a working log.
    ///
    /// Called once when a log is opened rather than on the append path, so a
    /// misconfiguration fails at startup instead of at the first publish.
    pub fn validate(&self) -> Result<()> {
        // A segment must have room for its header plus at least one record, or
        // rollover would loop without ever making progress.
        let minimum = crate::segment::SEGMENT_HEADER_LEN + crate::segment::RECORD_HEADER_LEN;
        if self.segment_size_bytes < minimum {
            return Err(StorageError::InvalidConfig(
                "segment_size_bytes is too small to hold a single record",
            ));
        }
        if self.index_spacing_bytes == 0 {
            return Err(StorageError::InvalidConfig(
                "index_spacing_bytes must be greater than zero",
            ));
        }
        if self.max_records_per_read == 0 {
            return Err(StorageError::InvalidConfig(
                "max_records_per_read must be greater than zero",
            ));
        }
        self.check_retention(self.retention())?;
        if self.max_open_sealed_segments == 0 {
            return Err(StorageError::InvalidConfig(
                "max_open_sealed_segments must be greater than zero",
            ));
        }
        if let FsyncMode::Periodic { interval } = self.fsync_mode
            && interval.is_zero()
        {
            // A zero interval is not "sync always" — it is a busy loop. Callers
            // who want per-commit durability must say `OnCommit`.
            return Err(StorageError::InvalidConfig(
                "periodic fsync interval must be greater than zero",
            ));
        }
        Ok(())
    }

    /// Longest window during which an acknowledged record may not be durable.
    ///
    /// `None` means unbounded: with `FsyncMode::None` the log makes no promise
    /// beyond what the operating system decides to do.
    pub fn durability_window(&self) -> Option<Duration> {
        match self.fsync_mode {
            FsyncMode::None => None,
            FsyncMode::Periodic { interval } => Some(interval),
            FsyncMode::OnCommit => Some(Duration::ZERO),
        }
    }

    /// The most a segment reserves ahead of its writes: the whole segment
    /// when preallocation is on, nothing when it is off. A segment starts with
    /// a small part of it and grows into it (`crate::segment::reservation`).
    pub(crate) fn reserve_limit_bytes(&self) -> u64 {
        if self.preallocate_segments {
            self.segment_size_bytes
        } else {
            0
        }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            segment_size_bytes: 256 * 1024 * 1024,
            index_spacing_bytes: 4 * 1024,
            fsync_mode: FsyncMode::Periodic {
                interval: Duration::from_millis(250),
            },
            max_records_per_read: 10_000,
            preallocate_segments: true,
            rollover_threshold_percent: 100,
            max_overshoot_percent: 100,
            repair_checksum_tail: false,
            verify_all_on_open: false,
            retention_bytes: None,
            retention_age: None,
            retention_check_interval: Duration::from_secs(60),
            max_open_sealed_segments: 256,
            offload: None,
            retention_hold: RetentionHold::AsSaved,
        }
    }
}

/// When appended bytes are flushed to stable storage, and so when an append
/// may be acknowledged. See `docs/durable-storage.md` for what each promises.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncMode {
    None,
    Periodic { interval: Duration },
    OnCommit,
}

#[cfg(test)]
mod tests;
