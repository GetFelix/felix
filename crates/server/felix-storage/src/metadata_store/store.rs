//! A member's metadata store, opened whole: the member file, the Raft log
//! and the snapshot, with the rules that tie them together checked before
//! anything is handed out.

use std::io;
use std::ops::Deref;
use std::path::PathBuf;

use super::{LOG_DIR, MemberFile, STORE_DIR, Started, decide_start};
use crate::log::{FsyncMode, LogConfig};
use crate::raft_log::{
    CommittedLeaderIdOf, EntryCodec, LogReader, LogTypes, RaftLog, SnapshotStore,
};
use crate::{DiskLog, StorageError};

/// One member's metadata store. The only way to open one.
///
/// Holds the store's lock until it and every clone of its member file are
/// dropped, and, until [`Self::shutdown`] succeeds, every clone of its log
/// and its snapshot store. Shut the store down before dropping it.
pub struct MetadataStore<C, K> {
    member: MemberFile,
    log: RaftLog<C, K>,
    snapshots: SnapshotStore,
    raft_state: bool,
}

impl<C, K> MetadataStore<C, K>
where
    C: LogTypes,
    CommittedLeaderIdOf<C>: From<u64> + Deref<Target = u64>,
    K: EntryCodec<C>,
{
    /// Open the store in `data_dir` for the cluster `cluster_id`, making it
    /// if the directory holds none. `label` names the log in errors.
    ///
    /// The start is decided first ([`decide_start`]), so a directory that
    /// holds an unconverted `raft.redb` is refused with nothing written.
    /// Then the store's lock is taken, and the log is opened as a Raft log
    /// that is fsynced on commit; there is no setting for either.
    ///
    /// Refused, with the store left as found:
    /// - a log purged past the stored snapshot, which has lost entries;
    /// - a saved committed id whose entry is of another term: the log is
    ///   not the one that id was saved for.
    ///
    /// Repaired:
    /// - a snapshot that covers the log's last entry empties the log, so it
    ///   starts right after the snapshot, as the purge that ends a snapshot
    ///   install would have left it;
    /// - a log with no vote, no entry and no purged id beside a snapshot
    ///   has lost its vote, so the member's vote is withheld, on disk
    ///   before this returns.
    ///
    /// A log with no vote, no entry and no purged id and no snapshot is
    /// reported by [`Self::has_raft_state`]. Purges follow
    /// [`RaftLog::purge_hides_past`].
    pub async fn open(
        data_dir: impl Into<PathBuf>,
        cluster_id: &str,
        codec: K,
        label: impl Into<String>,
    ) -> io::Result<Self> {
        Self::open_configured(
            data_dir.into(),
            cluster_id,
            codec,
            label.into(),
            LogConfig::default(),
        )
        .await
    }

    /// [`Self::open`] with the log's sizes taken from `config`. What makes
    /// it a Raft log, and when it is fsynced, is not the caller's to set.
    pub(super) async fn open_configured(
        data_dir: PathBuf,
        cluster_id: &str,
        codec: K,
        label: String,
        config: LogConfig,
    ) -> io::Result<Self> {
        // Before the member file is opened: that would build a store in a
        // directory this refuses.
        decide_start(&data_dir)?;
        let store_dir = data_dir.join(STORE_DIR);
        let member = MemberFile::open(&store_dir, cluster_id)?;
        let config = LogConfig {
            raft_log: true,
            fsync_mode: FsyncMode::OnCommit,
            retention_bytes: None,
            retention_age: None,
            ..config
        };
        // Fails on a saved committed id whose entry is of another term.
        // The log keeps the lock: its clones outlive this store, and the
        // lock must not go while any of them can write.
        let disk = DiskLog::open_refusing_other_commit(
            store_dir.join(LOG_DIR),
            label,
            config,
            std::sync::Arc::new(member.clone()),
        )
        .map_err(io_error)?;
        let opened = async {
            let log = RaftLog::new(disk.clone(), codec)?;
            let snapshots = SnapshotStore::open(&store_dir)?;
            snapshots.keep_alive(std::sync::Arc::new(member.clone()));
            log.purge_hides_past(&snapshots)?;
            // Before the log is emptied under the snapshot, which gives it
            // a purged id.
            let blank = log.is_blank().await?;
            let covered = snapshots.covers().is_some();
            if blank && covered {
                member.set_vote_withheld(true).await?;
            }
            log.start_after_snapshot(&snapshots).await?;
            Ok::<_, io::Error>((log, snapshots, !blank || covered))
        }
        .await;
        match opened {
            Ok((log, snapshots, raft_state)) => Ok(Self {
                member,
                log,
                snapshots,
                raft_state,
            }),
            Err(err) => {
                let _ = disk.shutdown().await;
                Err(err)
            }
        }
    }

    /// The store's member file.
    pub fn member_file(&self) -> &MemberFile {
        &self.member
    }

    /// The Raft log: openraft's `RaftLogStorage`. Every clone is the same
    /// log.
    pub fn log(&self) -> RaftLog<C, K> {
        self.log.clone()
    }

    /// A reader of the log: openraft's `RaftLogReader`.
    pub fn reader(&self) -> LogReader<C, K> {
        self.log.reader()
    }

    /// Where the state machine keeps its snapshot. The log measures its
    /// purges against this store, so the state machine must write through
    /// a clone of it and never open a second one on the directory.
    pub fn snapshots(&self) -> &SnapshotStore {
        &self.snapshots
    }

    /// Whether the open found any Raft state: a vote, an entry, a purged
    /// id or a snapshot. Without one the member has never voted from this
    /// store, or its log was emptied, and nothing here can tell which. The
    /// caller treats it as a member with an empty store: one that joins a
    /// running cluster starts with its vote withheld.
    pub fn has_raft_state(&self) -> bool {
        self.raft_state
    }

    /// Set the started mark, on disk before this returns. See
    /// [`MemberFile::mark_started`].
    pub async fn mark_started(&self) -> io::Result<Started> {
        self.member.mark_started().await
    }

    /// Close the snapshot store and the log, flushing the log, then give up
    /// the log's hold on the lock, so the store can be opened again once the
    /// store and every clone of its member file are dropped. From here on an
    /// append or install through any clone fails, so nothing writes into a
    /// store another process may have opened. A shutdown that fails keeps
    /// the log's hold, and the lock stays until the last clone of the log
    /// is gone.
    pub async fn shutdown(self) -> io::Result<()> {
        self.snapshots.close().await?;
        let disk = self.log.disk_log();
        disk.close().await.map_err(io_error)?;
        disk.release_keep_alive();
        Ok(())
    }
}

/// A storage error as an I/O error, keeping an I/O error's kind.
fn io_error(err: StorageError) -> io::Error {
    match err {
        StorageError::Io(err) => err,
        other => io::Error::other(other),
    }
}
