//! openraft's storage suite, all of it, over the adapter and a state
//! machine that keeps its snapshot in a [`SnapshotStore`].
//!
//! The suite wants one type that is both the log and its reader, so
//! [`Store`] hands each call to the adapter.
//!
//! [`SnapshotStore`]: crate::raft_log::SnapshotStore

use std::io;

use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::testing::log::{StoreBuilder, Suite};
use openraft::type_config::TypeConfigExt;
use openraft::type_config::alias::{LogIdOf, VoteOf};
use openraft::{OptionalSend, StorageError};
use tempfile::TempDir;

use super::machine::Machine;
use super::{Codec, Types, open};
use crate::raft_log::{LogReader, RaftLog};

/// The second field, when set, is stored before each purge.
pub(crate) struct Store(
    pub(crate) RaftLog<Types, Codec>,
    pub(crate) Option<super::machine::Unstored>,
);

impl RaftLogReader<Types> for Store {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> io::Result<Vec<super::TestEntry>>
    where
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        self.0.reader().try_get_log_entries(range).await
    }

    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> io::Result<Vec<super::TestEntry>> {
        self.0.reader().limited_get_log_entries(start, end).await
    }

    async fn read_vote(&mut self) -> io::Result<Option<VoteOf<Types>>> {
        self.0.reader().read_vote().await
    }
}

impl RaftLogStorage<Types> for Store {
    type LogReader = LogReader<Types, Codec>;

    async fn get_log_state(&mut self) -> io::Result<LogState<Types>> {
        self.0.log_state().await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.0.reader()
    }

    async fn save_vote(&mut self, vote: &VoteOf<Types>) -> io::Result<()> {
        self.0.save_vote(vote).await
    }

    async fn save_committed(&mut self, committed: Option<LogIdOf<Types>>) -> io::Result<()> {
        self.0.save_committed(committed).await
    }

    async fn read_committed(&mut self) -> io::Result<Option<LogIdOf<Types>>> {
        self.0.read_committed().await
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<Types>) -> io::Result<()>
    where
        I: IntoIterator<Item = super::TestEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        self.0.append(entries, callback).await
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<Types>>) -> io::Result<()> {
        self.0.truncate_after(last_log_id).await
    }

    async fn purge(&mut self, log_id: LogIdOf<Types>) -> io::Result<()> {
        if let Some(unstored) = &self.1 {
            unstored.store().await?;
        }
        self.0.purge(log_id).await
    }
}

struct Builder;

impl StoreBuilder<Types, Store, Machine, TempDir> for Builder {
    async fn build(&self) -> Result<(TempDir, Store, Machine), StorageError<Types>> {
        let dir = tempfile::tempdir().expect("dir");
        let raft = open(&dir);
        let machine = Machine::open(dir.path());
        Ok((dir, Store(raft, None), machine))
    }
}

#[test]
fn the_log_passes_the_whole_storage_suite() {
    Types::run(async {
        Suite::test_all(Builder).await.expect("suite");
    });
}
