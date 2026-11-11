//! openraft's storage suite, the cases the log can answer without a purge
//! or a state machine.
//!
//! The suite wants one type that is both the log and its reader, so
//! [`Store`] hands each call to the adapter. Cases that purge, apply or
//! snapshot wait for those parts.

use std::io;

use futures_util::Stream;
use openraft::storage::{
    EntryResponder, IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder,
    RaftStateMachine,
};
use openraft::testing::log::{StoreBuilder, Suite};
use openraft::type_config::TypeConfigExt;
use openraft::type_config::alias::{
    LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf,
};
use openraft::{OptionalSend, StorageError};
use tempfile::TempDir;

use super::{Codec, Types, open};
use crate::raft_log::{LogReader, RaftLog};

struct Store(RaftLog<Types, Codec>);

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
        self.0.purge(log_id).await
    }
}

/// The suite wants a state machine beside the log. None of the cases run
/// here use it.
struct NoStateMachine;

impl RaftSnapshotBuilder<Types> for NoStateMachine {
    type SnapshotData = ();

    async fn build_snapshot(&mut self) -> io::Result<SnapshotOf<Types, ()>> {
        Err(unsupported())
    }
}

impl RaftStateMachine<Types> for NoStateMachine {
    type SnapshotData = ();
    type SnapshotBuilder = NoStateMachine;

    async fn applied_state(
        &mut self,
    ) -> io::Result<(Option<LogIdOf<Types>>, StoredMembershipOf<Types>)> {
        Err(unsupported())
    }

    async fn apply<Strm>(&mut self, _entries: Strm) -> io::Result<()>
    where
        Strm: Stream<Item = io::Result<EntryResponder<Types>>> + Unpin + OptionalSend,
    {
        Err(unsupported())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        NoStateMachine
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMetaOf<Types>,
        _snapshot: (),
    ) -> io::Result<()> {
        Err(unsupported())
    }

    async fn get_current_snapshot(&mut self) -> io::Result<Option<SnapshotOf<Types, ()>>> {
        Err(unsupported())
    }
}

fn unsupported() -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, "no state machine here")
}

struct Builder;

impl StoreBuilder<Types, Store, NoStateMachine, TempDir> for Builder {
    async fn build(&self) -> Result<(TempDir, Store, NoStateMachine), StorageError<Types>> {
        let dir = tempfile::tempdir().expect("dir");
        let raft = open(&dir);
        Ok((dir, Store(raft), NoStateMachine))
    }
}

type Cases = Suite<Types, Store, NoStateMachine, Builder, TempDir>;

macro_rules! run {
    ($($case:ident),+ $(,)?) => {$(
        let (_dir, store, sm) = Builder.build().await.expect("build");
        Cases::$case(store, sm)
            .await
            .unwrap_or_else(|err| panic!("{}: {err}", stringify!($case)));
    )+};
}

#[test]
fn the_log_passes_the_storage_suite_cases_that_need_no_purge_or_state_machine() {
    Types::run(async {
        run!(
            last_membership_in_log_initial,
            last_membership_in_log_multi_step,
            save_vote,
            get_log_entries,
            limited_get_log_entries,
            leader_bounded_stream,
            entries_stream,
            log_reader_reads_new_entries,
            initial_logs,
            delete_logs_after_11,
            delete_logs_after_5,
            delete_logs_after_0,
            delete_logs_after_none,
        );
    });
}
