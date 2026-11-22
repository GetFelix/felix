//! The streams a group's commits are followed for, so its state machine
//! can serve a leader's readers itself as it publishes a commit. A leader
//! has no log pass to run, only held batches to release and readers to
//! wake, and doing that inline saves its follow task a poll per commit.
//! See [`super::RaftEngine::follow_commits`].

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use felix_broker::StreamHandle;
use tokio::sync::Notify;

/// Shared by a group's state machine, which serves the entries, and the
/// follow tasks, which add and remove them.
#[derive(Clone, Default)]
pub(crate) struct Followed(Arc<Inner>);

#[derive(Default)]
struct Inner {
    next: AtomicU64,
    entries: parking_lot::Mutex<Vec<Entry>>,
}

struct Entry {
    id: u64,
    stream: StreamHandle,
    /// Woken when the entry is dropped for a closed stream, or the state
    /// machine is gone.
    wake: Arc<Notify>,
}

/// One follow task's entry, removed when it is dropped.
pub(crate) struct Following {
    followed: Followed,
    id: u64,
    wake: Arc<Notify>,
}

impl Followed {
    /// Serve `stream` on each commit while this broker leads it.
    pub(crate) fn follow(&self, stream: StreamHandle) -> Following {
        let id = self.0.next.fetch_add(1, Ordering::Relaxed);
        let wake = Arc::new(Notify::new());
        self.0.entries.lock().push(Entry {
            id,
            stream,
            wake: Arc::clone(&wake),
        });
        Following {
            followed: self.clone(),
            id,
            wake,
        }
    }

    /// The commit index moved: release what it now covers and wake the
    /// readers at the bound, for each stream this broker leads. A closed
    /// stream's entry is dropped and its task woken to end, since a parked
    /// task does not see commits.
    pub(crate) fn serve(&self) {
        self.0.entries.lock().retain(|entry| {
            if !entry.stream.is_active() {
                entry.wake.notify_one();
                return false;
            }
            if entry.stream.leads_engine_log() {
                entry.stream.release_committed();
                entry.stream.notify_appended();
            }
            true
        });
    }

    /// The state machine is gone: wake every task so it sees the commits
    /// close.
    pub(crate) fn end(&self) {
        for entry in self.0.entries.lock().iter() {
            entry.wake.notify_one();
        }
    }
}

impl Following {
    /// Resolves once the entry is dropped for a closed stream or the state
    /// machine is gone. A wake that came first is kept.
    pub(crate) async fn woken(&self) {
        self.wake.notified().await;
    }
}

impl Drop for Following {
    fn drop(&mut self) {
        self.followed
            .0
            .entries
            .lock()
            .retain(|entry| entry.id != self.id);
    }
}

/// Held by the state machine, after the commits it publishes: once those
/// close, the follow tasks are woken to see it.
pub(crate) struct EndOnDrop(pub(crate) Followed);

impl Drop for EndOnDrop {
    fn drop(&mut self) {
        self.0.end();
    }
}
