//! Appends waiting for a flush, and the one task that waits for them.
//!
//! A flush covers a prefix of the log, so the task waits once for the newest
//! append it holds and then answers every one before it, in order.

use std::io;
use std::sync::{Arc, OnceLock, Weak};

use tokio::sync::{mpsc, watch};

use super::Shared;
use super::writer::io_error;
use crate::disk_log::PendingAppend;

/// What an append's callback is given.
pub(super) type Flushed = Box<dyn FnOnce(io::Result<()>) + Send>;

pub(super) struct FlushQueue {
    /// Appends whose callback has not fired.
    waiting: watch::Sender<usize>,
    /// To the task, which starts with the first append that has to wait.
    sender: OnceLock<mpsc::UnboundedSender<Waiting>>,
}

struct Waiting {
    pending: PendingAppend,
    flushed: Flushed,
}

impl Default for FlushQueue {
    fn default() -> Self {
        Self {
            waiting: watch::channel(0).0,
            sender: OnceLock::new(),
        }
    }
}

impl FlushQueue {
    /// No callback is outstanding, so one fired now is in order.
    pub(super) fn is_empty(&self) -> bool {
        *self.waiting.borrow() == 0
    }

    /// Returns once every callback queued so far has fired.
    pub(super) async fn drained(&self) {
        let mut waiting = self.waiting.subscribe();
        // The sender is `self.waiting`, so this cannot fail.
        let _ = waiting.wait_for(|waiting| *waiting == 0).await;
    }

    /// Fire `flushed` once the log's fsync policy is met for `pending`,
    /// after every callback queued before it.
    pub(super) fn push<K>(shared: &Arc<Shared<K>>, pending: PendingAppend, flushed: Flushed)
    where
        K: Send + Sync + 'static,
    {
        let queue = &shared.flushes;
        let sender = queue.sender.get_or_init(|| {
            let (sender, receiver) = mpsc::unbounded_channel();
            // Weak, so the task does not keep the log open once its last
            // user is gone.
            tokio::spawn(run(Arc::downgrade(shared), receiver));
            sender
        });
        queue.waiting.send_modify(|waiting| *waiting += 1);
        if let Err(mpsc::error::SendError(unsent)) = sender.send(Waiting { pending, flushed }) {
            queue.settle(1);
            (unsent.flushed)(Err(io::Error::other("the log's flush task has stopped")));
        }
    }

    fn settle(&self, fired: usize) {
        self.waiting
            .send_modify(|waiting| *waiting = waiting.saturating_sub(fired));
    }
}

async fn run<K>(shared: Weak<Shared<K>>, receiver: mpsc::UnboundedReceiver<Waiting>)
where
    K: Send + Sync + 'static,
{
    let mut task = Task {
        shared,
        receiver,
        batch: Vec::new(),
    };
    while task.receiver.recv_many(&mut task.batch, usize::MAX).await > 0 {
        let Some(shared) = task.shared.upgrade() else {
            return;
        };
        // Newest first, so the oldest is popped first.
        task.batch.reverse();
        let mut covered = Some(shared.log.commit(&task.batch[0].pending).await);
        while let Some(waiting) = task.batch.pop() {
            let flushed = if task.batch.is_empty() {
                covered.take().expect("taken once, by the newest")
            } else if matches!(covered, Some(Ok(()))) {
                Ok(())
            } else {
                // A flush that failed for the newest may still have covered
                // this one.
                shared.log.commit(&waiting.pending).await
            };
            // On drop, so a callback that panics is still counted.
            let _settle = Settle(&shared.flushes);
            (waiting.flushed)(flushed.map_err(io_error));
        }
    }
}

/// The task's state. Dropped when the task ends, however it ends.
struct Task<K> {
    shared: Weak<Shared<K>>,
    receiver: mpsc::UnboundedReceiver<Waiting>,
    /// Taken off the channel and not yet answered.
    batch: Vec<Waiting>,
}

impl<K> Drop for Task<K> {
    /// Fail what was never answered: the task panicked, or its runtime shut
    /// down. Otherwise those callbacks would never fire and a truncation
    /// would wait for them for ever.
    fn drop(&mut self) {
        self.receiver.close();
        let mut unanswered: Vec<_> = self.batch.drain(..).rev().collect();
        while let Ok(waiting) = self.receiver.try_recv() {
            unanswered.push(waiting);
        }
        let shared = self.shared.upgrade();
        for waiting in unanswered {
            let _settle = shared.as_ref().map(|shared| Settle(&shared.flushes));
            (waiting.flushed)(Err(io::Error::other("the log's flush task has stopped")));
        }
    }
}

struct Settle<'a>(&'a FlushQueue);

impl Drop for Settle<'_> {
    fn drop(&mut self) {
        self.0.settle(1);
    }
}
