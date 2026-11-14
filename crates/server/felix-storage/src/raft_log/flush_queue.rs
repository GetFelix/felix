//! Appends waiting for a flush, and the one task that waits for them.
//!
//! A flush covers a prefix of the log, so the task waits once for the newest
//! append it holds and then answers every one before it, in order.

use std::io;
use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use super::LogPoint;
use super::writer::io_error;
use crate::disk_log::PendingAppend;
use crate::raft_copy::CopyMarks;
use crate::{DiskLog, StorageError};

/// What an append's callback is given.
pub(super) type Flushed = Box<dyn FnOnce(io::Result<()>) + Send>;

pub(super) struct FlushQueue {
    /// Appends whose callback has not fired.
    waiting: Arc<watch::Sender<usize>>,
    /// To the task, which starts with the first append that has to wait.
    sender: parking_lot::Mutex<Option<mpsc::UnboundedSender<Waiting>>>,
}

struct Waiting {
    pending: PendingAppend,
    flushed: Flushed,
    /// The exact log mark to sync before `flushed` fires: the marks and the
    /// append's last entry.
    mark: Option<(CopyMarks, LogPoint)>,
}

impl Default for FlushQueue {
    fn default() -> Self {
        Self {
            waiting: Arc::new(watch::channel(0).0),
            sender: parking_lot::Mutex::new(None),
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

    /// Fire `flushed` once `log`'s fsync policy is met for `pending`, and
    /// `mark` is synced, after every callback queued before it.
    pub(super) fn push(
        &self,
        log: &DiskLog,
        pending: PendingAppend,
        flushed: Flushed,
        mark: Option<(CopyMarks, LogPoint)>,
    ) {
        self.waiting.send_modify(|waiting| *waiting += 1);
        let mut sender = self.sender.lock();
        // A task that stopped (a callback panicked, or its runtime shut
        // down) has failed what it held. Nothing on disk is in doubt, so
        // the next append starts another task rather than poisoning the
        // log. Its answers may come before the old task's failures, which
        // is still true: a flush covers a prefix.
        let sender = match &mut *sender {
            Some(sender) if !sender.is_closed() => sender,
            slot => slot.insert(Task::spawn(log.clone(), Arc::clone(&self.waiting))),
        };
        if let Err(mpsc::error::SendError(unsent)) = sender.send(Waiting {
            pending,
            flushed,
            mark,
        }) {
            settle(&self.waiting);
            (unsent.flushed)(Err(stopped()));
        }
    }
}

/// The task's state. Dropped when the task ends, however it ends, including
/// before its first poll.
struct Task {
    /// Held so an append still queued when the last user of the log goes is
    /// answered with its real flush. The task ends once the queue is gone
    /// and everything on it is answered.
    log: DiskLog,
    waiting: Arc<watch::Sender<usize>>,
    receiver: mpsc::UnboundedReceiver<Waiting>,
    /// Taken off the channel and not yet answered.
    batch: Vec<Waiting>,
}

impl Task {
    fn spawn(log: DiskLog, waiting: Arc<watch::Sender<usize>>) -> mpsc::UnboundedSender<Waiting> {
        let (sender, receiver) = mpsc::unbounded_channel();
        // Built before the spawn, so a task dropped unpolled still fails
        // what was sent to it.
        let task = Self {
            log,
            waiting,
            receiver,
            batch: Vec::new(),
        };
        tokio::spawn(task.run());
        sender
    }

    async fn run(mut self) {
        while self.receiver.recv_many(&mut self.batch, usize::MAX).await > 0 {
            // Newest first, so the oldest is popped first.
            self.batch.reverse();
            let probe_start = std::time::Instant::now();
            crate::stage_probe::FLUSH_ROUND_APPENDS_N.value(self.batch.len() as u64);
            let mut covered = Some(self.log.commit(&self.batch[0].pending).await);
            crate::stage_probe::FSYNC.since(probe_start);
            // The highest entry marked this round. The newest's mark covers
            // every append before it.
            let mut marked: Option<LogPoint> = None;
            while let Some(waiting) = self.batch.pop() {
                let mut flushed = if self.batch.is_empty() {
                    covered.take().expect("taken once, by the newest")
                } else if matches!(covered, Some(Ok(()))) {
                    Ok(())
                } else {
                    // A flush that failed for the newest may still have
                    // covered this one.
                    self.log.commit(&waiting.pending).await
                };
                if flushed.is_ok()
                    && let Some((marks, last)) = &waiting.mark
                    && marked.is_none_or(|marked| marked.index < last.index)
                {
                    // Once the newest is on disk so is everything before
                    // it, and one write covers the round.
                    let newest = self.batch.first().and_then(|newest| newest.mark.as_ref());
                    let point = match (&covered, newest) {
                        (Some(Ok(())), Some((_, newest))) => *newest,
                        _ => *last,
                    };
                    match marks.log(Some(point)).await {
                        Ok(()) => marked = Some(point),
                        Err(err) => flushed = Err(StorageError::Io(err)),
                    }
                }
                // On drop, so a callback that panics is still counted.
                let _settle = Settle(&self.waiting);
                (waiting.flushed)(flushed.map_err(io_error));
            }
        }
    }
}

impl Drop for Task {
    /// Fail what was never answered: the task panicked, or its runtime shut
    /// down. Otherwise those callbacks would never fire and a truncation
    /// would wait for them for ever.
    fn drop(&mut self) {
        self.receiver.close();
        let mut unanswered: Vec<_> = self.batch.drain(..).rev().collect();
        while let Ok(waiting) = self.receiver.try_recv() {
            unanswered.push(waiting);
        }
        for waiting in unanswered {
            let _settle = Settle(&self.waiting);
            (waiting.flushed)(Err(stopped()));
        }
    }
}

fn stopped() -> io::Error {
    io::Error::other("the log's flush task has stopped")
}

fn settle(waiting: &watch::Sender<usize>) {
    waiting.send_modify(|waiting| *waiting = waiting.saturating_sub(1));
}

struct Settle<'a>(&'a watch::Sender<usize>);

impl Drop for Settle<'_> {
    fn drop(&mut self) {
        settle(self.0);
    }
}
