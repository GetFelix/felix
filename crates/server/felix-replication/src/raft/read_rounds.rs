//! One read-index round at a time per group, shared by every read that
//! arrived before it was asked.
//!
//! A read is confirmed by a round asked after the reader took its value. A
//! round already asked when a read arrives may predate that value, so the
//! read waits for the next round, which is asked when the current one ends
//! and serves every read that arrived in between. See
//! `docs/per-shard-raft.md`, "Reads".

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::watch;

/// The rounds of one group. `A` is what a round answers.
pub(crate) struct ReadRounds<A> {
    state: parking_lot::Mutex<State<A>>,
    /// Rounds asked, for tests and the read benchmark.
    asked: AtomicU64,
}

struct State<A> {
    /// A task is asking rounds. It ends once no read waits.
    running: bool,
    /// The round not asked yet, which every read that arrives joins.
    pending: Option<watch::Sender<Option<A>>>,
}

/// A read's place in a round asked after [`ReadRounds::enter`] returned.
pub(crate) struct Entered<A> {
    answer: watch::Receiver<Option<A>>,
}

impl<A> Default for ReadRounds<A> {
    fn default() -> Self {
        Self {
            state: parking_lot::Mutex::new(State {
                running: false,
                pending: None,
            }),
            asked: AtomicU64::new(0),
        }
    }
}

impl<A: Clone + Send + Sync + 'static> ReadRounds<A> {
    /// Join the next round to be asked. If no task is asking rounds, one
    /// starts on `runtime` and calls `round` once per round until no read
    /// is left waiting.
    pub(crate) fn enter<F, Fut>(
        self: &Arc<Self>,
        runtime: &tokio::runtime::Handle,
        round: F,
    ) -> Entered<A>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = A> + Send + 'static,
    {
        let mut state = self.state.lock();
        let pending = state.pending.get_or_insert_with(|| watch::channel(None).0);
        let answer = pending.subscribe();
        if !state.running {
            state.running = true;
            drop(state);
            // Made here, not in the task: a runtime that is shutting down
            // drops the task without polling it.
            let reset = Reset {
                rounds: Arc::clone(self),
                ended: false,
            };
            runtime.spawn(reset.run(round));
        }
        Entered { answer }
    }

    /// Rounds asked so far.
    #[cfg(test)]
    pub(crate) fn asked(&self) -> u64 {
        self.asked.load(Ordering::Relaxed)
    }
}

/// The task asking rounds. If it is dropped or panics mid-round, the reads
/// waiting are told nothing, and the next read starts a new task.
struct Reset<A> {
    rounds: Arc<ReadRounds<A>>,
    /// The task ended with no read waiting, and said so under the lock.
    ended: bool,
}

impl<A> Reset<A> {
    async fn run<F, Fut>(mut self, round: F)
    where
        F: Fn() -> Fut,
        Fut: Future<Output = A>,
    {
        let rounds = Arc::clone(&self.rounds);
        loop {
            // Taken under the lock, so every read that joined it arrived
            // before it is asked, and none joins a round no task will ask.
            let current = {
                let mut state = rounds.state.lock();
                match state.pending.take() {
                    Some(current) => current,
                    None => {
                        state.running = false;
                        self.ended = true;
                        return;
                    }
                }
            };
            // Every read that joined has given up. None can join now.
            if current.receiver_count() == 0 {
                continue;
            }
            rounds.asked.fetch_add(1, Ordering::Relaxed);
            let answer = round().await;
            current.send_replace(Some(answer));
        }
    }
}

impl<A: Clone> Entered<A> {
    /// The answer of the round this read joined. `None` if its task ended
    /// without one.
    pub(crate) async fn answer(mut self) -> Option<A> {
        let answer = self.answer.wait_for(Option::is_some).await.ok()?;
        answer.clone()
    }
}

impl<A> Drop for Reset<A> {
    fn drop(&mut self) {
        // Another task may be running by now.
        if self.ended {
            return;
        }
        let mut state = self.rounds.state.lock();
        state.running = false;
        state.pending = None;
    }
}

#[cfg(test)]
mod tests;
