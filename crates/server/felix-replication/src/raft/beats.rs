//! Heartbeats sent together: every group's heartbeat for members on one
//! broker that is handed off within a short window goes in one message,
//! and the answer holds one result per heartbeat, in order. Appends that
//! carry entries, and every other message, go on their own at once. See
//! `docs/per-shard-raft.md`, "Heartbeats sent together".

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use felix_common::random::HashMap;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use tokio::sync::oneshot;

use super::network::{Addressee, RaftReply, RaftRequest, RaftTransport, Refused, Request};
use super::wire::MAX_BEATS;

type Answer = Result<RaftReply, Refused>;

/// A [`RaftTransport`] that holds each heartbeat for up to its window and
/// sends the ones for each broker together.
pub(crate) struct Beats {
    inner: Arc<dyn RaftTransport>,
    window: Duration,
    queued: Arc<parking_lot::Mutex<Queued>>,
}

#[derive(Default)]
struct Queued {
    batches: HashMap<Arc<str>, Batch>,
    /// Numbers the batches, so a window's timer sends only its own.
    next: u64,
}

struct Batch {
    id: u64,
    beats: Vec<RaftRequest>,
    waiters: Vec<oneshot::Sender<Answer>>,
    held: Vec<Option<Arc<Held>>>,
    /// Set by [`take`]: the first heartbeat, its window's opener's, is
    /// still in it.
    opener: bool,
}

impl Batch {
    /// Whether anything in it is still to go.
    fn live(&self) -> bool {
        self.held
            .iter()
            .any(|held| held.as_ref().is_none_or(|held| !held.withdrawn()))
    }
}

/// A commit-only append the client may take back while it waits in a
/// window, for the next append to carry its commit instead (see
/// `raft/network/client.rs`). Once its batch is taken to be sent, it can
/// no longer be taken back, so one append to a follower is never in flight
/// beside another.
pub(crate) struct Held(AtomicU8);

const UNSEEN: u8 = 0;
const QUEUED: u8 = 1;
const SENT: u8 = 2;
const WITHDRAWN: u8 = 3;

impl Held {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self(AtomicU8::new(UNSEEN)))
    }

    /// Take the append back. True only while it waits in a window; the
    /// caller then drops its send.
    pub(crate) fn withdraw(&self) -> bool {
        self.0
            .compare_exchange(QUEUED, WITHDRAWN, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn queued(&self) {
        let _ = self
            .0
            .compare_exchange(UNSEEN, QUEUED, Ordering::AcqRel, Ordering::Acquire);
    }

    /// Whether it goes: false if it was taken back.
    fn sending(&self) -> bool {
        self.0
            .compare_exchange(QUEUED, SENT, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn withdrawn(&self) -> bool {
        self.0.load(Ordering::Acquire) == WITHDRAWN
    }
}

tokio::task_local! {
    static HELD: Arc<Held>;
}

/// Runs `send` with `held` as the ticket for the heartbeat it sends.
pub(crate) async fn holding<F: Future>(held: Arc<Held>, send: F) -> F::Output {
    HELD.scope(held, send).await
}

impl Beats {
    /// `inner`, with heartbeats held for up to `window`. A zero window
    /// sends each on its own, as `inner` does.
    pub(crate) fn wrap(inner: Arc<dyn RaftTransport>, window: Duration) -> Arc<dyn RaftTransport> {
        if window.is_zero() {
            return inner;
        }
        Arc::new(Self {
            inner,
            window,
            queued: Arc::default(),
        })
    }

    /// The opener of a window waits it out on its own task. A batch of one
    /// it sends itself; one that others joined goes on a task of its own.
    async fn open(&self, broker: Arc<str>, id: u64, answer: oneshot::Receiver<Answer>) -> Answer {
        let mut window = Window {
            queued: Arc::clone(&self.queued),
            inner: Arc::clone(&self.inner),
            wait: self.window,
            broker,
            id,
            until: tokio::time::Instant::now() + self.window,
            open: true,
        };
        #[cfg(test)]
        counts::timer();
        let mut answer = answer;
        tokio::select! {
            biased;
            // It filled up and was sent before the window ended.
            answered = &mut answer => {
                window.open = false;
                return answered.unwrap_or_else(|_| Err(dropped()));
            }
            () = tokio::time::sleep_until(window.until) => {}
        }
        // Closed only once taken, so a drop before this leaves the guard
        // to send the others.
        let batch = take(&mut self.queued.lock(), &window.broker, Some(id));
        window.open = false;
        match batch {
            // Only the opener's own is left. If it was taken back, the one
            // left is another's and must get its own answer: flush below.
            Some(batch) if batch.opener && batch.beats.len() == 1 => {
                let mut answers = self.inner.send_beats(batch.beats, self.window).await;
                return answers.pop().unwrap_or_else(|| {
                    Err(Refused::Unreachable(
                        "the transport answered no heartbeat".into(),
                    ))
                });
            }
            Some(batch) => flush(&self.inner, self.window, batch),
            // It filled up and was sent.
            None => {}
        }
        receive(answer).await
    }
}

/// The window its opener waits out. Dropped while open, the opener stopped
/// waiting, and a task waits out the rest for the others.
struct Window {
    queued: Arc<parking_lot::Mutex<Queued>>,
    inner: Arc<dyn RaftTransport>,
    wait: Duration,
    broker: Arc<str>,
    id: u64,
    until: tokio::time::Instant,
    open: bool,
}

impl Drop for Window {
    fn drop(&mut self) {
        if !self.open {
            return;
        }
        let live = {
            let mut queued = self.queued.lock();
            match queued.batches.get(&self.broker) {
                Some(batch) if batch.id == self.id && batch.live() => true,
                Some(batch) if batch.id == self.id => {
                    queued.batches.remove(&self.broker);
                    false
                }
                _ => false,
            }
        };
        if !live {
            return;
        }
        let (queued, inner, wait) = (Arc::clone(&self.queued), Arc::clone(&self.inner), self.wait);
        let (broker, id, until) = (Arc::clone(&self.broker), self.id, self.until);
        #[cfg(test)]
        counts::spawn();
        tokio::spawn(async move {
            tokio::time::sleep_until(until).await;
            let batch = take(&mut queued.lock(), &broker, Some(id));
            if let Some(batch) = batch {
                flush(&inner, wait, batch);
            }
        });
    }
}

/// Removes `broker`'s batch, if it is batch `id`, and marks what is in it
/// as sent, leaving out what was taken back. `None` when nothing is left.
fn take(queued: &mut Queued, broker: &Arc<str>, id: Option<u64>) -> Option<Batch> {
    if id.is_some_and(|id| {
        queued
            .batches
            .get(broker)
            .is_none_or(|batch| batch.id != id)
    }) {
        // Sent when it filled up; a newer batch has its own timer.
        return None;
    }
    let batch = queued.batches.remove(broker)?;
    let mut kept = Batch {
        id: batch.id,
        beats: Vec::with_capacity(batch.beats.len()),
        waiters: Vec::with_capacity(batch.beats.len()),
        held: Vec::new(),
        opener: false,
    };
    let entries = batch.beats.into_iter().zip(batch.waiters).zip(batch.held);
    for (at, ((beat, waiter), held)) in entries.enumerate() {
        if held.as_ref().is_none_or(|held| held.sending()) {
            kept.opener |= at == 0;
            kept.beats.push(beat);
            kept.waiters.push(waiter);
        }
    }
    (!kept.beats.is_empty()).then_some(kept)
}

/// Send `batch` and hand each heartbeat its answer.
fn flush(inner: &Arc<dyn RaftTransport>, wait: Duration, batch: Batch) {
    let inner = Arc::clone(inner);
    #[cfg(test)]
    counts::spawn();
    // On its own task, so a caller that stops waiting does not take the
    // other groups' heartbeats with it.
    tokio::spawn(async move {
        let count = batch.beats.len();
        let mut answers = inner.send_beats(batch.beats, wait).await.into_iter();
        for waiter in batch.waiters {
            let answer = answers.next().unwrap_or_else(|| {
                Err(Refused::Unreachable(format!(
                    "the transport answered fewer than {count} heartbeats"
                )))
            });
            let _ = waiter.send(answer);
        }
    });
}

async fn receive(answer: oneshot::Receiver<Answer>) -> Answer {
    answer.await.unwrap_or_else(|_| Err(dropped()))
}

fn dropped() -> Refused {
    Refused::Unreachable("the heartbeat was dropped before it was sent".into())
}

fn is_beat(request: &RaftRequest) -> bool {
    matches!(&request.body, Request::Append(rpc) if rpc.entries.is_empty())
}

#[async_trait]
impl RaftTransport for Beats {
    async fn send(&self, request: RaftRequest) -> Answer {
        if !is_beat(&request) {
            return self.inner.send(request).await;
        }
        let held = HELD.try_with(Arc::clone).ok();
        let broker = Arc::clone(&request.to().broker);
        let (waiter, answer) = oneshot::channel();
        let (full, opened) = {
            let mut queued = self.queued.lock();
            let Queued { batches, next } = &mut *queued;
            let batch = batches.entry(Arc::clone(&broker)).or_insert_with(|| {
                *next += 1;
                Batch {
                    id: *next,
                    beats: Vec::new(),
                    waiters: Vec::new(),
                    held: Vec::new(),
                    opener: false,
                }
            });
            let first = batch.beats.is_empty();
            if let Some(held) = &held {
                held.queued();
            }
            batch.beats.push(request);
            batch.waiters.push(waiter);
            batch.held.push(held);
            match batch.beats.len() >= MAX_BEATS {
                true => (take(&mut queued, &broker, None), None),
                false => (None, first.then_some(batch.id)),
            }
        };
        if let Some(full) = full {
            flush(&self.inner, self.window, full);
        } else if let Some(id) = opened {
            return self.open(broker, id, answer).await;
        }
        receive(answer).await
    }

    async fn send_beats(&self, beats: Vec<RaftRequest>, wait: Duration) -> Vec<Answer> {
        self.inner.send_beats(beats, wait).await
    }
}

/// Timers and tasks the windows cost, counted on the test's thread.
#[cfg(test)]
pub(crate) mod counts {
    use std::cell::Cell;

    thread_local! {
        static TIMERS: Cell<u64> = const { Cell::new(0) };
        static SPAWNS: Cell<u64> = const { Cell::new(0) };
    }

    pub(crate) fn timer() {
        TIMERS.set(TIMERS.get() + 1);
    }

    pub(crate) fn spawn() {
        SPAWNS.set(SPAWNS.get() + 1);
    }

    /// `(timers, spawns)` so far on this thread.
    pub(crate) fn read() -> (u64, u64) {
        (TIMERS.get(), SPAWNS.get())
    }
}

/// Answer each heartbeat with `handle`, all at once, in their order. Once
/// one has answered, a member that stays silent for `wait` past the latest
/// answer is answered [`Refused::Slow`], so it holds the others up by no
/// more than that. Until the first answer every member is as slow, and all
/// wait: the broker is slow, not one member.
pub(crate) async fn answer_each<F, Fut>(
    beats: Vec<RaftRequest>,
    wait: Duration,
    handle: F,
) -> Vec<Answer>
where
    F: Fn(RaftRequest) -> Fut,
    Fut: Future<Output = Answer>,
{
    let addressees: Vec<Addressee> = beats
        .iter()
        .map(|beat| Addressee {
            group: beat.group.clone(),
            to: beat.to.clone(),
        })
        .collect();
    let mut answers: Vec<Option<Answer>> = (0..beats.len()).map(|_| None).collect();
    let mut pending: FuturesUnordered<_> = beats
        .into_iter()
        .enumerate()
        .map(|(at, beat)| handle(beat).map(move |answer| (at, answer)))
        .collect();
    if let Some((at, answer)) = pending.next().await {
        answers[at] = Some(answer);
    }
    while let Ok(Some((at, answer))) = tokio::time::timeout(wait, pending.next()).await {
        answers[at] = Some(answer);
    }
    answers
        .into_iter()
        .zip(addressees)
        .map(|(answer, to)| answer.unwrap_or_else(|| Err(Refused::Slow(Box::new(to)))))
        .collect()
}

#[cfg(test)]
mod tests;
