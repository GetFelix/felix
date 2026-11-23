//! Heartbeats sent together: every group's heartbeat for members on one
//! broker that is handed off within a short window goes in one message,
//! and the answer holds one result per heartbeat, in order. Appends that
//! carry entries, and every other message, go on their own at once. See
//! `docs/per-shard-raft.md`, "Heartbeats sent together".

use std::future::Future;
use std::sync::Arc;
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
}

/// Send `batch` and hand each heartbeat its answer.
fn flush(inner: &Arc<dyn RaftTransport>, wait: Duration, batch: Batch) {
    let inner = Arc::clone(inner);
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

fn is_beat(request: &RaftRequest) -> bool {
    matches!(&request.body, Request::Append(rpc) if rpc.entries.is_empty())
}

#[async_trait]
impl RaftTransport for Beats {
    async fn send(&self, request: RaftRequest) -> Answer {
        if !is_beat(&request) {
            return self.inner.send(request).await;
        }
        let broker = Arc::clone(&request.to().broker);
        let (waiter, answer) = oneshot::channel();
        let (full, timer) = {
            let mut queued = self.queued.lock();
            let Queued { batches, next } = &mut *queued;
            let batch = batches.entry(Arc::clone(&broker)).or_insert_with(|| {
                *next += 1;
                Batch {
                    id: *next,
                    beats: Vec::new(),
                    waiters: Vec::new(),
                }
            });
            let first = batch.beats.is_empty();
            batch.beats.push(request);
            batch.waiters.push(waiter);
            match batch.beats.len() >= MAX_BEATS {
                true => (batches.remove(&broker), None),
                false => (None, first.then_some(batch.id)),
            }
        };
        if let Some(full) = full {
            flush(&self.inner, self.window, full);
        } else if let Some(id) = timer {
            let queued = Arc::clone(&self.queued);
            let inner = Arc::clone(&self.inner);
            let window = self.window;
            tokio::spawn(async move {
                tokio::time::sleep(window).await;
                let batch = {
                    let mut queued = queued.lock();
                    match queued.batches.get(&broker) {
                        Some(batch) if batch.id == id => queued.batches.remove(&broker),
                        // Sent when it filled up; a newer batch has its own timer.
                        _ => None,
                    }
                };
                if let Some(batch) = batch {
                    flush(&inner, window, batch);
                }
            });
        }
        answer.await.unwrap_or_else(|_| {
            Err(Refused::Unreachable(
                "the heartbeat was dropped before it was sent".into(),
            ))
        })
    }

    async fn send_beats(&self, beats: Vec<RaftRequest>, wait: Duration) -> Vec<Answer> {
        self.inner.send_beats(beats, wait).await
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
