//! Heartbeats for one broker, gathered into `RaftBeats` frames.
//!
//! Every group a broker leads heartbeats each of its followers, so at a few
//! thousand groups one RPC per heartbeat is most of what idle groups cost.
//! An append without entries to a broker that offered
//! [`PeerCapabilities::RAFT_BEATS`] waits here up to [`WINDOW`] for others
//! to the same broker, and they travel in one frame. Each caller still gets
//! the answer to its own request. See `docs/per-shard-raft.md`,
//! "Heartbeats".
//!
//! [`PeerCapabilities::RAFT_BEATS`]: felix_wire::internal::PeerCapabilities::RAFT_BEATS

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use felix_common::random::HashMap;
use felix_wire::internal::{InternalMessage, RaftCall};
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::super::network::{RaftReply, Refused};
use super::super::wire;
use super::super::wire::beats::MAX_BEATS;
use crate::peer::PeerRequester;

/// How long a heartbeat waits for others to the same broker. Small against
/// the 200 ms heartbeat and the 1 s shortest election timeout, and a lease
/// counts from before the wait, so the wait only shortens it.
pub(crate) const WINDOW: Duration = Duration::from_millis(1);

/// Most heartbeats waiting for one broker. Past it the broker is not keeping
/// up, and a heartbeat is refused as a full request budget refuses one.
const MAX_QUEUED: usize = 4 * MAX_BEATS;

/// What a heartbeat's caller gets: its answer, or `None` when the member
/// did not answer before the rest of its frame, and the caller sends it on
/// its own.
pub(super) type Answer = Option<Result<RaftReply, Refused>>;

/// The heartbeats waiting for each broker, and what has been sent.
#[derive(Default)]
pub(super) struct Beats {
    targets: Mutex<HashMap<Arc<str>, Target>>,
    sent: Sent,
}

/// Raft frames sent, for measuring what coalescing saves.
#[derive(Debug, Default)]
pub(super) struct Sent {
    /// `Raft` frames: one request each.
    pub(super) single: AtomicU64,
    /// `RaftBeats` frames.
    pub(super) frames: AtomicU64,
    /// Heartbeats those frames carried.
    pub(super) beats: AtomicU64,
}

/// One broker's queue. `sending` is set while its driver runs; the driver
/// removes the entry when the queue empties, so a new heartbeat starts one.
#[derive(Default)]
struct Target {
    queued: VecDeque<Waiting>,
    sending: bool,
}

struct Waiting {
    body: Bytes,
    deadline: Duration,
    at: Instant,
    addr: SocketAddr,
    answer: oneshot::Sender<Answer>,
}

impl Beats {
    pub(super) fn sent(&self) -> &Sent {
        &self.sent
    }
}

/// Send the encoded heartbeat `body` to `broker` with the next frame, and
/// wait for its answer.
pub(super) async fn send<R>(
    requester: &Arc<R>,
    beats: &Arc<Beats>,
    broker: Arc<str>,
    addr: SocketAddr,
    deadline: Duration,
    body: Bytes,
) -> Answer
where
    R: PeerRequester + Send + Sync + 'static,
{
    let (answer, answered) = oneshot::channel();
    {
        let mut targets = beats.targets.lock();
        let target = targets.entry(Arc::clone(&broker)).or_default();
        if target.queued.len() >= MAX_QUEUED {
            return Some(Err(Refused::Unreachable(format!(
                "broker {broker}: {MAX_QUEUED} heartbeats already wait for it"
            ))));
        }
        target.queued.push_back(Waiting {
            body,
            deadline,
            at: Instant::now(),
            addr,
            answer,
        });
        if !target.sending {
            target.sending = true;
            tokio::spawn(drive(
                Arc::clone(requester),
                Arc::clone(beats),
                Arc::clone(&broker),
            ));
        }
    }
    answered.await.unwrap_or_else(|_| {
        Some(Err(Refused::Unreachable(format!(
            "broker {broker}: the heartbeat was dropped before it went"
        ))))
    })
}

/// Sends `broker`'s frames one at a time until its queue is empty. One at a
/// time keeps a member's heartbeats in the order they were sent, and a
/// broker that is slow to answer holds up only its own heartbeats, which
/// gather into the next frame meanwhile.
async fn drive<R>(requester: Arc<R>, beats: Arc<Beats>, broker: Arc<str>)
where
    R: PeerRequester + Send + Sync + 'static,
{
    // On a panic, so the next heartbeat starts a driver again; the waiting
    // callers see their senders dropped.
    let _driving = Driving {
        beats: &beats,
        broker: &broker,
    };
    loop {
        let wait = {
            let mut targets = beats.targets.lock();
            let Some(target) = targets.get_mut(&broker) else {
                return;
            };
            target.queued.retain(|waiting| !waiting.answer.is_closed());
            match target.queued.front() {
                // Under the lock, so a heartbeat queued after this starts
                // its own driver.
                None => {
                    targets.remove(&broker);
                    return;
                }
                Some(_) if target.queued.len() >= MAX_BEATS => Duration::ZERO,
                Some(first) => (first.at + WINDOW).saturating_duration_since(Instant::now()),
            }
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        let frame: Vec<Waiting> = {
            let mut targets = beats.targets.lock();
            let Some(target) = targets.get_mut(&broker) else {
                return;
            };
            let take = target.queued.len().min(MAX_BEATS);
            target.queued.drain(..take).collect()
        };
        if !frame.is_empty() {
            send_frame(&*requester, &beats.sent, &broker, frame).await;
        }
    }
}

struct Driving<'a> {
    beats: &'a Beats,
    broker: &'a Arc<str>,
}

impl Drop for Driving<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.beats.targets.lock().remove(self.broker);
        }
    }
}

/// Send one frame and hand each caller its own answer. A frame that gets no
/// usable answer fails every heartbeat in it the way a lost `Raft` frame
/// fails its one request.
async fn send_frame<R>(requester: &R, sent: &Sent, broker: &Arc<str>, frame: Vec<Waiting>)
where
    R: PeerRequester + Send + Sync,
{
    sent.frames.fetch_add(1, Ordering::Relaxed);
    sent.beats.fetch_add(frame.len() as u64, Ordering::Relaxed);
    let deadline = frame.iter().map(|waiting| waiting.deadline).max();
    let deadline = deadline.unwrap_or_default();
    let bodies: Vec<Bytes> = frame.iter().map(|waiting| waiting.body.clone()).collect();
    let answers = async {
        let body = wire::beats::encode(deadline, &bodies).map_err(|err| err.to_string())?;
        let call = InternalMessage::RaftBeats(RaftCall {
            correlation_id: 0,
            body,
        });
        let request = requester.request(broker, frame[0].addr, call);
        match tokio::time::timeout(deadline, request).await {
            Ok(Ok(InternalMessage::RaftBeatsAnswer(answer))) => {
                wire::beats::decode_answers(answer.body, frame.len())
                    .map_err(|err| format!("its answer does not read: {err}"))
            }
            // Overloaded, most likely. Raft sends again later.
            Ok(Ok(other)) => Err(format!("it answered {:?}", other.kind())),
            Ok(Err(err)) => Err(err.to_string()),
            Err(_) => Err("the heartbeats were not answered in time".to_owned()),
        }
    };
    match answers.await {
        Ok(answers) => {
            for (waiting, answer) in frame.into_iter().zip(answers) {
                let answer = answer.map(|body| {
                    wire::decode_reply(body).unwrap_or_else(|err| {
                        Err(Refused::Unreachable(format!(
                            "broker {broker}: its answer does not read: {err}"
                        )))
                    })
                });
                let _ = waiting.answer.send(answer);
            }
        }
        Err(detail) => {
            for waiting in frame {
                let refused = Refused::Unreachable(format!("broker {broker}: {detail}"));
                let _ = waiting.answer.send(Some(Err(refused)));
            }
        }
    }
}

#[cfg(test)]
mod tests;
