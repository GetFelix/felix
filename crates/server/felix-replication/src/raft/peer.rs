//! Raft messages over the broker transport: a [`RaftTransport`] that sends
//! through the peer pool, and the handler a peer listener answers them with.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use felix_wire::internal::{InternalMessage, PeerCapabilities, RaftCall, RaftLayouts};

use std::time::{Duration, Instant};

use felix_common::random::HashMap;
use parking_lot::Mutex;

use super::network::{RaftReply, RaftRequest, RaftTransport, Refused, send_each};
use super::runtime::GroupRuntime;
use super::wire;
use crate::peer::{PeerRequester, RaftHandler};

/// Where each broker's internal listener is.
pub trait PeerAddresses: Send + Sync + 'static {
    /// `None` for a broker this one does not know, which is not sent to.
    fn address(&self, broker: &str) -> Option<SocketAddr>;
}

impl<F> PeerAddresses for F
where
    F: Fn(&str) -> Option<SocketAddr> + Send + Sync + 'static,
{
    fn address(&self, broker: &str) -> Option<SocketAddr> {
        self(broker)
    }
}

/// Sends Raft messages to peer brokers over their internal listeners.
///
/// One per broker, shared by every group. A message goes only to a broker
/// that offered [`PeerCapabilities::RAFT`] in its handshake and reads a
/// layout this build writes; for any other the answer is
/// [`Refused::Unreachable`] with the reason, and nothing is sent.
pub struct PeerTransport<R> {
    requester: R,
    addresses: Arc<dyn PeerAddresses>,
    /// When each broker's layout refusal was last logged, so a peer that is
    /// refused on every heartbeat is logged about once a minute.
    logged: Mutex<HashMap<Arc<str>, Instant>>,
}

/// Sends refused because the peer reads no layout this build writes, by
/// `reason`. A refused heartbeat batch counts once.
pub(crate) const PEER_REFUSALS_TOTAL: &str = "felix_broker_raft_peer_refusals_total";
const LOG_EVERY: Duration = Duration::from_secs(60);

impl<R> PeerTransport<R> {
    /// `requester` is normally the broker's `Arc<PeerPool>`.
    pub fn new(requester: R, addresses: Arc<dyn PeerAddresses>) -> Self {
        Self {
            requester,
            addresses,
            logged: Mutex::new(HashMap::default()),
        }
    }

    /// Where `broker` listens and what it offered, once it is known to take
    /// Raft messages in a layout this build writes. Otherwise why not, and
    /// nothing may be sent.
    async fn reach(&self, broker: &Arc<str>) -> Result<(SocketAddr, PeerCapabilities), String>
    where
        R: PeerRequester,
    {
        let addr = self
            .addresses
            .address(broker)
            .ok_or_else(|| "no address is known".to_owned())?;
        // Checked here as well as in the pool, so a requester that does not
        // check cannot send one either.
        let offer = self
            .requester
            .raft_offer(broker, addr)
            .await
            .map_err(|err| err.to_string())?;
        if !offer.capabilities.contains(PeerCapabilities::RAFT) {
            return Err("it has not offered Raft".into());
        }
        if wire::choose_layout(wire::LAYOUTS, offer.layouts).is_none() {
            let stated = offer
                .layouts
                .map_or("no layouts".to_owned(), |l| format!("layouts {l}"));
            let reason = format!(
                "its handshake states Raft {stated} and this build writes {}",
                wire::LAYOUTS
            );
            metrics::counter!(PEER_REFUSALS_TOTAL, "reason" => "layout").increment(1);
            let now = Instant::now();
            let mut logged = self.logged.lock();
            if logged
                .get(broker)
                .is_none_or(|at| now.duration_since(*at) >= LOG_EVERY)
            {
                logged.insert(Arc::clone(broker), now);
                tracing::warn!(peer = %broker, "Raft messages to this peer are refused: {reason}");
            }
            return Err(reason);
        }
        Ok((addr, offer.capabilities))
    }
}

#[async_trait]
impl<R> RaftTransport for PeerTransport<R>
where
    R: PeerRequester + Send + Sync + 'static,
{
    async fn send(&self, mut request: RaftRequest) -> Result<RaftReply, Refused> {
        let broker = Arc::clone(&request.to().broker);
        let unreachable =
            |detail: String| Refused::Unreachable(format!("broker {broker}: {detail}"));
        let (addr, offered) = self.reach(&broker).await.map_err(unreachable)?;
        // A build that predates base pulls would refuse the kind; never
        // asking keeps its refusals out of its logs.
        if matches!(request.body, super::network::Request::BasePull { .. })
            && !offered.contains(PeerCapabilities::RAFT_BASE_PULL)
        {
            return Err(unreachable("it has not offered base pulls".into()));
        }
        wire::fit(&mut request);
        let body = wire::encode_request(&request).map_err(|err| unreachable(err.to_string()))?;
        let call = InternalMessage::Raft(RaftCall {
            correlation_id: 0,
            body,
        });
        match self.requester.request(&broker, addr, call).await {
            Ok(InternalMessage::RaftAnswer(answer)) => wire::decode_reply(answer.body)
                .map_err(|err| unreachable(format!("its answer does not read: {err}")))?,
            // Overloaded, or a refusal from a build that does not know the
            // kind. Raft sends again later either way.
            Ok(other) => Err(unreachable(format!("it answered {:?}", other.kind()))),
            Err(err) => Err(unreachable(err.to_string())),
        }
    }

    /// One message for all of them to a broker that offered
    /// [`PeerCapabilities::RAFT_BEATS`]; to any other, and for a single
    /// heartbeat, each goes as it always did.
    async fn send_beats(
        &self,
        mut beats: Vec<RaftRequest>,
        wait: Duration,
    ) -> Vec<Result<RaftReply, Refused>> {
        let count = beats.len();
        if count <= 1 {
            let Some(beat) = beats.pop() else {
                return Vec::new();
            };
            return vec![self.send(beat).await];
        }
        let broker = Arc::clone(&beats[0].to().broker);
        let all = |detail: String| -> Vec<Result<RaftReply, Refused>> {
            (0..count)
                .map(|_| Err(Refused::Unreachable(format!("broker {broker}: {detail}"))))
                .collect()
        };
        let (addr, offered) = match self.reach(&broker).await {
            Ok(reached) => reached,
            Err(detail) => return all(detail),
        };
        if !offered.contains(PeerCapabilities::RAFT_BEATS) {
            return send_each(self, beats).await;
        }
        let body = match wire::encode_beats(&beats, wait) {
            Ok(body) => body,
            Err(err) => return all(err.to_string()),
        };
        let call = InternalMessage::Raft(RaftCall {
            correlation_id: 0,
            body,
        });
        match self.requester.request(&broker, addr, call).await {
            Ok(InternalMessage::RaftAnswer(answer)) => {
                match wire::decode_beats_reply(answer.body, count) {
                    Ok(answers) => answers,
                    Err(err) => all(format!("its answer does not read: {err}")),
                }
            }
            Ok(other) => all(format!("it answered {:?}", other.kind())),
            Err(err) => all(err.to_string()),
        }
    }
}

#[async_trait]
impl RaftHandler for GroupRuntime {
    async fn answer(&self, from: Option<Arc<str>>, body: Bytes) -> Bytes {
        if wire::is_beats(&body) {
            return self.answer_beats(from, body).await;
        }
        let reply = match (wire::decode_request(body), from) {
            (Err(err), _) => Err(Refused::Unreachable(format!(
                "the message does not read: {err}"
            ))),
            (Ok(_), None) => Err(Refused::Unreachable(
                "the connection has not said which broker it is".to_owned(),
            )),
            (Ok(request), Some(from)) => self.handle_from(&from, request).await,
        };
        wire::encode_reply(&reply)
    }

    fn layouts(&self) -> RaftLayouts {
        wire::LAYOUTS
    }
}

impl GroupRuntime {
    /// A `BEATS` body: each heartbeat answered as on its own, in order.
    async fn answer_beats(&self, from: Option<Arc<str>>, body: Bytes) -> Bytes {
        let refused = |reason: String| wire::encode_reply(&Err(Refused::Unreachable(reason)));
        let (beats, wait) = match wire::decode_beats(body) {
            Ok(read) => read,
            Err(err) => return refused(format!("the message does not read: {err}")),
        };
        let Some(from) = from else {
            return refused("the connection has not said which broker it is".to_owned());
        };
        let began = std::time::Instant::now();
        let size = beats.len();
        let done: parking_lot::Mutex<Vec<(String, u64)>> = parking_lot::Mutex::new(Vec::new());
        let answers = super::beats::answer_each(beats, wait, |beat| {
            let label = beat.group.to_string();
            let done = &done;
            let fut = self.handle_from(&from, beat);
            async move {
                let answer = fut.await;
                done.lock()
                    .push((label, began.elapsed().as_millis() as u64));
                answer
            }
        })
        .await;
        let total = began.elapsed();
        if total > Duration::from_millis(300) {
            let done = done.lock();
            let lease = done
                .iter()
                .find(|(label, _)| label.contains("/leases/"))
                .map(|(_, ms)| *ms);
            let slowest = done.iter().map(|(_, ms)| *ms).max();
            let missing = size - done.len();
            tracing::warn!(from = ?from, size, total_ms = total.as_millis() as u64, wait_ms = wait.as_millis() as u64, answered = done.len(), unanswered = missing, slowest_answer_ms = ?slowest, lease_answer_ms = ?lease, "DIAG3032 follower slow heartbeat batch");
        }
        wire::encode_beats_reply(&answers)
    }
}

#[cfg(test)]
mod tests;
