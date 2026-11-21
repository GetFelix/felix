//! Raft messages over the broker transport: a [`RaftTransport`] that sends
//! through the peer pool, and the handler a peer listener answers them with.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use felix_wire::internal::{InternalMessage, PeerCapabilities, RaftCall};

use felix_common::random::HashMap;
use std::sync::atomic::Ordering;

use super::network::{RaftReply, RaftRequest, RaftTransport, Refused, Request};
use super::runtime::GroupRuntime;
use super::types::{GroupId, MemberId};
use super::wire;
use crate::peer::{PeerRequester, RaftHandler};
use beats::Beats;

mod beats;

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
/// that offered [`PeerCapabilities::RAFT`] in its handshake; for any other
/// the answer is [`Refused::Unreachable`] and nothing is sent. Heartbeats to
/// a broker that offered [`PeerCapabilities::RAFT_BEATS`] travel together
/// ([`beats`]); every other message, and every message to an older broker,
/// goes in its own frame.
pub struct PeerTransport<R> {
    requester: Arc<R>,
    addresses: Arc<dyn PeerAddresses>,
    beats: Arc<Beats>,
    coalesce: bool,
}

impl<R> PeerTransport<R> {
    /// `requester` is normally the broker's `Arc<PeerPool>`.
    pub fn new(requester: R, addresses: Arc<dyn PeerAddresses>) -> Self {
        Self {
            requester: Arc::new(requester),
            addresses,
            beats: Arc::default(),
            coalesce: true,
        }
    }

    /// Every heartbeat in its own frame, as to a broker without
    /// [`PeerCapabilities::RAFT_BEATS`]. For measuring what coalescing saves.
    #[doc(hidden)]
    pub fn without_coalescing(mut self) -> Self {
        self.coalesce = false;
        self
    }

    /// Frames sent so far: `Raft` frames, `RaftBeats` frames, and the
    /// heartbeats those carried. For measurements.
    #[doc(hidden)]
    pub fn frames_sent(&self) -> (u64, u64, u64) {
        let sent = self.beats.sent();
        (
            sent.single.load(Ordering::Relaxed),
            sent.frames.load(Ordering::Relaxed),
            sent.beats.load(Ordering::Relaxed),
        )
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
        let addr = self
            .addresses
            .address(&broker)
            .ok_or_else(|| unreachable("no address is known".into()))?;
        // Checked here as well as in the pool, so a requester that does not
        // check cannot send one either.
        let offered = self
            .requester
            .capabilities(&broker, addr)
            .await
            .map_err(|err| unreachable(err.to_string()))?;
        if !offered.contains(PeerCapabilities::RAFT) {
            return Err(unreachable("it has not offered Raft".into()));
        }
        wire::fit(&mut request);
        let body = wire::encode_request(&request).map_err(|err| unreachable(err.to_string()))?;
        // Only heartbeats: an append with entries keeps its own frame on a
        // replication stream, and openraft waits for each answer before it
        // sends that member the next append.
        if self.coalesce && offered.contains(PeerCapabilities::RAFT_BEATS) && is_beat(&request) {
            let beat = body.clone();
            let deadline = request.deadline;
            let broker = Arc::clone(&broker);
            // `None` when it was late in its frame: then it goes on its own
            // with the rest of its deadline, like any other request.
            if let Some(answer) =
                beats::send(&self.requester, &self.beats, broker, addr, deadline, beat).await
            {
                return answer;
            }
        }
        self.beats.sent().single.fetch_add(1, Ordering::Relaxed);
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
}

#[async_trait]
impl RaftHandler for GroupRuntime {
    async fn answer(&self, from: Option<Arc<str>>, body: Bytes) -> Bytes {
        let request = wire::decode_request(body)
            .map_err(|err| Refused::Unreachable(format!("the message does not read: {err}")));
        wire::encode_reply(&self.answer_request(from.as_deref(), request).await)
    }

    /// Each heartbeat is admitted and answered as it would be alone. Those
    /// for one member run in the order sent; the rest run at once. The
    /// frame is answered at half its deadline at the latest, so a member
    /// that is slow to answer holds up only its own heartbeat, which its
    /// sender then sends on its own.
    async fn answer_beats(&self, from: Option<Arc<str>>, body: Bytes) -> Bytes {
        let Ok((deadline, beats)) = wire::beats::decode(body) else {
            // No answers: the sender fails every heartbeat in the frame.
            return wire::beats::encode_answers(&[]);
        };
        let cut = tokio::time::Instant::now() + deadline / 2;
        let count = beats.len();
        let mut lanes: HashMap<(GroupId, MemberId), Vec<(usize, Result<RaftRequest, Refused>)>> =
            HashMap::default();
        let mut refused = Vec::new();
        for (at, body) in beats.into_iter().enumerate() {
            match wire::decode_request(body) {
                Ok(request) if is_beat(&request) => lanes
                    .entry((request.group.clone(), request.to.clone()))
                    .or_default()
                    .push((at, Ok(request))),
                Ok(_) => refused.push((at, "only a heartbeat travels with others".to_owned())),
                Err(err) => refused.push((at, format!("the message does not read: {err}"))),
            }
        }
        let from = from.as_deref();
        let lanes = lanes.into_values().map(|lane| async move {
            let mut answered = Vec::with_capacity(lane.len());
            for (at, request) in lane {
                let answer = tokio::time::timeout_at(cut, self.answer_request(from, request)).await;
                let Ok(answer) = answer else {
                    // Late, and so is every later one for this member.
                    break;
                };
                answered.push((at, wire::encode_reply(&answer)));
            }
            answered
        });
        let mut answers = vec![None; count];
        for (at, answer) in futures::future::join_all(lanes).await.into_iter().flatten() {
            answers[at] = Some(answer);
        }
        for (at, detail) in refused {
            answers[at] = Some(wire::encode_reply(&Err(Refused::Unreachable(detail))));
        }
        wire::beats::encode_answers(&answers)
    }
}

impl GroupRuntime {
    /// One request off the wire, admitted for the broker the connection is
    /// bound to and then handled.
    async fn answer_request(
        &self,
        from: Option<&str>,
        request: Result<RaftRequest, Refused>,
    ) -> Result<RaftReply, Refused> {
        let request = request?;
        match (impersonation(from, &request), from) {
            (Some(refused), _) => Err(refused),
            (None, Some(from)) => self.handle_from(from, request).await,
            // `impersonation` refuses an unbound connection already.
            (None, None) => Err(Refused::Unreachable(
                "the connection has not said which broker it is".to_owned(),
            )),
        }
    }
}

fn is_beat(request: &RaftRequest) -> bool {
    matches!(&request.body, Request::Append(append) if append.entries.is_empty())
}

/// Why `request` is refused when the connection it came on is bound to
/// `from`, or `None` when it may be answered. A vote, append, snapshot,
/// transfer or written position names the member that sent it, and openraft
/// acts on that name; a peer may only use its own. A read index names no
/// sender: the group answers it to any member that asks.
pub(crate) fn impersonation(from: Option<&str>, request: &RaftRequest) -> Option<Refused> {
    let named = sender(&request.body);
    let Some(from) = from else {
        return Some(Refused::Unreachable(
            "the connection has not said which broker it is".to_owned(),
        ));
    };
    match named {
        Some(named) if *named.broker != *from => {
            tracing::warn!(
                from,
                named = %named,
                group = %request.group(),
                "refusing a Raft message sent in another broker's name",
            );
            Some(Refused::Unreachable(format!(
                "broker {from} sent a message as {named}"
            )))
        }
        _ => None,
    }
}

/// The member `body` says sent it. A read index names none.
pub(crate) fn sender(body: &super::network::Request) -> Option<&super::types::MemberId> {
    use super::network::Request;
    use openraft::vote::RaftLeaderId;
    match body {
        Request::Append(append) => Some(append.vote.leader_id().node_id()),
        Request::Vote(vote) | Request::PreVote(vote) => Some(vote.vote.leader_id().node_id()),
        Request::Snapshot { vote, .. } => Some(vote.leader_id().node_id()),
        Request::TransferLeader(transfer) => Some(transfer.from_leader().leader_id().node_id()),
        Request::Written { from, .. } => Some(from),
        Request::ReadIndex => None,
    }
}

#[cfg(test)]
mod tests;
