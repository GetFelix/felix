//! An in-memory peer network.
//!
//! [`SimNet`] stands in for the QUIC peer transport between simulated brokers.
//! Each running node gets a [`SimPeer`], the `PeerRequester` its replication,
//! suspicion and election tasks use. A request goes straight to the target
//! node's `PeerRequestHandler` on a task of its own, so it runs to the end
//! even if the caller gives up on it, as it would on a real broker.
//!
//! What it keeps from the real transport:
//!
//! - Every message is encoded and decoded with the peer codec in both
//!   directions, so a field the codec drops is dropped here too.
//! - The caller waits as long as the pool would ([`request_timeout`]) and gets
//!   the pool's errors: `Unsupported` for a capability the target did not
//!   offer, `Timeout` for an answer that never came.
//! - The handler hears who asked only when the target keeps ballots.
//!
//! Faults are per directed link ([`Link`]): a partition refuses at once, as
//! the pool's partition injection does; a drop loses the request or its
//! answer and leaves the caller to time out; a delay holds a message in
//! flight. Drops are drawn from the seed the net was built with.
//!
//! Each [`SimNet::start`] is a new incarnation of its node. Once a node
//! crashes or restarts, the old incarnation's [`SimPeer`] is refused and
//! anything its handler was still answering is lost: tasks a crash leaves
//! running cannot reach the live cluster.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use felix_replication::peer::config::PeerTransportConfig;
use felix_replication::peer::identity::PeerIdentity;
use felix_replication::peer::pool::{
    PeerError, PeerRequester, request_timeout, required_capability,
};
use felix_replication::peer::server::PeerRequestHandler;
use felix_wire::internal::{InternalMessage, PeerCapabilities};
use parking_lot::Mutex;
use tokio::sync::oneshot;

/// The in-memory network between simulated brokers. Cloning shares it.
#[derive(Clone)]
pub struct SimNet {
    inner: Arc<Inner>,
}

impl SimNet {
    /// A network whose requesters time out as a pool under `config` would,
    /// and whose drops are drawn from `seed`.
    pub fn new(config: PeerTransportConfig, seed: u64) -> Self {
        Self {
            inner: Arc::new(Inner {
                config,
                state: Mutex::new(State {
                    nodes: HashMap::new(),
                    links: HashMap::new(),
                    next_incarnation: 1,
                    // Zero is xorshift's one fixed point.
                    rng: seed | 1,
                }),
            }),
        }
    }

    /// Bring up a new incarnation of `node.node_id`, ending any earlier one,
    /// and return the requester it sends with.
    pub fn start(&self, node: SimNode) -> SimPeer {
        let mut state = self.inner.state.lock();
        let incarnation = state.next_incarnation;
        state.next_incarnation += 1;
        state.nodes.insert(
            node.node_id.clone(),
            Running {
                incarnation,
                alive: true,
                capabilities: node.capabilities,
                identity: node.identity,
                handler: node.handler,
                known: HashMap::new(),
            },
        );
        SimPeer {
            net: Arc::clone(&self.inner),
            node_id: node.node_id,
            incarnation,
            capabilities: node.capabilities,
        }
    }

    /// Kill `node_id`'s running incarnation. Its requester is refused from now
    /// on, and requests to it fail as a dial to a dead host does.
    pub fn crash(&self, node_id: &str) {
        if let Some(node) = self.inner.state.lock().nodes.get_mut(node_id) {
            node.alive = false;
            node.known.clear();
        }
    }

    /// Set the link from `from` to `to`. The reverse direction is separate.
    pub fn set_link(&self, from: &str, to: &str, link: Link) {
        self.inner
            .state
            .lock()
            .links
            .insert((from.to_string(), to.to_string()), link);
    }

    /// The link from `from` to `to`.
    pub fn link(&self, from: &str, to: &str) -> Link {
        self.inner.state.lock().link(from, to)
    }

    /// Cut `a` and `b` off from each other in both directions.
    pub fn partition(&self, a: &str, b: &str) {
        for (from, to) in [(a, b), (b, a)] {
            let link = Link {
                partitioned: true,
                ..self.link(from, to)
            };
            self.set_link(from, to, link);
        }
    }

    /// Put every link back to a healthy one.
    pub fn heal(&self) {
        self.inner.state.lock().links.clear();
    }
}

/// One node as [`SimNet::start`] brings it up.
pub struct SimNode {
    pub node_id: String,
    /// What it offers its peers, as its pool and listener would.
    pub capabilities: PeerCapabilities,
    /// What its handshake says about it.
    pub identity: PeerIdentity,
    /// What answers its peers' requests.
    pub handler: Arc<dyn PeerRequestHandler>,
}

/// One directed link's faults. The default is a healthy link.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Link {
    /// Refuse every request at once, as the pool's partition injection does.
    /// An answer already on its way back is lost instead.
    pub partitioned: bool,
    /// How long each message takes to cross.
    pub delay: Duration,
    /// The chance, from 0 to 1, that a message is lost.
    pub drop: f64,
}

/// One incarnation's requester. Refused once that incarnation has ended.
pub struct SimPeer {
    net: Arc<Inner>,
    node_id: String,
    incarnation: u64,
    capabilities: PeerCapabilities,
}

impl SimPeer {
    /// The node this requester sends as.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Reach `node_id` without sending anything: what a dial and handshake
    /// would learn.
    fn reach(&self, node_id: &str) -> Result<(PeerCapabilities, PeerIdentity), PeerError> {
        let mut state = self.net.state.lock();
        state.check_sender(&self.node_id, self.incarnation)?;
        if state.link(&self.node_id, node_id).partitioned {
            return Err(partitioned(node_id));
        }
        let target = state.running(node_id)?;
        let reached = (target.capabilities, target.identity);
        state.record_handshake(&self.node_id, self.capabilities, node_id, reached.0);
        Ok(reached)
    }
}

impl PeerRequester for SimPeer {
    async fn request(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> Result<InternalMessage, PeerError> {
        self.request_identified(node_id, addr, message)
            .await
            .map(|(answer, _)| answer)
    }

    async fn capabilities(
        &self,
        node_id: &str,
        _addr: SocketAddr,
    ) -> Result<PeerCapabilities, PeerError> {
        self.reach(node_id).map(|(capabilities, _)| capabilities)
    }

    fn recorded_capabilities(&self, node_id: &str) -> Option<PeerCapabilities> {
        let state = self.net.state.lock();
        let node = state.nodes.get(&self.node_id)?;
        (node.alive && node.incarnation == self.incarnation)
            .then(|| node.known.get(node_id).copied())
            .flatten()
    }

    fn offers_election(&self) -> bool {
        self.capabilities.contains(PeerCapabilities::ELECTION)
    }

    fn offers_fence(&self) -> bool {
        self.capabilities
            .contains(felix_replication::promotion::REQUIRED)
    }

    async fn identity(&self, node_id: &str, _addr: SocketAddr) -> Result<PeerIdentity, PeerError> {
        self.reach(node_id).map(|(_, identity)| identity)
    }

    async fn request_identified(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> Result<(InternalMessage, PeerIdentity), PeerError> {
        let timeout = request_timeout(&self.net.config, &message);
        let (capabilities, identity) = self.reach(node_id)?;
        if let Some(needed) = required_capability(&message)
            && !capabilities.contains(needed)
        {
            return Err(PeerError::Unsupported {
                node_id: node_id.to_string(),
                kind: message.kind(),
            });
        }
        let frame = wire(&message).ok_or_else(|| disconnected(node_id))?;
        let (answered, answer) = oneshot::channel();
        tokio::spawn(deliver(
            Arc::clone(&self.net),
            Delivery {
                from: self.node_id.clone(),
                to: node_id.to_string(),
                frame,
            },
            answered,
        ));
        let wait = async {
            match answer.await {
                Ok(Some(answer)) => Ok(answer),
                // Lost on the way: the caller hears nothing and waits out its
                // timeout, exactly as a dropped packet leaves it.
                Ok(None) => std::future::pending().await,
                Err(_) => Err(disconnected(node_id)),
            }
        };
        let answer =
            tokio::time::timeout(timeout, wait)
                .await
                .map_err(|_| PeerError::Timeout {
                    node_id: node_id.to_string(),
                    timeout,
                })??;
        // A crash may have ended this incarnation while it waited.
        self.net
            .state
            .lock()
            .check_sender(&self.node_id, self.incarnation)?;
        let answer = wire(&answer).ok_or_else(|| disconnected(node_id))?;
        Ok((answer, identity))
    }
}

struct Inner {
    config: PeerTransportConfig,
    state: Mutex<State>,
}

struct State {
    nodes: HashMap<String, Running>,
    links: HashMap<(String, String), Link>,
    next_incarnation: u64,
    /// xorshift64, so a seed replays the same drops without a dependency.
    rng: u64,
}

impl State {
    fn link(&self, from: &str, to: &str) -> Link {
        self.links
            .get(&(from.to_string(), to.to_string()))
            .copied()
            .unwrap_or_default()
    }

    /// Refuse a requester whose incarnation has ended, as a shut-down pool is.
    fn check_sender(&self, node_id: &str, incarnation: u64) -> Result<(), PeerError> {
        match self.nodes.get(node_id) {
            Some(node) if node.alive && node.incarnation == incarnation => Ok(()),
            _ => Err(PeerError::ShuttingDown),
        }
    }

    fn running(&self, node_id: &str) -> Result<&Running, PeerError> {
        self.nodes
            .get(node_id)
            .filter(|node| node.alive)
            .ok_or_else(|| PeerError::Handshake {
                node_id: node_id.to_string(),
                detail: "not running (simulated)".to_string(),
            })
    }

    /// A handshake records each side's offer on the other, as the pool and
    /// listener share `KnownCapabilities`.
    fn record_handshake(
        &mut self,
        from: &str,
        from_offer: PeerCapabilities,
        to: &str,
        to_offer: PeerCapabilities,
    ) {
        if let Some(node) = self.nodes.get_mut(from) {
            node.known.insert(to.to_string(), to_offer);
        }
        if let Some(node) = self.nodes.get_mut(to) {
            node.known.insert(from.to_string(), from_offer);
        }
    }

    /// Whether a message crossing `link` is lost.
    fn drops(&mut self, link: Link) -> bool {
        if link.drop <= 0.0 {
            return false;
        }
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        ((self.rng >> 11) as f64 / (1u64 << 53) as f64) < link.drop
    }
}

/// One incarnation of a node.
struct Running {
    incarnation: u64,
    alive: bool,
    capabilities: PeerCapabilities,
    identity: PeerIdentity,
    handler: Arc<dyn PeerRequestHandler>,
    /// What each peer offered in its latest handshake with this incarnation.
    known: HashMap<String, PeerCapabilities>,
}

/// A request on its way to its target.
struct Delivery {
    from: String,
    to: String,
    frame: InternalMessage,
}

/// Carry `delivery` to its target, run its handler, and carry the answer back.
///
/// Sends `None` for an answer lost on the way back, and drops `answered`
/// without sending when the connection would have been lost: the target died
/// before answering.
async fn deliver(
    net: Arc<Inner>,
    delivery: Delivery,
    answered: oneshot::Sender<Option<InternalMessage>>,
) {
    let Delivery { from, to, frame } = delivery;
    let outbound = {
        let mut state = net.state.lock();
        let link = state.link(&from, &to);
        (!state.drops(link)).then_some(link.delay)
    };
    let Some(delay) = outbound else {
        // Lost on the way out. The caller times out; nothing ran.
        let _ = answered.send(None);
        return;
    };
    tokio::time::sleep(delay).await;
    let (handler, incarnation, peer) = {
        let state = net.state.lock();
        let Ok(target) = state.running(&to) else {
            return;
        };
        let peer = target
            .capabilities
            .contains(PeerCapabilities::BALLOTS)
            .then(|| Arc::<str>::from(from.as_str()));
        (Arc::clone(&target.handler), target.incarnation, peer)
    };
    let answer = handler.handle_from(peer, frame).await;
    let inbound = {
        let mut state = net.state.lock();
        // A target that died while answering takes its connection with it.
        if !state
            .nodes
            .get(&to)
            .is_some_and(|node| node.alive && node.incarnation == incarnation)
        {
            return;
        }
        let link = state.link(&to, &from);
        (!link.partitioned && !state.drops(link)).then_some(link.delay)
    };
    let Some(delay) = inbound else {
        let _ = answered.send(None);
        return;
    };
    tokio::time::sleep(delay).await;
    let _ = answered.send(Some(answer));
}

/// `message` as it arrives after the peer codec carries it, or `None` for one
/// the codec cannot carry: the real transport loses the connection on it.
fn wire(message: &InternalMessage) -> Option<InternalMessage> {
    let frame = message.encode().ok()?;
    InternalMessage::decode(frame).ok()
}

fn partitioned(node_id: &str) -> PeerError {
    PeerError::Unavailable {
        node_id: node_id.to_string(),
        detail: "partitioned from this broker (simulated)".to_string(),
    }
}

fn disconnected(node_id: &str) -> PeerError {
    PeerError::Disconnected {
        node_id: node_id.to_string(),
    }
}

#[cfg(test)]
mod tests;
