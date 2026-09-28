//! A UDP interposer between a client and one broker, for the connection-fault
//! scenarios.
//!
//! A client is pointed at [`Interposer::addr`] instead of the broker. Each
//! source address the client sends from gets its own upstream socket, the way
//! a NAT would, so every QUIC connection the client opens is a separate flow.
//! [`Interposer::inject`] then breaks those flows in one of three ways (see
//! [`LinkFault`]).
//!
//! QUIC runs over UDP and encrypts everything a middlebox could forge, so there
//! is no RST to send: a client learns that a flow is gone from its idle
//! timeout, or from the broker answering on a new one. What differs between
//! the faults is whether the old flow can come back and whether a new one gets
//! through.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

/// Large enough for any UDP datagram, so nothing is truncated on the way.
const MAX_DATAGRAM: usize = 65_536;

/// What a fault step does to the link.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LinkFault {
    /// Discard every datagram, both ways, for the hold. Flows that outlive it
    /// carry on; new connections through the interposer fail while it holds.
    Drop,
    /// Kill every flow open right now, for good. Their datagrams are discarded
    /// from here on, while a connection opened afterwards goes straight
    /// through: the old connection is gone and a reconnect works at once.
    Reset,
    /// Hold every datagram for the hold, then deliver them in order. Nothing
    /// is lost; everything is late.
    Stall,
}

impl std::fmt::Display for LinkFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Drop => "drop",
            Self::Reset => "reset",
            Self::Stall => "stall",
        })
    }
}

/// A UDP interposer in front of one broker's client listener.
///
/// Dropping it stops forwarding.
pub struct Interposer {
    addr: SocketAddr,
    shared: Arc<Shared>,
    stop: watch::Sender<bool>,
}

impl Interposer {
    /// Listen on a fresh loopback port and forward to `upstream`.
    pub async fn start(upstream: SocketAddr) -> Result<Self> {
        let listener = UdpSocket::bind("127.0.0.1:0")
            .await
            .context("bind the interposer")?;
        let addr = listener
            .local_addr()
            .context("read the interposer address")?;
        let shared = Arc::new(Shared {
            upstream,
            state: Mutex::new(State::default()),
        });
        let (stop, stopped) = watch::channel(false);
        tokio::spawn(serve(Arc::new(listener), Arc::clone(&shared), stopped));
        Ok(Self { addr, shared, stop })
    }

    /// Where a client should connect to go through the interposer.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Apply `fault`. A drop or a stall lasts `hold` and then heals on its
    /// own; a reset takes effect at once and `hold` is ignored.
    pub fn inject(&self, fault: LinkFault, hold: Duration) {
        let mut state = self.shared.state.lock().expect("interposer lock");
        let until = Instant::now() + hold;
        match fault {
            LinkFault::Drop => state.drop_until = Some(until),
            LinkFault::Stall => state.stall_until = Some(until),
            LinkFault::Reset => state.generation += 1,
        }
    }

    /// End a drop or a stall now. A reset flow stays dead.
    pub fn heal(&self) {
        let mut state = self.shared.state.lock().expect("interposer lock");
        state.drop_until = None;
        state.stall_until = None;
    }
}

impl Drop for Interposer {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

struct Shared {
    upstream: SocketAddr,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    drop_until: Option<Instant>,
    stall_until: Option<Instant>,
    /// Bumped by each reset. A flow opened under an older generation is dead.
    generation: u64,
}

impl Shared {
    /// When a datagram on a flow of `generation` may leave, or `None` to
    /// discard it.
    fn verdict(&self, generation: u64) -> Option<Instant> {
        let state = self.state.lock().expect("interposer lock");
        let now = Instant::now();
        if generation != state.generation || state.drop_until.is_some_and(|until| now < until) {
            return None;
        }
        Some(state.stall_until.map_or(now, |until| until.max(now)))
    }

    fn generation(&self) -> u64 {
        self.state.lock().expect("interposer lock").generation
    }
}

type Queued = (Instant, Vec<u8>);

struct Flow {
    generation: u64,
    to_upstream: mpsc::UnboundedSender<Queued>,
}

async fn serve(listener: Arc<UdpSocket>, shared: Arc<Shared>, mut stopped: watch::Receiver<bool>) {
    let mut flows: HashMap<SocketAddr, Flow> = HashMap::new();
    let mut buf = vec![0u8; MAX_DATAGRAM];
    loop {
        let (len, source) = tokio::select! {
            _ = stopped.changed() => return,
            received = listener.recv_from(&mut buf) => match received {
                Ok(received) => received,
                // A reset from an earlier send, on platforms that report one.
                Err(_) => continue,
            },
        };
        let flow = match flows.get(&source) {
            Some(flow) => flow,
            None => match open(source, &listener, &shared, stopped.clone()).await {
                Ok(flow) => flows.entry(source).or_insert(flow),
                // Out of sockets; the client retransmits, and the next
                // datagram tries again.
                Err(_) => continue,
            },
        };
        if let Some(due) = shared.verdict(flow.generation) {
            let _ = flow.to_upstream.send((due, buf[..len].to_vec()));
        }
    }
}

/// Start a flow for `source`: its own upstream socket, a queue each way, and a
/// reader for replies.
async fn open(
    source: SocketAddr,
    listener: &Arc<UdpSocket>,
    shared: &Arc<Shared>,
    stopped: watch::Receiver<bool>,
) -> Result<Flow> {
    let upstream = Arc::new(
        UdpSocket::bind("127.0.0.1:0")
            .await
            .context("bind an interposer upstream socket")?,
    );
    let generation = shared.generation();

    let (to_upstream, queue) = mpsc::unbounded_channel();
    tokio::spawn(deliver(queue, stopped.clone(), {
        let upstream = Arc::clone(&upstream);
        let to = shared.upstream;
        move |datagram| {
            let upstream = Arc::clone(&upstream);
            async move {
                let _ = upstream.send_to(&datagram, to).await;
            }
        }
    }));

    let (to_source, queue) = mpsc::unbounded_channel();
    tokio::spawn(deliver(queue, stopped.clone(), {
        let listener = Arc::clone(listener);
        move |datagram| {
            let listener = Arc::clone(&listener);
            async move {
                let _ = listener.send_to(&datagram, source).await;
            }
        }
    }));

    tokio::spawn(replies(
        upstream,
        generation,
        to_source,
        Arc::clone(shared),
        stopped,
    ));
    Ok(Flow {
        generation,
        to_upstream,
    })
}

/// Read what the broker sends back on one flow and queue it for the client.
async fn replies(
    upstream: Arc<UdpSocket>,
    generation: u64,
    to_source: mpsc::UnboundedSender<Queued>,
    shared: Arc<Shared>,
    mut stopped: watch::Receiver<bool>,
) {
    let mut buf = vec![0u8; MAX_DATAGRAM];
    loop {
        let len = tokio::select! {
            _ = stopped.changed() => return,
            received = upstream.recv_from(&mut buf) => match received {
                Ok((len, _)) => len,
                Err(_) => continue,
            },
        };
        if let Some(due) = shared.verdict(generation)
            && to_source.send((due, buf[..len].to_vec())).is_err()
        {
            return;
        }
    }
}

/// Send each queued datagram once its time comes, in order.
async fn deliver<F, Fut>(
    mut queue: mpsc::UnboundedReceiver<Queued>,
    mut stopped: watch::Receiver<bool>,
    mut send: F,
) where
    F: FnMut(Vec<u8>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    loop {
        let (due, datagram) = tokio::select! {
            _ = stopped.changed() => return,
            next = queue.recv() => match next {
                Some(next) => next,
                None => return,
            },
        };
        tokio::time::sleep_until(due).await;
        send(datagram).await;
    }
}

#[cfg(test)]
mod tests;
