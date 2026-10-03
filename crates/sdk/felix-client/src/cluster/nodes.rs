//! One client per broker, shared by everything a
//! [`ClusterClient`](super::ClusterClient) sends there.
//!
//! The entry broker, a shard's owner, the target of a redirect and an
//! idempotent producer's leader are often the same broker. Sharing one client
//! among them means a broker costs one connection until the streams on it
//! need more.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::client::Client;
use crate::config::ClientConfig;

/// The client for each broker a cluster client has reached.
pub(super) struct Nodes {
    server_name: String,
    config: ClientConfig,
    slots: Mutex<HashMap<SocketAddr, Arc<Slot>>>,
    /// The name each broker that advertised one was resolved from, which is
    /// what its certificate is checked against.
    names: Mutex<HashMap<SocketAddr, String>>,
}

impl Nodes {
    pub(super) fn new(server_name: &str, config: ClientConfig) -> Self {
        Self {
            server_name: server_name.to_string(),
            config,
            slots: Mutex::new(HashMap::new()),
            names: Mutex::new(HashMap::new()),
        }
    }

    /// Where a broker's advertised `host:port` is reached: the address itself,
    /// or what its name resolves to now. `None`, logged, for a name that does
    /// not resolve.
    ///
    /// Looked up every time it is asked, since a broker's name outlives its
    /// address: a rescheduled pod keeps its name and gets a new IP.
    pub(super) async fn resolve(&self, advertised: &str) -> Option<SocketAddr> {
        if let Ok(addr) = advertised.parse::<SocketAddr>() {
            return Some(addr);
        }
        // IPv4 when the name has both: a name like `localhost` resolves to
        // both families, and a broker is far more often listening on v4.
        let resolved = match tokio::net::lookup_host(advertised).await {
            Ok(addrs) => {
                let addrs: Vec<SocketAddr> = addrs.collect();
                addrs
                    .iter()
                    .find(|addr| addr.is_ipv4())
                    .or(addrs.first())
                    .copied()
            }
            Err(err) => {
                tracing::warn!(advertised, error = %err, "a broker's advertised name did not resolve");
                return None;
            }
        };
        let Some(addr) = resolved else {
            tracing::warn!(
                advertised,
                "a broker's advertised name resolved to no address"
            );
            return None;
        };
        if let Some((host, _)) = advertised.rsplit_once(':') {
            self.names
                .lock()
                .expect("node names")
                .insert(addr, host.to_string());
        }
        Some(addr)
    }

    /// Live connections to each broker a client is held for.
    ///
    /// A client already replaced after a failure, and still kept alive by a
    /// subscription on it, is not counted.
    pub(super) async fn connections_per_node(&self) -> Vec<(SocketAddr, usize)> {
        let mut counts = Vec::new();
        for (addr, slot) in self.held() {
            if let Some(client) = slot.client.lock().await.as_ref() {
                counts.push((addr, client.connection_count()));
            }
        }
        counts.sort();
        counts
    }

    /// Every client currently held, one per broker.
    pub(super) async fn clients(&self) -> Vec<Arc<Client>> {
        let mut clients = Vec::new();
        for (_, slot) in self.held() {
            if let Some(client) = slot.client.lock().await.as_ref() {
                clients.push(Arc::clone(client));
            }
        }
        clients
    }

    /// The client for the broker at `addr`, connecting only when there is no
    /// usable one.
    ///
    /// A client that has lost the connection under its workers is replaced
    /// here, which is what rebuilds a broker after a failure. Anyone still
    /// holding the old one keeps it until their own work on it fails.
    pub(super) async fn connect_to(&self, addr: SocketAddr) -> Result<Arc<Client>> {
        let slot = self.slot(addr);
        // Held across the connect, so concurrent callers for one broker share
        // a connect instead of each opening their own.
        let mut held = slot.client.lock().await;
        if let Some(client) = held.as_ref()
            && client.is_usable()
        {
            return Ok(Arc::clone(client));
        }
        // A broker that advertised a name is checked against that name, as a
        // client dialling the name directly would check it.
        let server_name = self
            .names
            .lock()
            .expect("node names")
            .get(&addr)
            .cloned()
            .unwrap_or_else(|| self.server_name.clone());
        let client =
            Arc::new(Client::connect_shared(addr, &server_name, self.config.clone()).await?);
        *held = Some(Arc::clone(&client));
        Ok(client)
    }

    /// The client for whichever of `addrs` answers first, in order, with the
    /// same contract as [`Client::connect_any`].
    pub(super) async fn connect_any(&self, addrs: &[SocketAddr]) -> Result<Arc<Client>> {
        if addrs.is_empty() {
            return Err(anyhow::anyhow!(
                "no broker addresses were given to connect to"
            ));
        }
        let mut refusals = Vec::with_capacity(addrs.len());
        for addr in addrs {
            match self.connect_to(*addr).await {
                Ok(client) => return Ok(client),
                Err(err) => refusals.push(format!("{addr}: {err:#}")),
            }
        }
        Err(anyhow::anyhow!(
            "no broker answered ({})",
            refusals.join("; ")
        ))
    }

    /// Stop handing out `client`, so the next caller for its broker gets a
    /// fresh one. A replacement someone else already made is left alone.
    pub(super) async fn forget(&self, client: &Arc<Client>) {
        let slot = self.slot(client.dialled());
        let mut held = slot.client.lock().await;
        if held
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, client))
        {
            *held = None;
        }
    }

    fn held(&self) -> Vec<(SocketAddr, Arc<Slot>)> {
        self.slots
            .lock()
            .expect("node slots")
            .iter()
            .map(|(addr, slot)| (*addr, Arc::clone(slot)))
            .collect()
    }

    fn slot(&self, addr: SocketAddr) -> Arc<Slot> {
        Arc::clone(
            self.slots
                .lock()
                .expect("node slots")
                .entry(addr)
                .or_default(),
        )
    }
}

/// The client for one broker.
#[derive(Default)]
struct Slot {
    client: tokio::sync::Mutex<Option<Arc<Client>>>,
}
