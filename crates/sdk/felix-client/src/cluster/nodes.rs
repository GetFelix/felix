//! One client per broker, shared by everything a
//! [`ClusterClient`](super::ClusterClient) sends there.
//!
//! The entry broker, a shard's owner, the target of a redirect and an
//! idempotent producer's leader are often the same broker. Sharing one client
//! among them means a broker costs one connection until the streams on it
//! need more.
//!
//! A cluster client acting as another identity has its own `Nodes` whose
//! clients are derived from the parent's, so it shares their connections.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::auth::TokenProvider;
use crate::client::{Client, fetch_from, fetch_token, no_broker_answered};
use crate::config::ClientConfig;

use super::seeds::{LOOKUP_TIMEOUT, Lookup};

/// The client for each broker a cluster client has reached.
pub(super) struct Nodes {
    source: Source,
    slots: Mutex<HashMap<SocketAddr, Arc<Slot>>>,
    /// The name each broker that advertised one was resolved from, which is
    /// what its certificate is checked against.
    names: Mutex<HashMap<SocketAddr, String>>,
}

/// Where a broker's client comes from.
enum Source {
    /// Connections of its own.
    Dial {
        server_name: String,
        config: Box<ClientConfig>,
        lookup: Arc<dyn Lookup>,
    },
    /// [`Client::with_identity`] over the client `base` holds for the same
    /// broker.
    Identity {
        base: Arc<Nodes>,
        tenant_id: String,
        tokens: Arc<dyn TokenProvider>,
    },
}

impl Nodes {
    pub(super) fn new(server_name: &str, config: ClientConfig, lookup: Arc<dyn Lookup>) -> Self {
        Self::with_source(Source::Dial {
            server_name: server_name.to_string(),
            config: Box::new(config),
            lookup,
        })
    }

    /// Clients that act as `tenant_id` with `tokens` over the connections of
    /// `parent`'s clients.
    pub(super) fn for_identity(
        parent: &Arc<Nodes>,
        tenant_id: String,
        tokens: Arc<dyn TokenProvider>,
    ) -> Self {
        // Always derived from the dialling set, so an identity of an identity
        // does not stack.
        let base = match &parent.source {
            Source::Dial { .. } => Arc::clone(parent),
            Source::Identity { base, .. } => Arc::clone(base),
        };
        Self::with_source(Source::Identity {
            base,
            tenant_id,
            tokens,
        })
    }

    fn with_source(source: Source) -> Self {
        Self {
            source,
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
        let found = self.lookup(advertised).await;
        self.adopt(advertised, found)
    }

    /// The address a broker that advertised `advertised` is dialled at, given
    /// what the name resolved to, and the name its certificate is checked
    /// against from now on.
    pub(super) fn adopt(
        &self,
        advertised: &str,
        found: io::Result<Vec<SocketAddr>>,
    ) -> Option<SocketAddr> {
        // The dialling set connects, so it is the one that needs the name.
        if let Source::Identity { base, .. } = &self.source {
            return base.adopt(advertised, found);
        }
        // IPv4 when the name has both: a name like `localhost` resolves to
        // both families, and a broker is far more often listening on v4.
        let resolved = match found {
            Ok(addrs) => addrs
                .iter()
                .find(|addr| addr.is_ipv4())
                .or(addrs.first())
                .copied(),
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

    /// Where each broker in `advertised` is reached now, as
    /// [`Nodes::resolve`] answers, with every name looked up at once. Names
    /// that do not resolve are left out.
    pub(super) async fn resolve_all(&self, advertised: &[String]) -> Vec<SocketAddr> {
        let mut addrs = Vec::with_capacity(advertised.len());
        let mut names = Vec::new();
        for broker in advertised {
            match broker.parse::<SocketAddr>() {
                Ok(addr) => addrs.push(addr),
                Err(_) => names.push(broker.clone()),
            }
        }
        for (name, found) in self.lookup_all(names).await {
            if let Some(addr) = self.adopt(&name, found) {
                addrs.push(addr);
            }
        }
        addrs
    }

    /// Every address `host_port` resolves to now, or an error after
    /// [`LOOKUP_TIMEOUT`].
    pub(super) async fn lookup(&self, host_port: &str) -> io::Result<Vec<SocketAddr>> {
        let mut found = self.lookup_all(vec![host_port.to_string()]).await;
        found
            .pop()
            .map(|(_, result)| result)
            .unwrap_or_else(|| Ok(Vec::new()))
    }

    /// Look every name up at once, each bounded by [`LOOKUP_TIMEOUT`], and
    /// return the answers in the order the names were given.
    ///
    /// Concurrent and bounded because a resolver that stops answering, rather
    /// than saying a name does not exist, otherwise holds a reconnect for
    /// every retry the system resolver makes, name after name.
    pub(super) async fn lookup_all(
        &self,
        names: Vec<String>,
    ) -> Vec<(String, io::Result<Vec<SocketAddr>>)> {
        let lookup = self.lookup_source();
        let mut lookups = tokio::task::JoinSet::new();
        for (index, name) in names.into_iter().enumerate() {
            let lookup = Arc::clone(&lookup);
            lookups.spawn(async move {
                let found = tokio::time::timeout(LOOKUP_TIMEOUT, lookup.lookup(&name))
                    .await
                    .unwrap_or_else(|_| {
                        Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            format!("no answer within {LOOKUP_TIMEOUT:?}"),
                        ))
                    });
                (index, name, found)
            });
        }
        let mut answers = Vec::new();
        while let Some(answer) = lookups.join_next().await {
            match answer {
                Ok(answer) => answers.push(answer),
                Err(err) => tracing::warn!(error = %err, "a name lookup task failed"),
            }
        }
        answers.sort_by_key(|(index, _, _)| *index);
        answers
            .into_iter()
            .map(|(_, name, found)| (name, found))
            .collect()
    }

    fn lookup_source(&self) -> Arc<dyn Lookup> {
        match &self.source {
            Source::Dial { lookup, .. } => Arc::clone(lookup),
            Source::Identity { base, .. } => base.lookup_source(),
        }
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

    /// Fetch the tokens a connect will present, bounded by `FETCH_TIMEOUT`.
    /// Callers that wrap [`connect_to`](Self::connect_to) in a short timer run
    /// this first, so the timer covers the handshake and not the provider.
    pub(super) async fn prefetch(&self) -> Result<()> {
        match &self.source {
            Source::Dial { config, .. } => fetch_token(config).await,
            Source::Identity { base, tokens, .. } => {
                // Together, so a slow pair costs one fetch timeout, not two.
                tokio::try_join!(Box::pin(base.prefetch()), fetch_from(tokens.as_ref())).map(drop)
            }
        }
    }

    /// The client for the broker at `addr`, connecting only when there is no
    /// usable one.
    ///
    /// A client that has lost the connection under its workers is replaced
    /// here, which is what rebuilds a broker after a failure. Anyone still
    /// holding the old one keeps it until their own work on it fails. An
    /// identity's client is replaced the same way, from the parent's client
    /// for that broker, which is itself rebuilt first if it has failed.
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
        let client = match &self.source {
            Source::Dial {
                server_name,
                config,
                ..
            } => {
                // A broker that advertised a name is checked against that
                // name, as a client dialling the name directly would check it.
                let server_name = self
                    .names
                    .lock()
                    .expect("node names")
                    .get(&addr)
                    .cloned()
                    .unwrap_or_else(|| server_name.clone());
                Client::connect_shared(addr, &server_name, ClientConfig::clone(config)).await?
            }
            Source::Identity {
                base,
                tenant_id,
                tokens,
            } => {
                let shared = Box::pin(base.connect_to(addr)).await?;
                shared
                    .with_identity(tenant_id.clone(), Arc::clone(tokens))
                    .await?
            }
        };
        let client = Arc::new(client);
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
        self.prefetch().await?;
        self.connect_any_prefetched(addrs).await
    }

    /// [`connect_any`](Self::connect_any) for a caller that has just run
    /// [`prefetch`](Self::prefetch).
    pub(super) async fn connect_any_prefetched(&self, addrs: &[SocketAddr]) -> Result<Arc<Client>> {
        if addrs.is_empty() {
            return Err(anyhow::anyhow!(
                "no broker addresses were given to connect to"
            ));
        }
        let mut refusals = Vec::with_capacity(addrs.len());
        for addr in addrs {
            match self.connect_to(*addr).await {
                Ok(client) => return Ok(client),
                Err(err) => refusals.push((*addr, err)),
            }
        }
        Err(no_broker_answered(refusals))
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
