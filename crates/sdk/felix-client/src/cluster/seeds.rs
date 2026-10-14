//! The brokers a [`ClusterClient`](super::ClusterClient) was told about, and
//! how their addresses are found again.
//!
//! A name outlives the addresses it resolves to: a Kubernetes rollout keeps
//! every pod's name and gives each one a new IP. When none of the addresses a
//! client holds answers any more, the names it was given, and the names the
//! brokers advertised, are the only way back in.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::nodes::Nodes;

/// How long one re-resolution is reused.
///
/// Every operation that fails against a dead cluster reconnects, so without
/// this a client with many callers would ask DNS once per failed operation.
/// The reconnect backoff paces each caller; this bounds them all together.
pub(super) const RESOLVE_AGAIN_AFTER: Duration = Duration::from_secs(1);

/// How long one name lookup may take before it counts as unresolved.
///
/// A resolver that has stopped answering, as opposed to one saying a name
/// does not exist, would otherwise hold a reconnect for as long as the system
/// resolver keeps retrying.
pub(super) const LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);

/// A broker address a cluster client is configured with.
///
/// A [`Seed::Name`] is resolved when the client connects and again whenever
/// none of the addresses it holds answers, so it keeps working after every
/// broker behind it has moved. A [`Seed::Addr`] is dialled as given.
///
/// ```
/// use felix_client::Seed;
///
/// let by_name: Seed = "felix.prod.svc:5000".parse()?;
/// let by_addr: Seed = "10.0.0.4:5000".parse()?;
/// assert!(matches!(by_name, Seed::Name(_)));
/// assert!(matches!(by_addr, Seed::Addr(_)));
/// # Ok::<(), anyhow::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Seed {
    /// An address, dialled as given.
    Addr(SocketAddr),
    /// A `host:port` resolved on connect and again after every known address
    /// stops answering. Every address the name resolves to is tried, so one
    /// name can stand for every broker behind a headless service.
    Name(String),
}

impl From<SocketAddr> for Seed {
    fn from(addr: SocketAddr) -> Self {
        Seed::Addr(addr)
    }
}

impl FromStr for Seed {
    type Err = anyhow::Error;

    /// An IP address with a port, else a `host:port`. A host without a port
    /// is refused rather than guessed at.
    fn from_str(value: &str) -> anyhow::Result<Self> {
        let value = value.trim();
        if let Ok(addr) = value.parse::<SocketAddr>() {
            return Ok(Seed::Addr(addr));
        }
        match value.rsplit_once(':') {
            Some((host, port)) if !host.is_empty() && port.parse::<u16>().is_ok() => {
                Ok(Seed::Name(value.to_string()))
            }
            _ => Err(anyhow::anyhow!(
                "broker address {value:?} needs a port, as host:port"
            )),
        }
    }
}

impl fmt::Display for Seed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Seed::Addr(addr) => addr.fmt(f),
            Seed::Name(name) => f.write_str(name),
        }
    }
}

/// Looks a `host:port` up. DNS in production; a test swaps in its own to move
/// brokers without touching the system resolver.
pub(crate) trait Lookup: Send + Sync {
    fn lookup<'a>(
        &'a self,
        host_port: &'a str,
    ) -> Pin<Box<dyn Future<Output = io::Result<Vec<SocketAddr>>> + Send + 'a>>;
}

/// The system resolver.
pub(crate) struct Dns;

impl Lookup for Dns {
    fn lookup<'a>(
        &'a self,
        host_port: &'a str,
    ) -> Pin<Box<dyn Future<Output = io::Result<Vec<SocketAddr>>> + Send + 'a>> {
        Box::pin(async move { Ok(tokio::net::lookup_host(host_port).await?.collect()) })
    }
}

/// The configured seeds, and the names worth resolving again.
pub(super) struct Seeds {
    configured: Vec<Seed>,
    /// Names brokers advertised in the last topology answer. Kept beside the
    /// seeds because a broker's own name is often the more durable one: a
    /// StatefulSet pod's DNS name survives every restart.
    advertised: Mutex<Vec<String>>,
    /// Each seed name's last answer that named an address. Kept per name so
    /// a name that stops resolving keeps its last addresses as a seed while
    /// the others move on.
    by_name: Mutex<HashMap<String, Vec<SocketAddr>>>,
    /// The last resolution and when it ran. Held across the lookups, so one
    /// resolution runs at a time for this client and every identity sharing
    /// it, and a caller that waited gets the answer it waited for.
    last: tokio::sync::Mutex<Option<(Instant, Vec<SocketAddr>)>>,
}

impl Seeds {
    pub(super) fn new(configured: Vec<Seed>) -> Self {
        Self {
            configured,
            advertised: Mutex::new(Vec::new()),
            by_name: Mutex::new(HashMap::new()),
            last: tokio::sync::Mutex::new(None),
        }
    }

    /// The configured seeds' addresses: the literal ones, and what each name
    /// last resolved to. Never trimmed by discovery.
    pub(super) fn addrs(&self) -> Vec<SocketAddr> {
        let by_name = self.by_name.lock().expect("resolved seeds");
        let mut addrs = Vec::new();
        for seed in &self.configured {
            let found = match seed {
                Seed::Addr(addr) => std::slice::from_ref(addr),
                Seed::Name(name) => by_name.get(name).map_or(&[][..], Vec::as_slice),
            };
            for addr in found {
                if !addrs.contains(addr) {
                    addrs.push(*addr);
                }
            }
        }
        addrs
    }

    /// Remember the names a topology answer advertised, for
    /// [`Seeds::resolve`] to look up again.
    pub(super) fn set_advertised(&self, advertised: impl IntoIterator<Item = String>) {
        let names = advertised
            .into_iter()
            .filter(|addr| addr.parse::<SocketAddr>().is_err())
            .collect();
        *self.advertised.lock().expect("advertised names") = names;
    }

    /// What the seed names and advertised names resolve to now, or what they
    /// resolved to within the last [`RESOLVE_AGAIN_AFTER`].
    ///
    /// Every name is looked up at once, each bounded by [`LOOKUP_TIMEOUT`].
    /// A name that does not resolve is logged and skipped: one broken name
    /// must not hide the others. Literal seeds are not included.
    pub(super) async fn resolve(&self, nodes: &Nodes) -> Vec<SocketAddr> {
        let mut last = self.last.lock().await;
        if let Some((at, addrs)) = last.as_ref()
            && at.elapsed() < RESOLVE_AGAIN_AFTER
        {
            return addrs.clone();
        }
        let seed_names: Vec<String> = self
            .configured
            .iter()
            .filter_map(|seed| match seed {
                Seed::Name(name) => Some(name.clone()),
                Seed::Addr(_) => None,
            })
            .collect();
        let mut names = seed_names.clone();
        for name in self.advertised.lock().expect("advertised names").iter() {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }

        let mut addrs = Vec::new();
        for (name, found) in nodes.lookup_all(names).await {
            if !seed_names.contains(&name) {
                // Through `Nodes::adopt`, as discovery does, so a broker
                // reached by its advertised name is checked against that name.
                if let Some(addr) = nodes.adopt(&name, found)
                    && !addrs.contains(&addr)
                {
                    addrs.push(addr);
                }
                continue;
            }
            let mut found = match found {
                Ok(found) if !found.is_empty() => found,
                Ok(_) => {
                    tracing::warn!(name, "a broker name resolved to no address");
                    continue;
                }
                Err(err) => {
                    tracing::warn!(name, error = %err, "a broker name did not resolve");
                    continue;
                }
            };
            // `localhost` and dual-stack names resolve to both families, and
            // a broker is far more often listening on v4.
            found.sort_by_key(|addr| !addr.is_ipv4());
            for addr in &found {
                if !addrs.contains(addr) {
                    addrs.push(*addr);
                }
            }
            self.by_name
                .lock()
                .expect("resolved seeds")
                .insert(name, found);
        }
        *last = Some((Instant::now(), addrs.clone()));
        addrs
    }
}
