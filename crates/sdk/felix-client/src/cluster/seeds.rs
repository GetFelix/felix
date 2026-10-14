//! The brokers a [`ClusterClient`](super::ClusterClient) was told about, and
//! how their addresses are found again.
//!
//! A name outlives the addresses it resolves to: a Kubernetes rollout keeps
//! every pod's name and gives each one a new IP. When none of the addresses a
//! client holds answers any more, the names it was given, and the names the
//! brokers advertised, are the only way back in.

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
    /// The last resolution and when it ran.
    resolved: Mutex<Resolved>,
}

#[derive(Default)]
struct Resolved {
    at: Option<Instant>,
    /// Every name's addresses, seeds and advertised alike.
    addrs: Vec<SocketAddr>,
    /// The seed names' share of `addrs`.
    seeds: Vec<SocketAddr>,
}

impl Seeds {
    pub(super) fn new(configured: Vec<Seed>) -> Self {
        Self {
            configured,
            advertised: Mutex::new(Vec::new()),
            resolved: Mutex::new(Resolved::default()),
        }
    }

    /// The configured seeds' addresses: the literal ones, and what each name
    /// last resolved to. Never trimmed by discovery.
    pub(super) fn addrs(&self) -> Vec<SocketAddr> {
        let mut addrs = self.literal();
        let resolved = self.resolved.lock().expect("resolved seeds");
        for addr in &resolved.seeds {
            if !addrs.contains(addr) {
                addrs.push(*addr);
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
    /// Literal seeds are not included.
    pub(super) async fn resolve(&self, nodes: &Nodes) -> Vec<SocketAddr> {
        {
            let resolved = self.resolved.lock().expect("resolved seeds");
            if let Some(at) = resolved.at
                && at.elapsed() < RESOLVE_AGAIN_AFTER
            {
                return resolved.addrs.clone();
            }
        }
        let seed_names: Vec<&str> = self
            .configured
            .iter()
            .filter_map(|seed| match seed {
                Seed::Name(name) => Some(name.as_str()),
                Seed::Addr(_) => None,
            })
            .collect();
        let advertised = self.advertised.lock().expect("advertised names").clone();
        let seeds = resolve_all(nodes, seed_names.iter().copied()).await;
        let mut addrs = seeds.clone();
        // Through `Nodes::resolve`, as discovery does, so a broker reached by
        // its advertised name is checked against that name.
        for name in advertised
            .iter()
            .filter(|name| !seed_names.contains(&name.as_str()))
        {
            if let Some(addr) = nodes.resolve(name).await
                && !addrs.contains(&addr)
            {
                addrs.push(addr);
            }
        }

        // Concurrent callers may both have looked up; the last one wins, and
        // both answers are equally fresh.
        let mut resolved = self.resolved.lock().expect("resolved seeds");
        resolved.at = Some(Instant::now());
        resolved.addrs.clone_from(&addrs);
        // A DNS outage must not strip the seeds: no answer at all keeps the
        // last one.
        if !seeds.is_empty() {
            resolved.seeds = seeds;
        }
        addrs
    }

    fn literal(&self) -> Vec<SocketAddr> {
        let mut addrs = Vec::new();
        for seed in &self.configured {
            if let Seed::Addr(addr) = seed
                && !addrs.contains(addr)
            {
                addrs.push(*addr);
            }
        }
        addrs
    }
}

/// Every address `names` resolve to, IPv4 first.
///
/// A name that does not resolve is logged and skipped: one broken name must
/// not hide the others.
async fn resolve_all<'a>(nodes: &Nodes, names: impl Iterator<Item = &'a str>) -> Vec<SocketAddr> {
    let mut addrs = Vec::new();
    for name in names {
        match nodes.lookup(name).await {
            Ok(found) => {
                if found.is_empty() {
                    tracing::warn!(name, "a broker name resolved to no address");
                }
                for addr in found {
                    if !addrs.contains(&addr) {
                        addrs.push(addr);
                    }
                }
            }
            Err(err) => tracing::warn!(name, error = %err, "a broker name did not resolve"),
        }
    }
    // `localhost` and dual-stack names resolve to both families, and a broker
    // is far more often listening on v4.
    addrs.sort_by_key(|addr| !addr.is_ipv4());
    addrs
}
