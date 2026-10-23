//! A client that outlives the broker it is talking to.
//!
//! [`Client`] holds connections built when it was created, so a broker going
//! away takes that client with it. A cluster survives losing a broker; until
//! now an application could not, and had to notice the failure and rebuild
//! everything itself.
//!
//! This holds one [`Client`] per broker it reaches (`nodes`), shared by every
//! role that broker plays -- entry, shard owner, redirect target, producer
//! leader -- and replaces a broker's client when it stops working. The one in
//! use for requests that go anywhere is rebuilt from the same seed list it
//! was created with.
//!
//! # Connections
//!
//! A broker's client starts with one QUIC connection and multiplexes every
//! stream onto it: publish and cache workers, subscriptions, watches and
//! one-off requests. It opens another only when those are saturated, up to
//! [`ClientConfig::cluster_conn_pool`](crate::ClientConfig::cluster_conn_pool).
//!
//! # Reconnecting is not resending
//!
//! The two are separated on purpose, and the names say which is which.
//!
//! [`ClusterClient::publish`] reconnects and reports the failed record to the
//! caller. Nothing that may have landed is sent twice, so nothing can arrive
//! twice — but the record that was in flight when the broker died is the
//! caller's problem. The one re-send is of a record a cached shard owner said
//! it did not apply.
//!
//! [`ClusterClient::publish_at_least_once`] reconnects *and sends the record
//! again*. A publish that failed after the broker had already written it will
//! then exist twice, and the broker cannot tell the difference: only the
//! application holds the identity that would make deduplication possible. The
//! name is the warning, and it is the guarantee the method actually provides.
//!
//! # Discovery adds, it never replaces
//!
//! A broker that supports it is asked which brokers a client may connect to,
//! and the answer is added to the endpoints this client will try. The
//! configured seeds always stay in that list. A client can therefore never end
//! up with fewer ways in than it was given, however wrong or stale the
//! cluster's answer turns out to be -- which is what makes it safe to take the
//! answer at all.
//!
//! # Names outlive addresses
//!
//! When no address in hand answers, the seeds given as `host:port` names and
//! the names brokers last advertised are resolved again and the new addresses
//! dialled. That is how a client gets back in after every broker has moved at
//! once, as in a rollout that restarts every pod.

mod batch;
mod cache;
mod cache_watch;
mod commit;
mod follow;
mod groups;
mod identity;
mod nodes;
mod publish;
mod retry;
mod routing;
mod seeds;
mod sharded;
mod subscribe;

pub use batch::{BatchReport, ShardPublish};
pub use follow::{ClusterCacheWatch, ClusterSubscription};
pub use retry::ReconnectPolicy;
pub use seeds::Seed;
pub use sharded::{
    ShardEvent, ShardOffsets, ShardedCacheWatch, ShardedCacheWatchItem, ShardedGroup,
    ShardedGroupRecord, ShardedSubscription,
};

pub(crate) use follow::resume_position;
pub(crate) use retry::{Attempt, Next, Retrying, next_step, route_went_stale, wants_reconnect};

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;

use crate::client::Client;
use crate::config::ClientConfig;
use crate::publish::{LearnWidth, StreamWidths};

/// How many times a subscribe or watch will follow a redirect before giving up.
///
/// A correct cluster needs one hop. More than that means the answer is moving
/// while it is being followed, and a bound is what turns "the cluster has not
/// settled" into an error the caller sees rather than a loop it does not.
const MAX_REDIRECTS: usize = 3;

/// A client that reconnects to another broker when the one it is using fails.
///
/// See `docs/multi-node-client.md` for what to configure and what this does on
/// its own. The short version:
///
/// ```rust,no_run
/// use std::time::Duration;
/// use felix_client::{AckMode, ClientConfig, ClusterClient, ReconnectPolicy};
///
/// # async fn example(quinn: quinn::ClientConfig) -> anyhow::Result<()> {
/// let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
/// config.auth_tenant_id = Some("acme".to_string());
/// config.auth_token = Some(std::env::var("FELIX_TOKEN")?);
/// // For a long-running client, set `config.token_provider` so reconnects
/// // get a fresh token.
///
/// // One name is enough; the rest are discovered. A name is resolved again
/// // when no broker the client knows answers, so prefer one to an IP.
/// let seeds = ["felix.prod.svc.cluster.local:5000".parse()?];
///
/// let client = ClusterClient::connect_seeds(
///     &seeds,
///     "broker.internal",
///     config,
///     ReconnectPolicy {
///         attempts: 5,
///         backoff: Duration::from_millis(200),
///         max_backoff: Duration::from_secs(2),
///         deadline: Some(Duration::from_secs(10)),
///     },
/// )
/// .await?;
///
/// client
///     .publish_at_least_once(
///         "acme",
///         "default",
///         "orders",
///         b"payload".to_vec(),
///         AckMode::PerMessage,
///     )
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct ClusterClient {
    /// What the application configured. Never removed from `endpoints`: the
    /// cluster's account of itself can be wrong, and these are the addresses
    /// someone chose deliberately. Shared with every identity derived from
    /// this client, so they re-resolve together.
    seeds: Arc<seeds::Seeds>,
    /// Everywhere worth trying: the seeds, plus whatever discovery has added.
    endpoints: RwLock<Vec<SocketAddr>>,
    policy: ReconnectPolicy,
    /// Replaced wholesale on reconnect. `RwLock` rather than a swap because a
    /// reconnect must exclude the publishes that would otherwise keep using the
    /// client being replaced.
    /// Shared with `widths`, which asks whichever client is in use.
    client: Arc<RwLock<Arc<Client>>>,
    /// Where a shard's publishes should go, learned from the acks of the ones
    /// that were forwarded.
    ///
    /// A forward is correct but costs a decrypt at the entry broker, a
    /// re-encrypt to the owner and a decrypt there -- roughly half the
    /// throughput per core (#536). The owner rides back on the ack, so the
    /// next batch for that shard can skip the hop.
    ///
    /// Keyed by shard, not by stream: a multi-shard stream has an owner per
    /// shard, and one entry for the whole stream would send every key to
    /// shard 0's owner. An unkeyed publish resolves to shard 0, a keyed one to
    /// `felix_wire::routing::shard_for` -- the function the broker routes
    /// with, shared so the two cannot drift.
    owners: RwLock<HashMap<ShardKey, Owner>>,
    /// How many shards each stream was placed with and how keys map onto
    /// them, as the cluster last said.
    ///
    /// The shard number picks the broker a keyed publish goes to, so a stale
    /// width can send two keys that now share a shard through two brokers,
    /// and so two writers. Kept like a plain client's widths: asked again on
    /// the same cadence against brokers that check declared widths, and
    /// dropped when a publish says the stream is gone or the width stale.
    widths: Arc<StreamWidths>,
    /// The broker that last served each shard's consumer groups, found by
    /// following `NotLeader`. Apart from `owners` because it is learned from a
    /// different answer, and a group redirect carries no fresher generation
    /// to arbitrate between the two.
    group_routes: RwLock<HashMap<ShardKey, Arc<Client>>>,
    /// Each cache's shard owners, asked once and dropped when a request to
    /// one fails. Apart from `owners` because a cache and a stream may share
    /// a name, and its entries carry no generation to compare.
    cache_routes: RwLock<HashMap<StreamKey, Arc<cache::CacheOwners>>>,
    /// The one client per broker that every field above draws from, so a
    /// broker costs one connection however many roles it plays.
    nodes: Arc<nodes::Nodes>,
}

impl ClusterClient {
    /// Connect to whichever seed answers, and remember the rest.
    pub async fn connect(
        seeds: &[SocketAddr],
        server_name: &str,
        config: ClientConfig,
    ) -> Result<Self> {
        Self::connect_with_policy(seeds, server_name, config, ReconnectPolicy::default()).await
    }

    /// [`ClusterClient::connect`] with an explicit reconnection policy.
    pub async fn connect_with_policy(
        seeds: &[SocketAddr],
        server_name: &str,
        config: ClientConfig,
        policy: ReconnectPolicy,
    ) -> Result<Self> {
        let seeds: Vec<Seed> = seeds.iter().copied().map(Seed::Addr).collect();
        Self::connect_seeds(&seeds, server_name, config, policy).await
    }

    /// Connect to whichever seed answers, where a seed may be a `host:port`
    /// name.
    ///
    /// Names are resolved now, and every address each one yields is tried.
    /// They are resolved again whenever no address this client holds
    /// answers, so a client configured with names recovers when every broker
    /// has moved to a new address at once. One with only IP seeds can then
    /// get back in only through the names brokers advertise.
    pub async fn connect_seeds(
        seeds: &[Seed],
        server_name: &str,
        config: ClientConfig,
        policy: ReconnectPolicy,
    ) -> Result<Self> {
        Self::connect_resolving(
            seeds,
            server_name,
            config,
            policy,
            Arc::new(seeds::Dns::new()),
        )
        .await
    }

    /// [`ClusterClient::connect_seeds`] resolving names through `lookup`.
    pub(crate) async fn connect_resolving(
        seeds_given: &[Seed],
        server_name: &str,
        config: ClientConfig,
        policy: ReconnectPolicy,
        lookup: Arc<dyn seeds::Lookup>,
    ) -> Result<Self> {
        if seeds_given.is_empty() {
            return Err(anyhow::anyhow!(
                "no broker addresses were given to connect to"
            ));
        }
        let nodes = Arc::new(nodes::Nodes::new(server_name, config, lookup));
        let seeds = Arc::new(seeds::Seeds::new(seeds_given.to_vec()));
        seeds.resolve(&nodes).await;
        let endpoints = seeds.addrs();
        if endpoints.is_empty() {
            let given: Vec<String> = seeds_given.iter().map(Seed::to_string).collect();
            return Err(anyhow::anyhow!(
                "none of the brokers given resolved to an address: {}",
                given.join(", ")
            ));
        }
        let client = nodes.connect_any(&endpoints).await?;
        let holder = Arc::new(RwLock::new(client));
        let cluster = Self {
            seeds,
            endpoints: RwLock::new(endpoints),
            policy,
            widths: cluster_widths(&holder),
            client: holder,
            owners: RwLock::new(HashMap::new()),
            group_routes: RwLock::new(HashMap::new()),
            cache_routes: RwLock::new(HashMap::new()),
            nodes,
        };
        cluster.discover().await;
        Ok(cluster)
    }

    /// Everywhere this client would try, seeds included.
    pub async fn endpoints(&self) -> Vec<SocketAddr> {
        self.endpoints.read().await.clone()
    }

    /// The client currently in use, for operations this wrapper does not cover.
    ///
    /// Subscriptions are the reason this is exposed: one is bound to the
    /// connection it was created on, so a reconnect cannot carry it across, and
    /// resuming it is the caller's to do with the offsets it has been given.
    pub async fn client(&self) -> Arc<Client> {
        Arc::clone(&*self.client.read().await)
    }

    /// The widths this client routes keyed publishes by.
    pub(crate) fn stream_widths(&self) -> Arc<StreamWidths> {
        Arc::clone(&self.widths)
    }

    pub(crate) fn policy(&self) -> &ReconnectPolicy {
        &self.policy
    }

    /// Ask the broker in use which brokers a client may connect to, and add
    /// them to the endpoints this client will try.
    ///
    /// Returns how many the cluster named. Zero is a legitimate answer: a
    /// broker with no cluster behind it, or one whose brokers do not advertise
    /// where clients reach them, has nothing to add. The configured seeds are
    /// kept regardless.
    ///
    /// Errors when the broker predates discovery, rather than reporting zero.
    /// A caller asking this question deserves to know the difference between
    /// "no brokers to name" and "cannot be asked".
    pub async fn refresh_topology(&self) -> Result<usize> {
        let client = self.client().await;
        let reported = client.topology().await?;
        self.seeds
            .set_advertised(reported.iter().map(|broker| broker.addr.clone()));
        let advertised: Vec<String> = reported.iter().map(|broker| broker.addr.clone()).collect();
        let discovered = self.nodes.resolve_all(&advertised).await;

        let mut endpoints = self.endpoints.write().await;
        // Rebuilt rather than appended to, so a broker that has left the
        // cluster stops being tried -- while the configured seeds stay whatever
        // the answer was.
        let mut next = discovered.clone();
        for seed in self.seeds.addrs() {
            if !next.contains(&seed) {
                next.push(seed);
            }
        }
        *endpoints = next;
        Ok(reported.len())
    }

    /// The client for one broker, shared with every other role that broker
    /// plays for this cluster client.
    pub(crate) async fn connect_to(&self, addr: SocketAddr) -> Result<Arc<Client>> {
        self.nodes.connect_to(addr).await
    }

    /// Where a broker that advertised `addr`, an address or a name, is
    /// reached now. See `Nodes::resolve`.
    pub(crate) async fn resolve(&self, advertised: &str) -> Option<SocketAddr> {
        self.nodes.resolve(advertised).await
    }

    /// Live connections to each broker this client holds one for.
    ///
    /// A broker starts at one, shared by every kind of traffic and every role
    /// it plays, and gains more only when the streams on it are saturated,
    /// up to [`ClientConfig::cluster_conn_pool`].
    pub async fn connections_per_node(&self) -> Vec<(SocketAddr, usize)> {
        self.nodes.connections_per_node().await
    }

    /// Replace `failed`, the client an operation just saw fail, with one
    /// connected to a broker that answers, trying `failed`'s broker last, and
    /// resolving names again when no address in hand answers.
    ///
    /// Does nothing when the client in use is no longer `failed`: someone
    /// else has already replaced it, and every operation that failed on the
    /// same broker must not each throw away the replacement and dial again.
    /// Held exclusively while dialling, so publishes queue behind it rather
    /// than racing to build several replacements for the same failure.
    pub(crate) async fn reconnect(&self, failed: &Arc<Client>) -> Result<()> {
        let (endpoints, stranded) = {
            let mut slot = self.client.write().await;
            if !Arc::ptr_eq(&slot, failed) {
                return Ok(());
            }
            // Read under the lock, so a reconnect that queued behind another
            // one dials what that one left, not what was there before it.
            let mut endpoints = self.endpoints.read().await.clone();
            // The broker in use just failed, so it is dialled afresh rather
            // than handed back from the shared clients. It goes last because
            // a draining broker still completes handshakes and would be
            // picked again until it exits.
            self.nodes.forget(&slot).await;
            let failed_addr = slot.dialled();
            endpoints.sort_by_key(|addr| *addr == failed_addr);
            match self.nodes.connect_any(&endpoints).await {
                Ok(client) => {
                    *slot = client;
                    (endpoints, None)
                }
                Err(err) => (endpoints, Some(err)),
            }
        };
        if let Some(err) = stranded
            && !self.reconnect_by_name(&endpoints, failed, err).await?
        {
            // Another caller replaced the client while this one was
            // resolving, and has run discovery for it.
            return Ok(());
        }
        // After the swap, and outside the write lock: the broker that answered
        // is the one that knows what the cluster looks like now, and a failover
        // is exactly when the answer has changed.
        self.discover().await;
        Ok(())
    }

    /// Every address in hand failed. Without this the client could only get
    /// back in through one of them, and after a rollout that moved every
    /// broker none will ever answer again.
    ///
    /// Resolves outside the client lock, since lookups can take seconds and
    /// every operation reads the client. `false` when someone else replaced
    /// `failed` meanwhile, which leaves theirs in place.
    async fn reconnect_by_name(
        &self,
        tried: &[SocketAddr],
        failed: &Arc<Client>,
        err: anyhow::Error,
    ) -> Result<bool> {
        let fresh: Vec<SocketAddr> = self
            .seeds
            .resolve(&self.nodes)
            .await
            .into_iter()
            .filter(|addr| !tried.contains(addr))
            .collect();
        let mut slot = self.client.write().await;
        if !Arc::ptr_eq(&slot, failed) {
            return Ok(false);
        }
        if fresh.is_empty() {
            return Err(err);
        }
        *slot = self.nodes.connect_any(&fresh).await.map_err(|by_name| {
            anyhow::anyhow!("{err:#}; nor did the names resolved again ({by_name:#})")
        })?;
        // Every address tried just failed, so only the new ones and the
        // configured seeds are kept. Discovery adds the rest back.
        let mut next = fresh;
        for seed in self.seeds.addrs() {
            if !next.contains(&seed) {
                next.push(seed);
            }
        }
        *self.endpoints.write().await = next;
        Ok(true)
    }

    /// Refresh if the broker supports it, and carry on if it does not.
    ///
    /// Discovery is an improvement on the seed list, never a precondition for
    /// using one: a cluster that cannot be asked leaves the client exactly as
    /// well off as it was before it asked.
    async fn discover(&self) {
        let client = self.client().await;
        if !client.supports_topology() {
            return;
        }
        if let Err(err) = self.refresh_topology().await {
            tracing::debug!(error = %err, "topology refresh failed; keeping the endpoints in hand");
        }
    }
}

/// A stream.
type StreamKey = (String, String, String);

/// One shard of one stream, as the owner cache keys it.
type ShardKey = (String, String, String, u32);

/// The broker that owns one shard of a stream, and a client connected to it.
struct Owner {
    node_id: String,
    /// The ownership epoch this was true for. A lower one is an older answer
    /// arriving late, and must not overwrite a newer one -- two brokers
    /// mid-rebalance would otherwise take turns replacing each other.
    generation: u64,
    client: Arc<Client>,
    /// The stream's width when this was learned; `None` for shard 0 of an
    /// unkeyed publish.
    width: Option<(u32, felix_wire::routing::ShardRouting)>,
}

#[cfg(test)]
mod tests;

/// A cluster client's widths, asked of whichever client is in use when the
/// question is asked. Re-asks go through that client's own re-ask stream.
///
/// Always declared and asked again, whatever the first broker negotiated:
/// re-asking needs only `stream_shards`, and each declaration is sent only
/// to a stream that negotiated it, and used for routing only by a client
/// whose broker checks it.
fn cluster_widths(client: &Arc<RwLock<Arc<Client>>>) -> Arc<StreamWidths> {
    let learn: LearnWidth = {
        let client = Arc::clone(client);
        Arc::new(move |tenant_id, namespace, stream| {
            let client = Arc::clone(&client);
            Box::pin(async move {
                let client = Arc::clone(&*client.read().await);
                client.stream_routing(&tenant_id, &namespace, &stream).await
            })
        })
    };
    let recheck: LearnWidth = {
        let client = Arc::clone(client);
        Arc::new(move |tenant_id, namespace, stream| {
            let client = Arc::clone(&client);
            Box::pin(async move {
                let client = Arc::clone(&*client.read().await);
                let recheck = client.publish_widths().recheck();
                recheck(tenant_id, namespace, stream).await
            })
        })
    };
    Arc::new(StreamWidths::new(learn, true).rechecking_with(recheck))
}
