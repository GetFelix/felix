//! What a broker says about itself: its view of a shard, the subscriptions it
//! serves, and how the cluster is laid out.
//!
//! These ask the broker the client is using right now. `inspectShard` and
//! `listSubscriptions` are not forwarded, so an operator who wants every
//! broker's answer connects to each one. They need `node.view:cluster:*`.

use std::sync::Arc;

use felix_client::ShardKind;
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::args::u64_of;
use crate::client::Client;
use crate::errors::{classify, invalid};
use crate::types::or_null;

fn kind_of(kind: &str) -> Result<ShardKind> {
    match kind {
        "stream" => Ok(ShardKind::Stream),
        "cache" => Ok(ShardKind::Cache),
        other => Err(invalid(format!(
            r#"kind must be "stream" or "cache", got {other:?}"#
        ))),
    }
}

fn big(value: Option<u64>) -> Either<BigInt, Null> {
    or_null(value.map(BigInt::from))
}

/// A shard's assignment: who leads it, who holds copies, and where a move is
/// taking it.
#[napi(object)]
pub struct InspectedAssignment {
    pub generation: BigInt,
    pub leader: String,
    pub replicas: Vec<String>,
    pub draining: bool,
    pub successor: Either<String, Null>,
}

/// A promoted leader's fence: which replicas took its generation.
#[napi(object)]
pub struct InspectedFence {
    pub took: Vec<String>,
    pub pending: Vec<String>,
    pub attempts: u32,
    pub retry_in_ms: Either<BigInt, Null>,
}

/// The broker's lease, which a leader needs to serve writes.
#[napi(object)]
pub struct InspectedLease {
    pub held: bool,
    pub remaining_ms: BigInt,
}

/// One replica as the leader sees it.
#[napi(object)]
pub struct InspectedReplica {
    pub node_id: String,
    pub role: String,
    pub next_offset: Either<BigInt, Null>,
    pub lag: Either<BigInt, Null>,
    pub fence: Either<bool, Null>,
    pub state: String,
    pub halted: Either<String, Null>,
}

/// One broker's view of one shard.
#[napi(object)]
pub struct ShardInspection {
    /// The broker that answered. Empty when it is not in a cluster.
    pub node_id: String,
    /// How many shards the stream or cache has, as this broker sees it. `0`
    /// when it knows nothing of it.
    pub shards: u32,
    /// `leader`, `follower` or `none`.
    pub role: String,
    /// `unassigned`, `opening`, `fencing`, `active`, `draining`, `closed` or
    /// `failed`.
    pub phase: String,
    /// Whether this broker takes writes for the shard right now.
    pub serving: bool,
    /// Why not, when `serving` is false.
    pub reason: Either<String, Null>,
    pub detail: Either<String, Null>,
    pub generation: Either<BigInt, Null>,
    pub assignment: Either<InspectedAssignment, Null>,
    pub fence: Either<InspectedFence, Null>,
    pub lease: Either<InspectedLease, Null>,
    /// One past the last record in this broker's copy of the log.
    pub tail: Either<BigInt, Null>,
    /// The leader's commit mark; only a leader of a quorum shard has one.
    pub committed: Either<BigInt, Null>,
    pub accepted_generation: Either<BigInt, Null>,
    /// The leader's view of every other replica. Empty from a follower.
    pub replicas: Vec<InspectedReplica>,
}

impl From<felix_client::ShardInspection> for ShardInspection {
    fn from(view: felix_client::ShardInspection) -> Self {
        Self {
            node_id: view.node_id,
            shards: view.shards,
            role: view.role,
            phase: view.phase,
            serving: view.serving,
            reason: or_null(view.reason),
            detail: or_null(view.detail),
            generation: big(view.generation),
            assignment: or_null(view.assignment.map(|a| InspectedAssignment {
                generation: BigInt::from(a.generation),
                leader: a.leader,
                replicas: a.replicas,
                draining: a.draining,
                successor: or_null(a.successor),
            })),
            fence: or_null(view.fence.map(|f| InspectedFence {
                took: f.took,
                pending: f.pending,
                attempts: f.attempts,
                retry_in_ms: big(f.retry_in_ms),
            })),
            lease: or_null(view.lease.map(|l| InspectedLease {
                held: l.held,
                remaining_ms: BigInt::from(l.remaining_ms),
            })),
            tail: big(view.tail),
            committed: big(view.committed),
            accepted_generation: big(view.accepted_generation),
            replicas: view
                .replicas
                .into_iter()
                .map(|r| InspectedReplica {
                    node_id: r.node_id,
                    role: r.role,
                    next_offset: big(r.next_offset),
                    lag: big(r.lag),
                    fence: or_null(r.fence),
                    state: r.state,
                    halted: or_null(r.halted),
                })
                .collect(),
        }
    }
}

/// Which subscriptions `listSubscriptions` returns. Every field set narrows
/// the list; none set lists them all.
#[napi(object)]
pub struct SubscriptionFilter {
    pub tenant_id: Option<String>,
    pub namespace: Option<String>,
    pub stream: Option<String>,
    pub shard: Option<u32>,
    /// Only subscriptions made under this principal.
    pub principal: Option<String>,
    /// Only subscriptions whose queue has dropped records.
    pub dropping: Option<bool>,
}

impl From<SubscriptionFilter> for felix_client::SubscriptionFilter {
    fn from(filter: SubscriptionFilter) -> Self {
        Self {
            tenant_id: filter.tenant_id,
            namespace: filter.namespace,
            stream: filter.stream,
            shard: filter.shard,
            principal: filter.principal,
            dropping: filter.dropping.unwrap_or(false),
        }
    }
}

/// Where a page of `listSubscriptions` ended. Pass `nextCursor` back as is.
#[napi(object)]
pub struct SubscriptionCursor {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub shard: u32,
    pub subscriber_id: BigInt,
}

impl SubscriptionCursor {
    fn into_client(self) -> Result<felix_client::SubscriptionCursor> {
        Ok(felix_client::SubscriptionCursor {
            tenant_id: self.tenant_id,
            namespace: self.namespace,
            stream: self.stream,
            shard: self.shard,
            subscriber_id: u64_of(self.subscriber_id)?,
        })
    }
}

/// One subscription as the broker serving it sees it.
#[napi(object)]
pub struct InspectedSubscription {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub shard: u32,
    pub subscriber_id: BigInt,
    pub subscription_id: Either<BigInt, Null>,
    pub connection: Either<BigInt, Null>,
    pub peer: Either<String, Null>,
    pub principal: Either<String, Null>,
    /// What a publish does when the queue is full: `block`, `drop_new` or
    /// `drop_old`.
    pub policy: String,
    /// Batches waiting in the queue.
    pub depth: BigInt,
    /// Batches the queue holds at most.
    pub capacity: BigInt,
    /// Records the queue has dropped since the subscription started.
    pub dropped: BigInt,
    /// One past the last offset taken from the queue for delivery.
    pub position: Either<BigInt, Null>,
    /// The shard's next offset.
    pub tail: BigInt,
    pub age_ms: BigInt,
}

impl From<felix_client::InspectedSubscription> for InspectedSubscription {
    fn from(sub: felix_client::InspectedSubscription) -> Self {
        Self {
            tenant_id: sub.tenant_id,
            namespace: sub.namespace,
            stream: sub.stream,
            shard: sub.shard,
            subscriber_id: BigInt::from(sub.subscriber_id),
            subscription_id: big(sub.subscription_id),
            connection: big(sub.connection),
            peer: or_null(sub.peer),
            principal: or_null(sub.principal),
            policy: sub.policy,
            depth: BigInt::from(sub.depth),
            capacity: BigInt::from(sub.capacity),
            dropped: BigInt::from(sub.dropped),
            position: big(sub.position),
            tail: BigInt::from(sub.tail),
            age_ms: BigInt::from(sub.age_ms),
        }
    }
}

/// One page of `listSubscriptions`.
#[napi(object)]
pub struct SubscriptionsPage {
    /// The broker that answered. Empty when it is not in a cluster.
    pub node_id: String,
    pub subscriptions: Vec<InspectedSubscription>,
    /// Pass this back for the next page. Null on the last one.
    pub next_cursor: Either<SubscriptionCursor, Null>,
}

/// A broker a client may connect to.
#[napi(object)]
pub struct BrokerEndpoint {
    pub node_id: String,
    /// `host:port`, as the broker advertises it to clients.
    pub addr: String,
}

/// How many shards a stream has and how keys map onto them.
#[napi(object)]
pub struct StreamRouting {
    pub shards: u32,
    /// `"modulo"` or `"jump_hash"`.
    pub routing: String,
}

/// Who owns one shard, as the broker's routing snapshot sees it.
#[napi(object)]
pub struct ShardOwner {
    pub shard: u32,
    /// Null when no broker can serve the shard right now (see `unavailable`),
    /// or when the broker answering is not in a cluster.
    pub node_id: Either<String, Null>,
    pub addr: Either<String, Null>,
    pub generation: BigInt,
    pub unavailable: Either<String, Null>,
}

/// What the broker in use advertised. Asking for something it did not
/// advertise fails without sending anything.
#[napi(object)]
pub struct Supports {
    pub inspect: bool,
    pub topology: bool,
    pub ack_on_commit: bool,
    pub stream_shards: bool,
    pub cache_shards: bool,
    pub shard_owners: bool,
}

#[napi]
impl Client {
    /// Which of the cluster questions the broker in use will answer.
    #[napi]
    pub async fn supports(&self) -> Result<Supports> {
        let connection = self.cluster()?.client().await;
        Ok(Supports {
            inspect: connection.supports_inspect(),
            topology: connection.supports_topology(),
            ack_on_commit: connection.supports_ack_on_commit(),
            stream_shards: connection.supports_stream_shards(),
            cache_shards: connection.supports_cache_shards(),
            shard_owners: connection.supports_shard_owners(),
        })
    }

    /// The broker in use, view of one shard: phase, generation, fence, lease,
    /// tail and, where it leads, every replica's position. `kind` is
    /// `"stream"` or `"cache"` and `name` the stream or cache. Needs
    /// `node.view:cluster:*`, and rejects when the broker did not advertise
    /// inspection.
    #[napi]
    pub async fn inspect_shard(
        &self,
        kind: String,
        tenant_id: String,
        namespace: String,
        name: String,
        shard: u32,
    ) -> Result<ShardInspection> {
        let kind = kind_of(&kind)?;
        let connection = self.cluster()?.client().await;
        connection
            .inspect_shard(kind, &tenant_id, &namespace, &name, shard)
            .await
            .map(Into::into)
            .map_err(classify)
    }

    /// One page of the subscriptions the broker in use serves: each one's
    /// queue depth and capacity, overflow policy, records dropped and
    /// position against the shard's tail. `limit` defaults to 100 and the
    /// broker caps it at 1000. Needs `node.view:cluster:*`.
    #[napi]
    pub async fn list_subscriptions(
        &self,
        filter: Option<SubscriptionFilter>,
        limit: Option<u32>,
        cursor: Option<SubscriptionCursor>,
    ) -> Result<SubscriptionsPage> {
        let filter = filter.map(Into::into).unwrap_or_default();
        let cursor = cursor.map(SubscriptionCursor::into_client).transpose()?;
        let connection = self.cluster()?.client().await;
        let page = connection
            .list_subscriptions(filter, limit, cursor)
            .await
            .map_err(classify)?;
        Ok(SubscriptionsPage {
            node_id: page.node_id,
            subscriptions: page.subscriptions.into_iter().map(Into::into).collect(),
            next_cursor: or_null(page.next_cursor.map(|c| SubscriptionCursor {
                tenant_id: c.tenant_id,
                namespace: c.namespace,
                stream: c.stream,
                shard: c.shard,
                subscriber_id: BigInt::from(c.subscriber_id),
            })),
        })
    }

    /// The brokers the cluster says a client may connect to. Empty for a
    /// broker with no cluster behind it. `refreshTopology` adds them to the
    /// endpoints this client tries.
    #[napi]
    pub async fn topology(&self) -> Result<Vec<BrokerEndpoint>> {
        let connection = self.cluster()?.client().await;
        let brokers = connection.topology().await.map_err(classify)?;
        Ok(brokers
            .into_iter()
            .map(|b| BrokerEndpoint {
                node_id: b.node_id,
                addr: b.addr,
            })
            .collect())
    }

    /// How many shards a stream has and how its keys map onto them. `0`
    /// shards means the broker knows nothing of the stream.
    #[napi]
    pub async fn stream_routing(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
    ) -> Result<StreamRouting> {
        let connection = self.cluster()?.client().await;
        let (shards, routing) = connection
            .stream_routing(&tenant_id, &namespace, &stream)
            .await
            .map_err(classify)?;
        let routing = match routing {
            felix_client::ShardRouting::Modulo => "modulo",
            felix_client::ShardRouting::JumpHash => "jump_hash",
        };
        Ok(StreamRouting {
            shards,
            routing: routing.to_string(),
        })
    }

    /// Which broker owns each shard of a stream or cache, in shard order, as
    /// the broker in use sees it. The answer is a snapshot and can be stale.
    #[napi]
    pub async fn shard_owners(
        &self,
        kind: String,
        tenant_id: String,
        namespace: String,
        name: String,
    ) -> Result<Vec<ShardOwner>> {
        let kind = kind_of(&kind)?;
        let connection: Arc<_> = self.cluster()?.client().await;
        let owners = connection
            .shard_owners(&tenant_id, &namespace, &name, kind)
            .await
            .map_err(classify)?;
        Ok(owners
            .into_iter()
            .map(|o| ShardOwner {
                shard: o.shard,
                node_id: or_null(o.node_id),
                addr: or_null(o.addr),
                generation: BigInt::from(o.generation),
                unavailable: or_null(o.unavailable),
            })
            .collect())
    }

    /// How many shards a cache was placed with. A prefix watch reads one
    /// shard, so this is how many a whole-cache watch covers. `0` means the
    /// broker knows nothing of the cache.
    #[napi]
    pub async fn cache_shards(
        &self,
        tenant_id: String,
        namespace: String,
        cache: String,
    ) -> Result<u32> {
        let connection = self.cluster()?.client().await;
        connection
            .cache_shards(&tenant_id, &namespace, &cache)
            .await
            .map_err(classify)
    }
}
