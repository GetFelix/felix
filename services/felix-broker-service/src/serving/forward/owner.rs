//! The owner's side: applying a publish another broker forwarded.
//!
//! Ownership is re-checked here, against this broker's own view, and the
//! requester's generation has to match it exactly. That check is the whole
//! safety property: the requester resolved against a snapshot that may already
//! be wrong, and a broker that trusted it would write a shard it no longer
//! holds.
//!
//! A mismatch in either direction is answered, never guessed at:
//!
//! - **The requester is behind** — this broker no longer owns the shard, or owns
//!   it at a newer generation. `NotLeader`, naming the current owner.
//! - **The requester is ahead** — this broker's watch has not caught up.
//!   `StaleRoute`. It must not accept: it may already have lost the shard.
//!
//! Ownership says this broker *may* write the shard; it says nothing about
//! whether the client behind the forward may. That is the second check: the
//! request carries the client's own token, and it is verified here against
//! the same keys and the same action a direct publish would be. A forwarder
//! that skipped its check, or that is not a Felix broker at all, is refused
//! `Unauthorized`, and so is one that carries no credential.
use std::sync::Arc;

use async_trait::async_trait;
use felix_authz::{
    Action, CacheScope, Namespace, StreamName, TenantId, cache_resource, stream_resource,
};
use felix_broker::Broker;
use felix_router::{Resolution, ShardRouter};
use felix_wire::internal::{
    CacheOpKind, ErrorCode, ForwardCacheError, ForwardCacheOk, ForwardCacheOp, ForwardPublish,
    ForwardPublishError, ForwardPublishOk, InternalMessage, NotLeader,
};

use crate::serving::auth::BrokerAuth;
use crate::shards::lifecycle::fence::Fenced;
use crate::shards::routing::{Dispatch, IngressRouter};
use crate::shards::{ShardKey, ShardKind};
use felix_replication::peer::metrics;
use felix_replication::peer::server::PeerRequestHandler;

/// Applies forwarded publishes against the local broker.
pub struct ForwardingHandler {
    broker: Arc<Broker>,
    ingress: Arc<IngressRouter>,
    router: Arc<ShardRouter>,
    /// This broker's own internal address, so a `NotLeader` that names *this*
    /// node at a newer generation is one the requester can act on.
    advertise_addr: String,
    /// How far a majority of each shard's replica set has got.
    ///
    /// A forwarded publish is still a publish to this stream, so it owes the
    /// client the guarantee the stream asks for. Acknowledging on local
    /// durability alone made `Quorum` depend on which broker the client
    /// happened to reach — honoured when it talked to the leader, silently
    /// downgraded to `Leader` through any other broker, which is where a
    /// failover then lost the record.
    marks: Option<Arc<felix_replication::quorum::QuorumMarks>>,
    quorum_timeout: std::time::Duration,
    /// Verifies the client credential a forward carries, against the same
    /// keys a direct request is checked with.
    auth: Arc<BrokerAuth>,
}

impl ForwardingHandler {
    pub fn new(
        broker: Arc<Broker>,
        ingress: Arc<IngressRouter>,
        router: Arc<ShardRouter>,
        advertise_addr: String,
        marks: Option<Arc<felix_replication::quorum::QuorumMarks>>,
        quorum_timeout: std::time::Duration,
        auth: Arc<BrokerAuth>,
    ) -> Self {
        Self {
            broker,
            ingress,
            router,
            advertise_addr,
            marks,
            quorum_timeout,
            auth,
        }
    }

    pub(crate) async fn apply(&self, publish: ForwardPublish) -> InternalMessage {
        let correlation_id = publish.correlation_id;
        let key = ShardKey {
            tenant_id: publish.shard.tenant_id.clone(),
            namespace: publish.shard.namespace.clone(),
            stream: publish.shard.stream.clone(),
            shard: publish.shard.shard,
            kind: ShardKind::Stream,
        };

        // A forward can reach this broker between a move's fence and its
        // cut-over, or ahead of this broker's own routes. Either settles in
        // milliseconds, so it waits here rather than bouncing the requester.
        self.ingress.settle(&key, publish.shard.generation).await;
        if let Some(denial) = self.check_ownership(correlation_id, &key, publish.shard.generation) {
            return denial.into_publish_answer(correlation_id);
        }

        let resource = stream_resource(
            &TenantId::new(&key.tenant_id),
            &Namespace::new(&key.namespace),
            &StreamName::new(&key.stream),
        );
        // The publisher recorded here is the one this broker verified, not
        // anything the forwarding broker says.
        let publisher = match self
            .authorize(
                &publish.credential,
                &key.tenant_id,
                Action::StreamPublish,
                &resource,
            )
            .await
        {
            Ok(publisher) => publisher,
            Err(detail) => {
                metrics::record_served(metrics::OUTCOME_UNAUTHORIZED);
                return error(correlation_id, ErrorCode::Unauthorized, detail);
            }
        };

        let handle = match self
            .broker
            .resolve_stream_handle(&key.tenant_id, &key.namespace, &key.stream, key.shard)
            .await
        {
            Ok(handle) => handle,
            Err(err) => {
                metrics::record_served(metrics::OUTCOME_REFUSED);
                return error(correlation_id, ErrorCode::Unavailable, err.to_string());
            }
        };

        let fenced = match self.ingress.fence().admit(&key, publish.shard.generation) {
            Ok(fenced) => fenced,
            Err(refused) => {
                return self
                    .fenced(correlation_id, &key, publish.shard.generation, refused)
                    .await
                    .into_publish_answer(correlation_id);
            }
        };
        let published = self
            .broker
            .publish_batch_with_outcome(&handle, &publish.payloads, publisher.as_ref())
            .await;
        drop(fenced);
        match published {
            Ok(outcome) => {
                // The same wait the direct publish path makes. A forwarded
                // publish is still a publish to this stream, and the client on
                // the other end of the forward asked for the stream's guarantee,
                // not for whichever one this path happened to provide.
                if let Err(err) = felix_replication::quorum::await_quorum(
                    &handle,
                    Some(&key),
                    &outcome,
                    self.marks.as_deref(),
                    Some(self.ingress.as_ref()),
                    self.quorum_timeout,
                )
                .await
                {
                    metrics::record_served(metrics::OUTCOME_ERROR);
                    return error(correlation_id, ErrorCode::StorageFailed, err.to_string());
                }
                metrics::record_served(metrics::OUTCOME_OK);
                InternalMessage::ForwardPublishOk(ForwardPublishOk::new(
                    correlation_id,
                    outcome.offsets,
                ))
            }
            Err(err) => {
                // The owner accepted the request and the write failed. Distinct
                // from a refusal: the requester must not retry, because this
                // broker did try.
                metrics::record_served(metrics::OUTCOME_ERROR);
                error(correlation_id, ErrorCode::StorageFailed, err.to_string())
            }
        }
    }

    /// Serve a cache operation forwarded here because this broker owns the
    /// key's shard.
    ///
    /// The same ownership gates as a forwarded publish, for the same reason: a
    /// broker that served a cache key it no longer owns is the divergence this
    /// whole path exists to prevent.
    pub(crate) async fn apply_cache_op(&self, op: ForwardCacheOp) -> InternalMessage {
        let correlation_id = op.correlation_id;
        let key = ShardKey {
            tenant_id: op.shard.tenant_id.clone(),
            namespace: op.shard.namespace.clone(),
            stream: op.shard.stream.clone(),
            shard: op.shard.shard,
            kind: ShardKind::Cache,
        };

        // The same wait a forwarded publish makes, for the same reason.
        self.ingress.settle(&key, op.shard.generation).await;
        if let Some(denial) = self.check_ownership(correlation_id, &key, op.shard.generation) {
            return denial.into_cache_answer(correlation_id);
        }

        let action = match op.op {
            CacheOpKind::Get | CacheOpKind::CounterGet => Action::CacheRead,
            CacheOpKind::Put | CacheOpKind::Delete | CacheOpKind::CounterAdd => Action::CacheWrite,
        };
        let resource = cache_resource(
            &TenantId::new(&key.tenant_id),
            &Namespace::new(&key.namespace),
            &CacheScope::new(&key.stream),
        );
        if let Err(detail) = self
            .authorize(&op.credential, &key.tenant_id, action, &resource)
            .await
            .map(drop)
        {
            metrics::record_served(metrics::OUTCOME_UNAUTHORIZED);
            return InternalMessage::ForwardCacheError(ForwardCacheError {
                correlation_id,
                code: ErrorCode::Unauthorized,
                detail,
            });
        }

        // A read follows the lease as a write does: another broker may be
        // leading the shard by now, with writes this one never saw. A fleet
        // that reads without the lease confirms by a round after the value
        // instead.
        if matches!(op.op, CacheOpKind::Get | CacheOpKind::CounterGet)
            && !self.ingress.fence().lease_valid()
            && !felix_replication::quorum::read_skips_lease(
                &self.broker,
                &key,
                self.marks.as_deref(),
                Some(self.ingress.as_ref()),
            )
            .await
        {
            crate::cluster::lease::metrics::record_refusal(
                crate::cluster::lease::metrics::BOUNDARY_READ,
            );
            return self
                .fenced(
                    correlation_id,
                    &key,
                    op.shard.generation,
                    Fenced::LeaseLapsed,
                )
                .await
                .into_cache_answer(correlation_id);
        }

        let cache = self.broker.cache();
        let writes = matches!(op.op, CacheOpKind::Put | CacheOpKind::Delete);
        let fenced = if writes {
            match self.ingress.fence().admit(&key, op.shard.generation) {
                Ok(fenced) => Some(fenced),
                Err(refused) => {
                    return self
                        .fenced(correlation_id, &key, op.shard.generation, refused)
                        .await
                        .into_cache_answer(correlation_id);
                }
            }
        } else {
            None
        };
        let value = match op.op {
            CacheOpKind::Put => {
                let ttl = (op.ttl_ms > 0).then(|| std::time::Duration::from_millis(op.ttl_ms));
                cache
                    .put(
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        key.shard,
                        &op.key,
                        op.value,
                        ttl,
                    )
                    .await
                    .map(|()| None)
            }
            CacheOpKind::Get => {
                cache
                    .get(
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        key.shard,
                        &op.key,
                    )
                    .await
            }
            CacheOpKind::Delete => {
                cache
                    .delete(
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        key.shard,
                        &op.key,
                    )
                    .await
            }
            CacheOpKind::CounterAdd | CacheOpKind::CounterGet => {
                return self.apply_counter_op(op, &key).await;
            }
        };
        drop(fenced);
        // Refused by the store: never answered as a success, or the quorum
        // wait would pass on the old tail and ack a put no read can see.
        let value = match value {
            Ok(value) => value,
            Err(err) => {
                metrics::record_served(metrics::OUTCOME_ERROR);
                return InternalMessage::ForwardCacheError(ForwardCacheError {
                    correlation_id,
                    code: if writes {
                        ErrorCode::StorageFailed
                    } else {
                        ErrorCode::Unavailable
                    },
                    detail: err.to_string(),
                });
            }
        };

        // The same wait the requester's own path makes for a local write or
        // read: the client asked for the cache's guarantee, wherever the key
        // happens to live.
        if let Err(err) = felix_replication::quorum::await_cache_quorum(
            &self.broker,
            &key,
            self.marks.as_deref(),
            Some(self.ingress.as_ref()),
            self.quorum_timeout,
            if writes {
                felix_replication::quorum::Access::Write
            } else {
                felix_replication::quorum::Access::Read
            },
        )
        .await
        {
            metrics::record_served(metrics::OUTCOME_ERROR);
            return InternalMessage::ForwardCacheError(ForwardCacheError {
                correlation_id,
                // A read changed nothing, so it is simply not available here.
                code: if writes {
                    ErrorCode::StorageFailed
                } else {
                    ErrorCode::Unavailable
                },
                detail: err.to_string(),
            });
        }

        metrics::record_served(metrics::OUTCOME_OK);
        InternalMessage::ForwardCacheOk(ForwardCacheOk {
            correlation_id,
            value,
        })
    }

    /// Whether the client behind a forward may perform `action` on `resource`.
    ///
    /// The forwarder already asked this; the answer is not trusted because the
    /// owner cannot tell an honest forwarder from anything else that can reach
    /// its port. `Err` is the detail for an `Unauthorized` refusal. The token
    /// never appears in it.
    async fn authorize(
        &self,
        credential: &str,
        tenant_id: &str,
        action: Action,
        resource: &str,
    ) -> Result<Option<bytes::Bytes>, String> {
        if credential.is_empty() {
            return Err(
                "forwarded with no credential: the forwarding broker predates credentialed \
                 forwards, or stripped it"
                    .to_string(),
            );
        }
        let ctx = self
            .auth
            .authenticate(tenant_id, credential)
            .await
            .map_err(|err| format!("the publisher's credential was refused: {err}"))?;
        if ctx.tenant_id != tenant_id || !ctx.matcher.allows(action, resource) {
            return Err(format!(
                "the publisher's credential does not allow {action:?} on {resource}"
            ));
        }
        Ok(ctx.publisher)
    }

    /// Every check a forwarded request must pass before it touches storage.
    ///
    /// Shared by the publish and cache paths deliberately. These gates are the
    /// whole reason forwarding is safe — ownership, readiness, and an exact
    /// generation match — and two copies of them would eventually disagree
    /// about which writes a broker may accept.
    ///
    /// `None` means this broker owns `key` at exactly `claimed_generation`.
    /// `Some` is why it does not, for the caller to answer in its own shape.
    fn check_ownership(
        &self,
        correlation_id: u64,
        key: &ShardKey,
        claimed_generation: u64,
    ) -> Option<Denial> {
        match self.ingress.dispatch(key) {
            Dispatch::Local { .. } => {}
            Dispatch::Forward {
                node_id,
                advertise_addr,
                generation,
            } => {
                // The shard moved on. Answering `NotLeader` rather than
                // forwarding again is deliberate: a chain of brokers each
                // relaying to the next would make one request's latency and
                // failure modes unbounded. The requester decides.
                metrics::record_served(metrics::OUTCOME_NOT_LEADER);
                return Some(Denial::Answer(InternalMessage::NotLeader(NotLeader {
                    correlation_id,
                    node_id,
                    advertise_addr: advertise_addr.to_string(),
                    generation,
                })));
            }
            Dispatch::Unavailable(reason) => {
                metrics::record_served(metrics::OUTCOME_REFUSED);
                return Some(Denial::Refused {
                    code: ErrorCode::Unavailable,
                    detail: reason.to_string(),
                });
            }
        }

        // Local, but at which generation? `dispatch` already required local
        // readiness to match the router, so this reads the same number it
        // agreed on.
        let ours = match self
            .router
            .resolve(&crate::shards::routing::to_router_key(key))
        {
            Resolution::Local { generation } => generation,
            // Between the dispatch above and here the view changed. Refusing is
            // the only safe answer; the requester retries and gets a definite
            // one.
            _ => {
                metrics::record_served(metrics::OUTCOME_REFUSED);
                return Some(Denial::Refused {
                    code: ErrorCode::Unavailable,
                    detail: "ownership changed while the request was being served".to_string(),
                });
            }
        };

        if claimed_generation != ours {
            metrics::record_served(metrics::OUTCOME_REFUSED);
            return Some(if claimed_generation > ours {
                // The requester has seen a newer assignment than this broker.
                // Accepting would be writing a shard we may already have lost.
                Denial::Refused {
                    code: ErrorCode::StaleRoute,
                    detail: format!(
                        "this broker is at generation {ours}, the requester at {claimed_generation}"
                    ),
                }
            } else {
                Denial::Answer(InternalMessage::NotLeader(NotLeader {
                    correlation_id,
                    node_id: self.router.local_node_id().to_string(),
                    advertise_addr: self.advertise_addr.clone(),
                    generation: ours,
                }))
            });
        }

        None
    }

    /// A write that passed the ownership check and was then refused by the
    /// fence: the shard stopped serving here while the request was on its way
    /// in. Waits for the routes that say where it went, so the requester is
    /// sent there; refused, and retried by the requester, only if the move
    /// has not cut over within the hold.
    async fn fenced(
        &self,
        correlation_id: u64,
        key: &ShardKey,
        generation: u64,
        refused: Fenced,
    ) -> Denial {
        // A lapsed lease is not a move: no newer route is coming to wait for,
        // and this broker must not name itself the owner while it lasts.
        if refused == Fenced::LeaseLapsed {
            metrics::record_served(metrics::OUTCOME_REFUSED);
            return Denial::Refused {
                code: ErrorCode::Unavailable,
                detail: refused.to_string(),
            };
        }
        self.ingress.settle(key, generation.saturating_add(1)).await;
        if let Some(denial) = self.check_ownership(correlation_id, key, generation) {
            return denial;
        }
        metrics::record_served(metrics::OUTCOME_REFUSED);
        Denial::Refused {
            code: ErrorCode::Unavailable,
            detail: refused.to_string(),
        }
    }

    /// The counter half of a forwarded cache op: the delta and the sum both
    /// ride the envelope's value bytes as eight big-endian bytes.
    async fn apply_counter_op(&self, op: ForwardCacheOp, key: &ShardKey) -> InternalMessage {
        let correlation_id = op.correlation_id;
        let Some(counters) = self.broker.counters() else {
            // Refused rather than answered empty: a requester told a counter
            // does not exist, when the truth is this broker cannot count,
            // would trust an answer nothing stands behind.
            return InternalMessage::ForwardCacheError(ForwardCacheError {
                correlation_id,
                code: ErrorCode::Unavailable,
                detail: "this broker has no durable storage for counters".to_string(),
            });
        };
        let served = match op.op {
            CacheOpKind::CounterAdd => {
                let delta = match felix_storage::counter_log::decode_sum(&op.value) {
                    Ok(delta) => delta,
                    Err(err) => {
                        return InternalMessage::ForwardCacheError(ForwardCacheError {
                            correlation_id,
                            code: ErrorCode::Malformed,
                            detail: err.to_string(),
                        });
                    }
                };
                let _fenced = match self.ingress.fence().admit(key, op.shard.generation) {
                    Ok(fenced) => fenced,
                    Err(refused) => {
                        return self
                            .fenced(correlation_id, key, op.shard.generation, refused)
                            .await
                            .into_cache_answer(correlation_id);
                    }
                };
                let added = counters
                    .add(
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        key.shard,
                        &op.key,
                        delta,
                    )
                    .await;
                drop(_fenced);
                match added {
                    Ok((sum, offset)) => self
                        .counter_quorum(key, Some(offset + 1), "counter add")
                        .await
                        .map(|()| Some(sum))
                        .map_err(|detail| (ErrorCode::StorageFailed, detail)),
                    Err(err) => Err((ErrorCode::Unavailable, err.to_string())),
                }
            }
            _ => {
                let sum = counters
                    .get(
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        key.shard,
                        &op.key,
                    )
                    .await
                    .map_err(|err| (ErrorCode::Unavailable, err.to_string()));
                match sum {
                    Ok(sum) => self
                        .counter_quorum(key, None, "read")
                        .await
                        .map(|()| sum)
                        .map_err(|detail| (ErrorCode::Unavailable, detail)),
                    Err(err) => Err(err),
                }
            }
        };
        match served {
            Ok(sum) => {
                metrics::record_served(metrics::OUTCOME_OK);
                InternalMessage::ForwardCacheOk(ForwardCacheOk {
                    correlation_id,
                    value: sum.map(felix_storage::counter_log::encode_sum),
                })
            }
            Err((code, detail)) => InternalMessage::ForwardCacheError(ForwardCacheError {
                correlation_id,
                code,
                detail,
            }),
        }
    }

    /// [`felix_replication::quorum::await_counter_quorum`] for a forwarded
    /// counter op, as the requester's own path waits for a local one.
    async fn counter_quorum(
        &self,
        key: &ShardKey,
        end: Option<u64>,
        what: &'static str,
    ) -> Result<(), String> {
        felix_replication::quorum::await_counter_quorum(
            &self.broker,
            key,
            self.marks.as_deref(),
            Some(self.ingress.as_ref()),
            self.quorum_timeout,
            end,
            what,
        )
        .await
        .map_err(|err| err.to_string())
    }
}

#[async_trait]
impl PeerRequestHandler for ForwardingHandler {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        match request {
            InternalMessage::ForwardPublish(publish) => self.apply(publish).await,
            InternalMessage::ForwardCacheOp(op) => self.apply_cache_op(op).await,
            // Responses have no business arriving as requests, and a broker that
            // answered one would be inventing a request that was never made.
            other => error(
                other.correlation_id(),
                ErrorCode::Malformed,
                format!("{:?} is not a request", other.kind()),
            ),
        }
    }
}

/// Why a forwarded request is not this broker's to serve.
///
/// Kept abstract rather than pre-built, because the two forwarded paths answer
/// a refusal in different shapes: a requester matches on the message kind to
/// decide what happened, so a cache operation refused with a publish's error
/// type reads to it as a protocol violation rather than a refusal.
#[allow(clippy::large_enum_variant)] // Returned once and consumed at once.
enum Denial {
    /// Already complete. `NotLeader` is a routing answer and is the same
    /// message whichever path asked.
    Answer(InternalMessage),
    /// A refusal each path wraps in its own error kind.
    Refused { code: ErrorCode, detail: String },
}

impl Denial {
    fn into_publish_answer(self, correlation_id: u64) -> InternalMessage {
        match self {
            Self::Answer(message) => message,
            Self::Refused { code, detail } => error(correlation_id, code, detail),
        }
    }

    fn into_cache_answer(self, correlation_id: u64) -> InternalMessage {
        match self {
            Self::Answer(message) => message,
            Self::Refused { code, detail } => {
                InternalMessage::ForwardCacheError(ForwardCacheError {
                    correlation_id,
                    code,
                    detail,
                })
            }
        }
    }
}

fn error(correlation_id: u64, code: ErrorCode, detail: String) -> InternalMessage {
    InternalMessage::ForwardPublishError(ForwardPublishError {
        correlation_id,
        code,
        detail,
    })
}

#[cfg(test)]
mod tests;
