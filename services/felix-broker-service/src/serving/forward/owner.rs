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
//! - **The requester named another incarnation** — its route was for a
//!   stream or cache deleted and created again under the same name, which
//!   can start at the generation the deleted one had. `StaleRoute`, in either
//!   direction: generations of two incarnations say nothing about each other.
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
    Action, CacheKeys, CacheScope, Namespace, PermissionMatcher, StreamName, TenantId,
    cache_resource, stream_resource,
};
use felix_broker::{Broker, SequenceReuse};
use felix_replication::engine::{CommitError, Engine};
use felix_router::{Resolution, ShardRouter};
use felix_wire::internal::{
    CacheOpKind, ErrorCode, ForwardCacheCondition, ForwardCacheError, ForwardCacheOk,
    ForwardCacheOp, ForwardCacheOutcome, ForwardLease, ForwardProducer, ForwardPublish,
    ForwardPublishError, ForwardPublishOk, InternalMessage, LeaseForwardCacheOk, NotLeader,
    ShardRef,
};

use crate::serving::auth::{AuthContext, BrokerAuth};
use crate::serving::lease::LeaseMint;
use crate::shards::lifecycle::fence::Fenced;
use crate::shards::routing::{Dispatch, IngressRouter, RaftRefusal, StreamEngine};
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
    /// What a write waits on before it is acknowledged.
    ///
    /// A forwarded publish is still a publish to this stream, so it owes the
    /// client the guarantee the stream asks for. Acknowledging on local
    /// durability alone made `Quorum` depend on which broker the client
    /// happened to reach — honoured when it talked to the leader, silently
    /// downgraded to `Leader` through any other broker, which is where a
    /// failover then lost the record.
    engine: Arc<StreamEngine>,
    quorum_timeout: std::time::Duration,
    /// Verifies the client credential a forward carries, against the same
    /// keys a direct request is checked with.
    auth: Arc<BrokerAuth>,
}

impl ForwardingHandler {
    pub fn new(
        broker: Arc<Broker>,
        engine: Arc<StreamEngine>,
        router: Arc<ShardRouter>,
        advertise_addr: String,
        quorum_timeout: std::time::Duration,
        auth: Arc<BrokerAuth>,
    ) -> Self {
        let ingress = engine
            .serving()
            .cloned()
            .expect("a forwarding handler needs an engine with shard routing");
        Self {
            broker,
            ingress,
            router,
            advertise_addr,
            engine,
            quorum_timeout,
            auth,
        }
    }

    #[cfg(test)]
    pub(crate) fn engine(&self) -> &Arc<StreamEngine> {
        &self.engine
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
        // A Raft shard has no move to wait out: its group says who leads.
        let on_raft = self.engine.on_raft(&key);
        if !on_raft {
            self.ingress
                .settle(&key, publish.shard.generation, publish.shard.incarnation)
                .await;
        }
        if let Some(denial) = self.check_ownership(correlation_id, &key, &publish.shard) {
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
                |matcher| matcher.allows(Action::StreamPublish, &resource),
            )
            .await
        {
            Ok(ctx) => ctx.publisher,
            // A sequenced forward's sender tells its device the answer
            // outright, so a token that could not be checked right now is not
            // reported as refused. Older senders get what they always got.
            Err(Denied {
                detail,
                unchecked: true,
            }) if publish.producer.is_some() => {
                metrics::record_served(metrics::OUTCOME_REFUSED);
                return error(correlation_id, ErrorCode::Unavailable, detail);
            }
            Err(Denied { detail, .. }) => {
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

        if on_raft {
            return self.apply_on_raft(&key, &handle, publish).await;
        }
        let fenced = match self.ingress.fence().admit(&key, publish.shard.generation) {
            Ok(fenced) => fenced,
            Err(refused) => {
                return self
                    .fenced(correlation_id, &key, &publish.shard, refused)
                    .await
                    .into_publish_answer(correlation_id);
            }
        };
        let published = match publish.producer {
            // A re-send of a batch this log already holds is answered with the
            // held batch's offsets, and still waits for the quorum below: the
            // first attempt may not have reached a majority yet.
            Some(producer) => {
                self.apply_sequenced(&handle, &publish, producer, publisher)
                    .await
            }
            None => {
                self.broker
                    .publish_batch_with_outcome(
                        &handle,
                        &publish.payloads,
                        publisher.as_ref(),
                        publish.headers.as_deref(),
                    )
                    .await
            }
        };
        drop(fenced);
        match published {
            Ok(outcome) => {
                // The same wait the direct publish path makes. A forwarded
                // publish is still a publish to this stream, and the client on
                // the other end of the forward asked for the stream's guarantee,
                // not for whichever one this path happened to provide.
                if let Err(err) = self
                    .engine
                    .await_commit(
                        &handle,
                        Some(&key),
                        &outcome,
                        publish.shard.generation,
                        self.quorum_timeout,
                    )
                    .await
                {
                    metrics::record_served(metrics::OUTCOME_ERROR);
                    // A sequenced forward can be sent again to whoever leads
                    // now, so it is told the leadership moved rather than that
                    // the write failed.
                    let deposed = matches!(
                        err.downcast_ref::<felix_replication::quorum::QuorumError>(),
                        Some(felix_replication::quorum::QuorumError::LeadershipLost { .. })
                    );
                    let code = if deposed && publish.producer.is_some() {
                        ErrorCode::LeadershipLost
                    } else {
                        ErrorCode::StorageFailed
                    };
                    return error(correlation_id, code, err.to_string());
                }
                metrics::record_served(metrics::OUTCOME_OK);
                InternalMessage::ForwardPublishOk(ForwardPublishOk::new(
                    correlation_id,
                    outcome.offsets,
                ))
            }
            // The ingress broker saw the finalize before this one did. Nothing
            // was written; it answers the client with `feature_not_enabled`.
            Err(err @ felix_broker::BrokerError::RecordHeadersNotEnabled) => {
                metrics::record_served(metrics::OUTCOME_REFUSED);
                error(correlation_id, ErrorCode::UnsupportedKind, err.to_string())
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

    /// Propose a forwarded batch to the shard's Raft group, which this
    /// broker leads, and answer at its commit. The requester's generation is
    /// the term it saw; the batch is written at the term served here now.
    async fn apply_on_raft(
        &self,
        key: &ShardKey,
        handle: &felix_broker::StreamHandle,
        publish: ForwardPublish,
    ) -> InternalMessage {
        let correlation_id = publish.correlation_id;
        if publish.headers.is_some() {
            metrics::record_served(metrics::OUTCOME_REFUSED);
            let what = "record headers";
            let err = felix_broker::BrokerError::NotOnEngineStream { what };
            return error(correlation_id, ErrorCode::UnsupportedKind, err.to_string());
        }
        let records = publish.payloads.len() as u64;
        let Some(term) = self.engine.raft_term(key) else {
            metrics::record_served(metrics::OUTCOME_REFUSED);
            let detail = "this broker stopped leading the shard's Raft group";
            return error(correlation_id, ErrorCode::Unavailable, detail.to_string());
        };
        // Deposed since the ownership check, or the group is taking no
        // more. Nothing was written, so the requester may send it again.
        let not_taken = |refused: RaftRefusal| {
            metrics::record_served(metrics::OUTCOME_REFUSED);
            error(correlation_id, ErrorCode::Unavailable, refused.to_string())
        };
        let committed = match publish.producer {
            Some(producer) => {
                let turn = self
                    .broker
                    .producer_turn(handle, producer.producer_id)
                    .await;
                let proposed = self.engine.publish_idempotent_on_raft(
                    &self.broker,
                    handle,
                    key,
                    term,
                    turn,
                    producer.sequence,
                    SequenceReuse::Refuse,
                    publish.payloads,
                    self.quorum_timeout,
                );
                match proposed {
                    Some(Ok(proposed)) => proposed.complete().await.map_err(|err| {
                        // As on the hybrid: a sequenced forward can be sent
                        // again to whoever leads now.
                        let code = match err {
                            CommitError::LeadershipLost { .. } => ErrorCode::LeadershipLost,
                            _ => ErrorCode::StorageFailed,
                        };
                        (code, err.to_string())
                    }),
                    // The sequence was refused, as the hybrid refuses it.
                    Some(Err(RaftRefusal::Broker(
                        err @ (felix_broker::BrokerError::SequenceGap { .. }
                        | felix_broker::BrokerError::UnknownProducer { .. }
                        | felix_broker::BrokerError::SequenceExpired { .. }
                        | felix_broker::BrokerError::SequenceReused { .. }),
                    ))) => {
                        metrics::record_served(metrics::OUTCOME_ERROR);
                        return error(correlation_id, ErrorCode::StorageFailed, err.to_string());
                    }
                    Some(Err(refused)) => return not_taken(refused),
                    None => return not_taken(RaftRefusal::NotLeading { term }),
                }
            }
            None => {
                let proposed = self.engine.publish_on_raft(
                    &self.broker,
                    handle,
                    key,
                    term,
                    publish.payloads,
                    self.quorum_timeout,
                );
                match proposed {
                    Some(Ok(proposed)) => proposed
                        .complete()
                        .await
                        .map_err(|err| (ErrorCode::StorageFailed, err.to_string())),
                    Some(Err(refused)) => return not_taken(refused),
                    None => return not_taken(RaftRefusal::NotLeading { term }),
                }
            }
        };
        match committed {
            Ok(first) => {
                metrics::record_served(metrics::OUTCOME_OK);
                InternalMessage::ForwardPublishOk(ForwardPublishOk::new(
                    correlation_id,
                    first.map(|first| (first, first + records.saturating_sub(1))),
                ))
            }
            // The batch may still commit, so this is not a refusal.
            Err((code, detail)) => {
                metrics::record_served(metrics::OUTCOME_ERROR);
                error(correlation_id, code, detail)
            }
        }
    }

    /// Write a forwarded batch as its producer's idempotent batch.
    async fn apply_sequenced(
        &self,
        handle: &felix_broker::StreamHandle,
        publish: &ForwardPublish,
        producer: ForwardProducer,
        publisher: Option<bytes::Bytes>,
    ) -> felix_broker::Result<felix_broker::PublishOutcome> {
        let claimed = self
            .broker
            .claim_batch_idempotent(
                handle,
                producer.producer_id,
                producer.sequence,
                &publish.payloads,
                SequenceReuse::Refuse,
                publisher.as_ref(),
                publish.headers.as_deref(),
            )
            .await?;
        Ok(self.broker.complete_idempotent(claimed).await?.outcome)
    }

    /// Serve a cache operation forwarded here because this broker owns the
    /// key's shard.
    ///
    /// The same ownership gates as a forwarded publish, for the same reason: a
    /// broker that served a cache key it no longer owns is the divergence this
    /// whole path exists to prevent.
    pub(crate) async fn apply_cache_op(&self, op: ForwardCacheOp) -> InternalMessage {
        self.apply_cache(op, false).await
    }

    /// A conditional put that also mints a lease token if it applies, the way
    /// a client connected here would get one.
    pub(crate) async fn apply_lease_cache_op(&self, op: ForwardCacheOp) -> InternalMessage {
        self.apply_cache(op, true).await
    }

    async fn apply_cache(&self, op: ForwardCacheOp, lease: bool) -> InternalMessage {
        let correlation_id = op.correlation_id;
        let key = ShardKey {
            tenant_id: op.shard.tenant_id.clone(),
            namespace: op.shard.namespace.clone(),
            stream: op.shard.stream.clone(),
            shard: op.shard.shard,
            kind: ShardKind::Cache,
        };

        // The same wait a forwarded publish makes, for the same reason.
        self.ingress
            .settle(&key, op.shard.generation, op.shard.incarnation)
            .await;
        if let Some(denial) = self.check_ownership(correlation_id, &key, &op.shard) {
            return denial.into_cache_answer(correlation_id);
        }

        let action = match op.op {
            CacheOpKind::Get | CacheOpKind::CounterGet => Action::CacheRead,
            CacheOpKind::Put | CacheOpKind::Delete | CacheOpKind::CounterAdd => Action::CacheWrite,
        };
        // Re-checked with the key, so an origin that checked only the cache
        // cannot widen a key grant here.
        let (tenant, namespace, cache) = (
            TenantId::new(&key.tenant_id),
            Namespace::new(&key.namespace),
            CacheScope::new(&key.stream),
        );
        let resource = format!(
            "{} key {:?}",
            cache_resource(&tenant, &namespace, &cache),
            op.key
        );
        let caller = match self
            .authorize(
                &op.credential,
                &key.tenant_id,
                action,
                &resource,
                |matcher| {
                    matcher.allows_cache_keys(
                        action,
                        &tenant,
                        &namespace,
                        &cache,
                        CacheKeys::Key(&op.key),
                    )
                },
            )
            .await
        {
            Ok(caller) => caller,
            Err(Denied { detail, .. }) => {
                metrics::record_served(metrics::OUTCOME_UNAUTHORIZED);
                return InternalMessage::ForwardCacheError(ForwardCacheError {
                    correlation_id,
                    code: ErrorCode::Unauthorized,
                    detail,
                });
            }
        };
        // Planned from the credential the owner verified itself, before the
        // write, so a refused mint writes nothing.
        let mint = if lease {
            match self.lease_mint(&op, &caller, &key) {
                Ok(mint) => Some(mint),
                Err((code, detail)) => {
                    metrics::record_served(metrics::OUTCOME_UNAUTHORIZED);
                    return InternalMessage::ForwardCacheError(ForwardCacheError {
                        correlation_id,
                        code,
                        detail,
                    });
                }
            }
        } else {
            None
        };
        // Taken before the write, as on the local path.
        let entry_expires_at = (op.ttl_ms > 0).then(|| {
            felix_common::clock::system_time() + std::time::Duration::from_millis(op.ttl_ms)
        });

        // A read follows the lease as a write does: another broker may be
        // leading the shard by now, with writes this one never saw. A fleet
        // that reads without the lease confirms by a round after the value
        // instead.
        if matches!(op.op, CacheOpKind::Get | CacheOpKind::CounterGet)
            && !self.ingress.fence().lease_valid()
            && !self.engine.read_skips_lease(&self.broker, &key).await
        {
            crate::cluster::lease::metrics::record_refusal(
                crate::cluster::lease::metrics::BOUNDARY_READ,
            );
            return self
                .fenced(correlation_id, &key, &op.shard, Fenced::LeaseLapsed)
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
                        .fenced(correlation_id, &key, &op.shard, refused)
                        .await
                        .into_cache_answer(correlation_id);
                }
            }
        } else {
            None
        };
        let at = if writes {
            self.engine
                .cache_write_at(&self.broker, &key, op.shard.generation)
                .await
        } else {
            None
        };
        let answered = match (op.op, op.condition) {
            (CacheOpKind::Put, None) => {
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
                    .map(|()| (None, None))
            }
            (CacheOpKind::Get, None) => cache
                .get(
                    &key.tenant_id,
                    &key.namespace,
                    &key.stream,
                    key.shard,
                    &op.key,
                )
                .await
                .map(|value| (value, None)),
            (CacheOpKind::Delete, None) => cache
                .delete(
                    &key.tenant_id,
                    &key.namespace,
                    &key.stream,
                    key.shard,
                    &op.key,
                )
                .await
                .map(|value| (value, None)),
            (
                CacheOpKind::Put,
                Some(
                    condition @ (ForwardCacheCondition::Absent | ForwardCacheCondition::Version(_)),
                ),
            ) => {
                let ttl = (op.ttl_ms > 0).then(|| std::time::Duration::from_millis(op.ttl_ms));
                let condition = match condition {
                    ForwardCacheCondition::Version(version) => {
                        felix_storage::CacheCondition::Version(version)
                    }
                    _ => felix_storage::CacheCondition::Absent,
                };
                cache
                    .put_if(
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        key.shard,
                        &op.key,
                        op.value,
                        ttl,
                        condition,
                    )
                    .await
                    .map(|written| (None, Some(outcome(written))))
            }
            (CacheOpKind::Delete, Some(ForwardCacheCondition::Version(version))) => cache
                .delete_if(
                    &key.tenant_id,
                    &key.namespace,
                    &key.stream,
                    key.shard,
                    &op.key,
                    version,
                )
                .await
                .map(|written| (None, Some(outcome(written)))),
            (CacheOpKind::Get, Some(ForwardCacheCondition::Unconditional)) => cache
                .get_versioned(
                    &key.tenant_id,
                    &key.namespace,
                    &key.stream,
                    key.shard,
                    &op.key,
                )
                .await
                .map(|found| {
                    let version = found.as_ref().map(|found| found.version);
                    (
                        found.map(|found| found.value),
                        Some(ForwardCacheOutcome {
                            applied: true,
                            version,
                        }),
                    )
                }),
            (CacheOpKind::CounterAdd | CacheOpKind::CounterGet, None) => {
                return self.apply_counter_op(op, &key).await;
            }
            (kind, Some(condition)) => {
                metrics::record_served(metrics::OUTCOME_ERROR);
                return InternalMessage::ForwardCacheError(ForwardCacheError {
                    correlation_id,
                    code: ErrorCode::Malformed,
                    detail: format!("{kind:?} cannot carry {condition:?}"),
                });
            }
        };
        drop(fenced);
        // Refused by the store: never answered as a success, or the quorum
        // wait would pass on the old tail and ack a put no read can see.
        let (value, outcome) = match answered {
            Ok(answered) => answered,
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
        if let Err(err) = self
            .engine
            .await_cache_quorum(
                &self.broker,
                &key,
                self.quorum_timeout,
                if writes {
                    felix_replication::quorum::Access::Write
                } else {
                    felix_replication::quorum::Access::Read
                },
                at,
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
        if let (Some(mint), Some(outcome)) = (mint, outcome) {
            let lease = match outcome {
                ForwardCacheOutcome {
                    applied: true,
                    version: Some(version),
                } => mint.mint(version, entry_expires_at),
                _ => ForwardLease::NotMinted,
            };
            return InternalMessage::LeaseForwardCacheOk(LeaseForwardCacheOk {
                correlation_id,
                outcome,
                lease,
            });
        }
        InternalMessage::ForwardCacheOk(ForwardCacheOk {
            correlation_id,
            value,
            outcome,
        })
    }

    /// What a lease token would grant the caller of `op`, or the refusal.
    fn lease_mint(
        &self,
        op: &ForwardCacheOp,
        caller: &AuthContext,
        key: &ShardKey,
    ) -> Result<LeaseMint, (ErrorCode, String)> {
        if !matches!(
            (op.op, op.condition),
            (
                CacheOpKind::Put,
                Some(ForwardCacheCondition::Absent | ForwardCacheCondition::Version(_))
            )
        ) {
            return Err((
                ErrorCode::Malformed,
                "only a conditional put mints a lease token".to_string(),
            ));
        }
        let tokens = self.auth.lease_tokens().ok_or_else(|| {
            (
                ErrorCode::Unauthorized,
                "this broker does not mint lease tokens".to_string(),
            )
        })?;
        LeaseMint::plan(tokens, caller, &key.namespace, &key.stream, &op.key)
            .map_err(|refusal| (ErrorCode::Unauthorized, refusal.to_string()))
    }

    /// Whether the client behind a forward may perform `action` on `resource`,
    /// by `allowed` over its verified grants. `action` and `resource` only
    /// name the request in a refusal.
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
        allowed: impl FnOnce(&PermissionMatcher) -> bool,
    ) -> Result<AuthContext, Denied> {
        if credential.is_empty() {
            return Err(Denied::refused(
                "forwarded with no credential: the forwarding broker predates credentialed \
                 forwards, or stripped it"
                    .to_string(),
            ));
        }
        let ctx = self
            .auth
            .authenticate(tenant_id, credential)
            .await
            .map_err(|err| Denied {
                // Anything but a verdict on the token itself, such as the
                // tenant's keys being unreachable.
                unchecked: !(err.is::<felix_authz::AuthzError>()
                    || err.is::<crate::serving::auth::CredentialRefused>()
                    || err.is::<crate::serving::revocation::Revoked>()),
                detail: format!("the publisher's credential was refused: {err}"),
            })?;
        if ctx.tenant_id != tenant_id || !allowed(&ctx.matcher) {
            return Err(Denied::refused(format!(
                "the publisher's credential does not allow {action:?} on {resource}"
            )));
        }
        Ok(ctx)
    }

    /// Every check a forwarded request must pass before it touches storage.
    ///
    /// Shared by the publish and cache paths deliberately. These gates are the
    /// whole reason forwarding is safe — ownership, readiness, and an exact
    /// generation match — and two copies of them would eventually disagree
    /// about which writes a broker may accept.
    ///
    /// `None` means this broker owns `key` at exactly the claimed generation,
    /// and at the claimed incarnation when the requester named one. `Some` is
    /// why it does not, for the caller to answer in its own shape.
    fn check_ownership(
        &self,
        correlation_id: u64,
        key: &ShardKey,
        claimed: &ShardRef,
    ) -> Option<Denial> {
        let claimed_generation = claimed.generation;
        match self.ingress.dispatch(key) {
            // The group's term is the whole check: the requester's number is
            // the term it saw, and the write is proposed at the one led now.
            Dispatch::Local { .. } if self.engine.on_raft(key) => return None,
            Dispatch::Local { .. } => {}
            Dispatch::Forward {
                node_id,
                advertise_addr,
                generation,
                ..
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
        // agreed on. The incarnation comes from the same snapshot.
        let router_key = crate::shards::routing::to_router_key(key);
        let routes = self.router.routes();
        let ours = match self.router.resolve_with(&routes, &router_key) {
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

        // Checked before the generation: two incarnations' generations are
        // unrelated numbers. A requester that named none predates the check,
        // or routes a stream from before incarnations.
        if let Some(sent) = claimed.incarnation {
            let held = routes
                .table()
                .get(&router_key)
                .map_or(0, |route| route.incarnation);
            if sent != held {
                metrics::record_served(metrics::OUTCOME_REFUSED);
                let what = match key.kind {
                    ShardKind::Stream => "stream",
                    ShardKind::Cache => "cache",
                };
                return Some(Denial::Refused {
                    code: ErrorCode::StaleRoute,
                    detail: format!(
                        "this broker routes incarnation {held} of the {what}, the requester {sent}"
                    ),
                });
            }
        }

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
        claimed: &ShardRef,
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
        self.ingress
            .settle(
                key,
                claimed.generation.saturating_add(1),
                claimed.incarnation,
            )
            .await;
        if let Some(denial) = self.check_ownership(correlation_id, key, claimed) {
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
                            .fenced(correlation_id, key, &op.shard, refused)
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
                        .counter_quorum(key, Some(offset + 1), op.shard.generation, "counter add")
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
                        .counter_quorum(key, None, op.shard.generation, "read")
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
                    outcome: None,
                })
            }
            Err((code, detail)) => InternalMessage::ForwardCacheError(ForwardCacheError {
                correlation_id,
                code,
                detail,
            }),
        }
    }

    /// [`Engine::await_counter_quorum`] for a forwarded
    /// counter op, as the requester's own path waits for a local one.
    async fn counter_quorum(
        &self,
        key: &ShardKey,
        end: Option<u64>,
        generation: u64,
        what: &'static str,
    ) -> Result<(), String> {
        self.engine
            .await_counter_quorum(
                &self.broker,
                key,
                self.quorum_timeout,
                end,
                generation,
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
            InternalMessage::LeaseForwardCacheOp(op) => self.apply_lease_cache_op(op).await,
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

/// Why a forwarded request's credential was not accepted. The detail never
/// holds the token.
struct Denied {
    detail: String,
    /// The token could not be checked, rather than being refused.
    unchecked: bool,
}

impl Denied {
    fn refused(detail: String) -> Self {
        Self {
            detail,
            unchecked: false,
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

fn outcome(written: felix_storage::ConditionalWrite) -> ForwardCacheOutcome {
    ForwardCacheOutcome {
        applied: written.applied,
        version: written.version,
    }
}

#[cfg(test)]
mod tests;
