//! QUIC cache-watch handling: a keyed subscription over a cache's log.
//!
//! A `CacheWatch` arrives on the bi-directional control stream; changes go back
//! on a fresh uni stream, bound by the same `EventStreamHello` a stream
//! subscription uses. The join is the point of this module, and it follows the
//! register-before-read discipline the stream resume path proved out:
//!
//! 1. Register the watcher with the hub — from here every applied change is
//!    either queued or reported as the lag offset.
//! 2. Read the shard log's tail. Registration happened first, so every change
//!    below the tail is on disk and every change at or past it is queued.
//! 3. Serve the catch-up — `[from_offset, tail)` from the log for a resume,
//!    or each matching key's current value for a retained watch and for a
//!    resume compaction has collapsed — then live changes, dropping queued
//!    duplicates below the tail by offset.
//!
//! Reading history first and registering after is the version that looks
//! natural and loses the write that lands in between.
//!
//! Loss is loud, never silent: a watcher whose queue overflowed is ended with
//! `CacheWatchLagged` naming the first missed offset, because filtering makes
//! offsets sparse and a drop would otherwise be indistinguishable from other
//! keys' traffic. Re-watching from that offset is gapless.
//!
//! On a `Quorum` cache a watch hands out nothing past the committed mark: the
//! catch-up waits for the mark to reach the tail, and each live change waits
//! for the mark to pass it ([`CommitGate`]). A change past the mark can be
//! lost at failover and its offset reused.

use std::sync::Arc;

use anyhow::Result;
use felix_broker::{Broker, CacheWatchFilter, CacheWatchSubscription};
use felix_wire::Message;
use tokio::sync::mpsc;

use super::publish::{Outgoing, PublishContext, send_outgoing_critical};
use crate::serving::quic::SUBSCRIPTION_ID;
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::codec::write_message;

/// Changes queued per watcher before the watch is ended as lagged. A watch is
/// a control-plane-sized feed — one key or prefix, not a stream's firehose —
/// so this bounds memory without a per-deployment knob.
const WATCH_QUEUE_CAPACITY: usize = 1024;

/// Everything `handle_cache_watch_message` needs to answer on the control
/// stream, bundled so the failure paths cannot drift apart.
pub(crate) struct WatchResponder<'a> {
    pub(crate) out_ack_tx: &'a mpsc::Sender<Outgoing>,
    pub(crate) out_ack_depth: &'a Arc<std::sync::atomic::AtomicUsize>,
    pub(crate) ack_throttle_tx: &'a tokio::sync::watch::Sender<bool>,
    pub(crate) ack_timeout_state: &'a Arc<parking_lot::Mutex<super::publish::AckTimeoutState>>,
    pub(crate) cancel_tx: &'a tokio::sync::watch::Sender<bool>,
}

impl WatchResponder<'_> {
    pub(crate) async fn send(&self, message: Message) -> Result<()> {
        super::publish::handle_ack_enqueue_result(
            send_outgoing_critical(
                self.out_ack_tx,
                self.out_ack_depth,
                "felix_broker_out_ack_depth",
                self.ack_throttle_tx,
                Outgoing::Message(message),
            )
            .await,
            self.ack_timeout_state,
            self.ack_throttle_tx,
            self.cancel_tx,
        )
        .await
    }
}

/// The request's watch parameters, decoded but not yet validated.
pub(crate) struct WatchRequest {
    pub(crate) tenant_id: String,
    pub(crate) namespace: String,
    pub(crate) cache: String,
    pub(crate) key: Option<String>,
    pub(crate) prefix: Option<String>,
    pub(crate) shard: Option<u32>,
    pub(crate) from_offset: Option<u64>,
    pub(crate) retained: bool,
    pub(crate) subscription_id: Option<u64>,
}

/// Handle a `CacheWatch` from the control-stream loop.
///
/// Always returns `Ok(true)` ("handled; keep the control stream alive") unless
/// the response path itself fails — the same convention subscribe follows.
pub(crate) async fn handle_cache_watch_message(
    broker: Arc<Broker>,
    connection: felix_transport::QuicConnection,
    publish_ctx: &PublishContext,
    responder: WatchResponder<'_>,
    request: WatchRequest,
    peer_features: u32,
    owner: Option<&crate::serving::auth::AuthContext>,
) -> Result<bool> {
    // Exactly one of key/prefix. Refused rather than guessed at: "both" has
    // two defensible readings and a client relying on either would silently
    // get the other.
    let filter = match (&request.key, &request.prefix) {
        (Some(key), None) => CacheWatchFilter::Key(key.clone()),
        (None, Some(prefix)) => CacheWatchFilter::Prefix(prefix.clone()),
        _ => {
            responder
                .send(
                    ClientError::invalid("cache_watch takes exactly one of key or prefix")
                        .into_message(),
                )
                .await?;
            return Ok(true);
        }
    };

    if !broker
        .cache_exists(&request.tenant_id, &request.namespace, &request.cache)
        .await
    {
        responder
            .send(
                ClientError::not_found(format!(
                    "cache scope not found: {}/{}/{}",
                    request.tenant_id, request.namespace, request.cache
                ))
                .into_message(),
            )
            .await?;
        return Ok(true);
    }

    // Which shard this watch reads. A key names its shard by hashing — the
    // same resolution a get or put uses — while a prefix watch reads the shard
    // it was addressed to, because keys sharing a prefix hash apart.
    let shards = publish_ctx
        .ingress
        .as_deref()
        .map(|ingress| {
            ingress.shards_for(
                crate::shards::ShardKind::Cache,
                &request.tenant_id,
                &request.namespace,
                &request.cache,
            )
        })
        .unwrap_or(1);
    let shard = match (&filter, request.shard) {
        (CacheWatchFilter::Key(key), _) => {
            crate::shards::routing::shard_for(shards, Some(key.as_bytes()))
        }
        (CacheWatchFilter::Prefix(_), Some(shard)) => shard,
        (CacheWatchFilter::Prefix(_), None) if shards <= 1 => 0,
        // Defaulting to shard 0 here would watch only the keys that happen to
        // hash there and look like a working prefix watch: silently partial.
        (CacheWatchFilter::Prefix(_), None) => {
            responder
                .send(
                    ClientError::invalid(format!(
                        "cache {} has {shards} shards and a prefix watch reads one; name the \
                         shard, and open one watch per shard to cover the prefix",
                        request.cache
                    ))
                    .into_message(),
                )
                .await?;
            return Ok(true);
        }
    };
    // A shard the cache does not have is refused. Accepting it would register a
    // watch on a log nothing writes to — quiet forever, and indistinguishable
    // from a quiet prefix.
    if shard >= shards.max(1) {
        responder
            .send(
                ClientError::invalid(format!(
                    "cache {} has {} shard(s); shard {} does not exist",
                    request.cache,
                    shards.max(1),
                    shard
                ))
                .into_message(),
            )
            .await?;
        return Ok(true);
    }

    // Watches are served by the shard's owner, never proxied: a watch served
    // off the owner would go quiet on writes it cannot see, which is
    // indistinguishable from a quiet key.
    if let Some(answer) = super::redirect::redirect_for(
        publish_ctx.ingress.as_deref(),
        publish_ctx.client_endpoints.as_deref(),
        &request.tenant_id,
        &request.namespace,
        &request.cache,
        shard,
        crate::shards::ShardKind::Cache,
        peer_features,
    ) {
        responder.send(answer).await?;
        return Ok(true);
    }

    let Some(hub) = broker.cache_watches() else {
        // Reachable only by a client that ignored negotiation: the feature bit
        // is advertised exactly when the hub exists.
        responder
            .send(
                ClientError::invalid(
                    "this broker's cache is not log-backed, so it cannot serve watches",
                )
                .into_message(),
            )
            .await?;
        return Ok(true);
    };

    if let Err(cap) = publish_ctx.identity.try_reserve() {
        responder
            .send(ClientError::limit_exceeded(cap.message()).into_message())
            .await?;
        return Ok(true);
    }
    // Reserved; every exit below must release exactly once — failure paths do
    // it inline, the delivery task does it on the success path.
    let subscriptions = Arc::clone(&publish_ctx.identity);

    // Register *before* reading the tail: this pins the live edge. Every
    // change applied from here on is queued (or reported as lag), so the
    // history below the tail is a closed range nothing can escape.
    let register = || {
        hub.register(
            &request.tenant_id,
            &request.namespace,
            &request.cache,
            shard,
            filter.clone(),
            WATCH_QUEUE_CAPACITY,
        )
    };
    let mut watch = register();

    // Checked again now that it is registered: an ending that landed since
    // the first check would otherwise have missed it.
    let shard_key = crate::shards::ShardKey {
        tenant_id: request.tenant_id.clone(),
        namespace: request.namespace.clone(),
        stream: request.cache.clone(),
        shard,
        kind: crate::shards::ShardKind::Cache,
    };
    if let Some(refusal) =
        super::redirect::stopped_serving(publish_ctx.ingress.as_deref(), &shard_key)
    {
        drop(watch);
        subscriptions.release();
        responder.send(refusal).await?;
        return Ok(true);
    }

    // A shard a Raft group runs here joins at the group's applied edge. Only
    // committed entries are applied, so no gate applies. A resume below the
    // edge is replayed from the member's recent changes or its log above its
    // snapshot, and answered with a resnapshot when the snapshot covers it.
    let refuse_unready = |why: &str| {
        ClientError::unavailable(
            &crate::shards::routing::Reason::NotReady,
            format!("this member could not confirm its state is current: {why}; retry"),
        )
        .into_message()
    };
    let mut raft = match raft_join(&publish_ctx.engine, &shard_key, &filter, request.retained).await
    {
        Ok(raft) => raft,
        Err(why) => {
            drop(watch);
            subscriptions.release();
            responder.send(refuse_unready(why)).await?;
            return Ok(true);
        }
    };
    let mut replayed = None;
    if let (Some(join), false, Some(from)) = (raft.as_mut(), request.retained, request.from_offset)
        && from <= join.tail
    {
        let replay = raft_replay(&publish_ctx.engine, &shard_key, &filter, from).await;
        // A replay from the log that outlasted the live queue is not handed
        // out: the client would resume below the recorded changes and start
        // the same replay again. The watch registers afresh and is
        // resnapshotted, which costs no fold.
        let overflowed = watch.lagged().is_some() && watch.moved().is_none();
        if overflowed {
            watch = register();
            // An ending that landed after the old watch's moved check is
            // missed by the new one, as on the first registration.
            if let Some(refusal) =
                super::redirect::stopped_serving(publish_ctx.ingress.as_deref(), &shard_key)
            {
                drop(watch);
                subscriptions.release();
                responder.send(refusal).await?;
                return Ok(true);
            }
        }
        if !overflowed
            && let Some(replay) = replay
            && let Some(changes) = replay.changes
        {
            join.tail = replay.tail;
            replayed = Some(changes);
        } else {
            // Handed current values instead, which have to include every
            // put acknowledged before the watch began.
            match raft_join(&publish_ctx.engine, &shard_key, &filter, true).await {
                Ok(Some(again)) => *join = again,
                refused => {
                    drop(watch);
                    subscriptions.release();
                    let why = refused.err().unwrap_or("it no longer runs the shard");
                    responder.send(refuse_unready(why)).await?;
                    return Ok(true);
                }
            }
        }
    }
    // Told when the watch ends, so the member keeps recording for its resume.
    let raft_ended = raft
        .is_some()
        .then(|| (Arc::clone(&publish_ctx.engine), shard_key.clone()));
    let (tail, base) = match &raft {
        Some(join) => {
            let mut tail = join.tail;
            // A resume from past the edge is an offset another member gave
            // out, so this one is behind: wait for it to apply that far. The
            // watch registered at the old edge, so every change from there on
            // is queued and none is missed by joining at `from`.
            if let (false, Some(from)) = (request.retained, request.from_offset)
                && from > tail
            {
                if !raft_reaches(
                    &publish_ctx.engine,
                    &shard_key,
                    from,
                    publish_ctx.quorum_timeout,
                )
                .await
                {
                    subscriptions.release();
                    responder
                        .send(
                            ClientError::unavailable(
                                &crate::shards::routing::Reason::NotReady,
                                format!(
                                    "this member has applied only up to {tail}, short of the \
                                     resume offset {from}; retry"
                                ),
                            )
                            .into_message(),
                        )
                        .await?;
                    return Ok(true);
                }
                tail = from;
            }
            // Replayed changes reach back to the resume offset; without
            // them, anything below the edge is resnapshotted.
            let base = if replayed.is_some() { 0 } else { tail };
            (tail, base)
        }
        // No Raft member of this shard is open here yet: the shard's store
        // is never written outside its group, so a watch served from it would
        // join an empty log and hand out no state. Retryable.
        None => {
            subscriptions.release();
            let refusal = crate::serving::cache_routing::raft::not_open(&shard_key);
            responder
                .send(ClientError::from_anyhow(&refusal).into_message())
                .await?;
            return Ok(true);
        }
    };

    // What fills the gap between the requested position and the live edge.
    enum CatchUp {
        None,
        /// Replay the changes from the resume offset up to the tail.
        Replay,
        /// Each matching key's current value, then live — either because the
        /// watch asked for retained delivery, or because the offset it asked
        /// to resume from was collapsed by compaction and current state is the
        /// defined answer, never a silent gap.
        Snapshot,
    }
    let catch_up = match (request.retained, request.from_offset) {
        (true, Some(_)) => {
            // A resume already reconstructs the state a retained start would
            // shortcut, and delivering both would hand over every value twice.
            subscriptions.release();
            responder
                .send(
                    ClientError::invalid("cache_watch takes retained or from_offset, not both")
                        .into_message(),
                )
                .await?;
            return Ok(true);
        }
        (true, None) => CatchUp::Snapshot,
        (false, None) => CatchUp::None,
        (false, Some(from)) if from > tail => {
            subscriptions.release();
            responder
                .send(Message::SubscribeCursorError {
                    reason: felix_wire::CursorErrorReason::InFuture,
                    requested: from,
                    available: tail,
                    trim_headers: None,
                })
                .await?;
            return Ok(true);
        }
        (false, Some(from)) if from >= base => CatchUp::Replay,
        (false, Some(_)) => CatchUp::Snapshot,
    };
    // The compacted-resume case, distinct from asked-for retained delivery
    // because the confirmation reports them differently.
    let resnapshot = matches!(catch_up, CatchUp::Snapshot) && !request.retained;

    // Snapshots are read *before* the confirmation goes out, because a
    // retained watch's confirmation carries how many values follow — `Some(0)`
    // being the defined "joined an empty key" answer, which silence could
    // never be. Entries at or past the tail are left out: they were applied
    // after the watcher registered, so their changes arrive live, and writing
    // them here would double them.
    let snapshot = match (&catch_up, raft) {
        (CatchUp::Snapshot, Some(join)) => Some(join.entries),
        (_, _) => None,
    };
    let retained_count = request
        .retained
        .then(|| snapshot.as_ref().map_or(0, Vec::len) as u64);

    let subscription_id = request
        .subscription_id
        .unwrap_or_else(|| SUBSCRIPTION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed));

    let mut event_send = match connection.open_uni().await {
        Ok(send) => send,
        Err(err) => {
            subscriptions.release();
            responder
                .send(
                    ClientError::internal(err.to_string())
                        .with_retry(felix_wire::RetryClass::Retry)
                        .into_message(),
                )
                .await?;
            return Ok(true);
        }
    };
    // Hello first, so the client can bind subscription_id -> stream before any
    // change arrives.
    if let Err(err) = write_message(
        &mut event_send,
        Message::EventStreamHello { subscription_id },
    )
    .await
    {
        subscriptions.release();
        tracing::info!(error = %err, "cache watch event stream closed");
        return Ok(true);
    }

    // Acknowledge before writing any catch-up, exactly as subscribe does: the
    // client reads the event stream only after it sees the confirmation, so a
    // catch-up larger than the stream's flow-control window would deadlock if
    // it went first.
    responder
        .send(Message::CacheWatchStarted {
            subscription_id,
            resume_offset: tail,
            resnapshot,
            retained_count,
        })
        .await?;

    let catch_up_result = match (catch_up, snapshot) {
        // A Raft shard replays its recent changes, or nothing when the
        // resume is at or past its edge.
        (CatchUp::Replay, _) => {
            write_raft_changes(&mut event_send, replayed.unwrap_or_default()).await
        }
        (CatchUp::Snapshot, Some(entries)) => write_snapshot(&mut event_send, entries).await,
        _ => Ok(()),
    };
    if let Err(err) = catch_up_result {
        subscriptions.release();
        if let Some((engine, key)) = &raft_ended {
            raft_watch_ended(engine, key);
        }
        tracing::info!(error = %err, "cache watch catch-up failed");
        return Ok(true);
    }

    // Live delivery. Queued changes below the tail are duplicates of what the
    // catch-up already wrote — the registration race — and are dropped by
    // offset; everything else is written until the client goes away or the
    // watch laps its queue.
    let shard_moved = Some(subscription_id);
    let revoked = owner.map(|auth| {
        (
            auth.revocable.clone(),
            ClientError::revoked().into_message(),
        )
    });
    tokio::spawn(async move {
        run_watch_delivery(watch, event_send, tail, subscriptions, shard_moved, revoked).await;
        if let Some((engine, key)) = raft_ended {
            raft_watch_ended(&engine, &key);
        }
    });
    Ok(true)
}

/// Where a watch joins a cache shard a Raft group runs here.
struct RaftJoin {
    tail: u64,
    entries: Vec<felix_storage::CacheSnapshotEntry>,
}

/// The join point of `key`'s Raft group on this broker, read after the
/// watch registered. `None` when no Raft member runs the shard here. Values are read
/// only when `entries` asks for them, and then the join first passes the
/// group's read barrier; its error is retryable.
async fn raft_join(
    engine: &crate::shards::routing::StreamEngine,
    key: &crate::shards::ShardKey,
    filter: &CacheWatchFilter,
    entries: bool,
) -> Result<Option<RaftJoin>, &'static str> {
    #[cfg(feature = "raft")]
    {
        let keep = |name: &str| entries && filter.matches(name);
        let Some(raft) = engine.raft(key) else {
            return Ok(None);
        };
        // Handed-out state (a retained set, or a resume's resnapshot) has to
        // include every put acknowledged before the watch began, and this
        // member may be a follower that has not applied it yet. A live-only
        // watch hands out none.
        let point = if entries {
            raft.cache_retained_point(key, &keep).await?
        } else {
            raft.cache_watch_point(key, &keep)
        };
        Ok(point.map(|point| RaftJoin {
            tail: point.tail,
            entries: point.entries,
        }))
    }
    #[cfg(not(feature = "raft"))]
    {
        let _ = (engine, key, filter, entries);
        Ok(None)
    }
}

/// The changes a watch resuming from `from` missed on `key`'s Raft member
/// here, read after the watch registered. `None` when no group runs it.
async fn raft_replay(
    engine: &crate::shards::routing::StreamEngine,
    key: &crate::shards::ShardKey,
    filter: &CacheWatchFilter,
    from: u64,
) -> Option<RaftReplay> {
    #[cfg(feature = "raft")]
    {
        let replay = engine
            .raft(key)?
            .cache_replay(key, from, &|name| filter.matches(name))
            .await?;
        Some(RaftReplay {
            tail: replay.tail,
            changes: replay.changes,
        })
    }
    #[cfg(not(feature = "raft"))]
    {
        let _ = (engine, key, filter, from);
        None
    }
}

/// Tell `key`'s Raft member here that a watch of it ended.
fn raft_watch_ended(engine: &crate::shards::routing::StreamEngine, key: &crate::shards::ShardKey) {
    #[cfg(feature = "raft")]
    if let Some(raft) = engine.raft(key) {
        raft.cache_watch_ended(key);
    }
    #[cfg(not(feature = "raft"))]
    let _ = (engine, key);
}

#[cfg(feature = "raft")]
type RaftChange = felix_replication::raft::CacheReplayed;
#[cfg(not(feature = "raft"))]
type RaftChange = std::convert::Infallible;

/// See [`raft_replay`].
struct RaftReplay {
    tail: u64,
    changes: Option<Vec<RaftChange>>,
}

/// Deliver a Raft member's replayed changes on the event stream, deletes as
/// changes with no value, as a log replay delivers them.
async fn write_raft_changes(
    event_send: &mut quinn::SendStream,
    changes: Vec<RaftChange>,
) -> Result<()> {
    #[cfg(feature = "raft")]
    for change in changes {
        write_message(
            event_send,
            Message::CacheEvent {
                key: change.key,
                value: change.value,
                offset: change.offset,
                expires_at_millis: change.expires_at_millis,
            },
        )
        .await?;
    }
    #[cfg(not(feature = "raft"))]
    let _ = (event_send, changes);
    Ok(())
}

/// Wait, up to `timeout`, until `key`'s Raft member here has applied every
/// entry below `from`. False when it has not, or no longer runs the shard.
async fn raft_reaches(
    engine: &crate::shards::routing::StreamEngine,
    key: &crate::shards::ShardKey,
    from: u64,
    timeout: std::time::Duration,
) -> bool {
    #[cfg(feature = "raft")]
    {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let tail = engine
                .raft(key)
                .and_then(|raft| raft.cache_watch_point(key, &|_| false))
                .map(|point| point.tail);
            match tail {
                Some(tail) if tail >= from => return true,
                None => return false,
                Some(_) if tokio::time::Instant::now() >= deadline => return false,
                Some(_) => tokio::time::sleep(std::time::Duration::from_millis(5)).await,
            }
        }
    }
    #[cfg(not(feature = "raft"))]
    {
        let _ = (engine, key, from, timeout);
        false
    }
}

/// Deliver a snapshot's current values on the event stream.
async fn write_snapshot(
    event_send: &mut quinn::SendStream,
    entries: Vec<felix_storage::CacheSnapshotEntry>,
) -> Result<()> {
    for entry in entries {
        write_message(
            event_send,
            Message::CacheEvent {
                key: entry.key,
                value: Some(entry.value),
                offset: entry.offset,
                expires_at_millis: entry.expires_at_millis,
            },
        )
        .await?;
    }
    Ok(())
}

/// Drain the watcher queue onto the event stream until the client goes away or
/// the watch falls behind.
async fn run_watch_delivery(
    mut watch: CacheWatchSubscription,
    mut event_send: quinn::SendStream,
    resume_offset: u64,
    subscriptions: Arc<super::publish::IdentityLimits>,
    // The watch's id, when the client offered `FEATURE_SHARD_MOVED`.
    shard_moved: Option<u64>,
    // The credential the watch was opened with, and the last frame to send
    // when it is revoked.
    revoked: Option<(crate::serving::revocation::Revocable, Message)>,
) {
    let (revocable, mut revoked_message) = revoked.unzip();
    let mut revocation = Box::pin(async move {
        match revocable {
            Some(revocable) => revocable.revoked().await,
            None => std::future::pending().await,
        }
    });
    loop {
        let next = tokio::select! {
            next = watch.recv() => next,
            () = &mut revocation => {
                tracing::info!("ending a cache watch: its credential was revoked");
                if let Some(message) = revoked_message.take() {
                    let _ = write_message(&mut event_send, message).await;
                }
                break;
            }
        };
        match next {
            Some(event) => {
                if event.offset < resume_offset {
                    // Applied before the tail was read, so the catch-up already
                    // covered it; the offset is what makes the duplicate cheap
                    // to detect.
                    continue;
                }
                let message = Message::CacheEvent {
                    key: event.key,
                    value: event.value,
                    offset: event.offset,
                    expires_at_millis: event.expires_at_millis,
                };
                if let Err(err) = write_message(&mut event_send, message).await {
                    tracing::debug!(error = %err, "cache watch client went away");
                    break;
                }
            }
            None => {
                if let (Some(subscription_id), Some(moved)) = (shard_moved, watch.moved()) {
                    let _ = write_message(
                        &mut event_send,
                        Message::ShardMoved {
                            subscription_id,
                            resume_from: moved.resume_from,
                            node_id: moved.to.node_id.clone(),
                            addr: moved.to.addr.clone(),
                            generation: moved.to.generation,
                        },
                    )
                    .await;
                } else if let Some(resume_from) = watch.lagged() {
                    // Ended loudly: everything queued was delivered above, and
                    // this names the first change that was not.
                    let _ =
                        write_message(&mut event_send, Message::CacheWatchLagged { resume_from })
                            .await;
                }
                break;
            }
        }
    }
    let _ = event_send.finish();
    subscriptions.release();
}

#[cfg(test)]
mod tests;
