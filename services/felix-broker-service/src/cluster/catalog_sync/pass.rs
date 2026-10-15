//! One sync pass: seed from snapshots on a cold start, then follow the change
//! feeds.

use std::sync::Arc;

use anyhow::Result;
use felix_broker::{Broker, BrokerError, CacheMetadata};

use super::SyncState;
use super::apply::{
    apply_cache_upsert, apply_namespace_create, apply_stream_upsert, read_consistency,
};
use super::fetch::*;
use super::wire::*;
use crate::cluster::feed_cursor::{BehindCursor, Verdict};

/// Pages of one change feed applied per pass, so a broker far behind catches
/// up in one pass without a single feed starving the others forever.
const MAX_PAGES_PER_FEED: usize = 64;

/// Performs a single sync iteration: fetches and applies any new control-plane state.
///
/// Algorithm:
/// - On cold start (`next_seq == 0`), fetch full snapshots in dependency order:
///   1. Tenants
///   2. Namespaces
///   3. Caches
///   4. Streams
///      This ensures that hierarchical dependencies (tenants -> namespaces -> caches/streams)
///      are satisfied before applying more granular resources.
///
/// - After seeding, fetch incremental change feeds in the same order.
///   - Each feed is followed page by page until a page comes back empty, up to
///     `MAX_PAGES_PER_FEED` pages a pass.
///   - Each fetch is best-effort: errors are logged and the corresponding cursor is not advanced.
///   - On success, the cursor is advanced to the new `next_seq`.
///   - Deletion events use `remove_*` and ignore missing entries.
///
/// - The ordering ensures that removal of higher-level resources (e.g., tenant deletion)
///   occurs before dependent resources, preventing orphaned objects.
///
/// `state` moves past each change as it is applied. A change that fails stops
/// the pass with the cursor on it, so the next pass starts there instead of
/// applying the changes before it again. Replaying a delete that a create
/// already followed would remove the new stream.
pub(super) async fn sync_into(
    broker: &Arc<Broker>,
    client: &reqwest::Client,
    base_url: &str,
    bearer: Option<&str>,
    state: &mut SyncState,
) -> Result<()> {
    let log_as_debug = cfg!(test) || std::env::var_os("RUST_TEST_THREADS").is_some();
    // === Cold start snapshot seeding ===
    // 1. Seed tenants first.
    if !state.seeded.tenants {
        match fetch_tenant_snapshot(client, base_url, bearer).await {
            Ok(snapshot) => {
                for tenant in snapshot.items {
                    broker.register_tenant(tenant.tenant_id).await?;
                }
                state.next_tenant_seq = snapshot.next_seq;
                state.seeded.tenants = true;
            }
            Err(err) => {
                if log_as_debug {
                    tracing::debug!(error = %err, "control plane tenant snapshot failed");
                } else {
                    tracing::warn!(error = %err, "control plane tenant snapshot failed");
                }
            }
        }
    }

    // 2. Seed namespaces after tenants.
    if !state.seeded.namespaces {
        match fetch_namespace_snapshot(client, base_url, bearer).await {
            Ok(snapshot) => {
                for namespace in snapshot.items {
                    apply_namespace_create(broker, namespace.tenant_id, namespace.namespace)
                        .await?;
                }
                state.next_namespace_seq = snapshot.next_seq;
                state.seeded.namespaces = true;
            }
            Err(err) => {
                if log_as_debug {
                    tracing::debug!(error = %err, "control plane namespace snapshot failed");
                } else {
                    tracing::warn!(error = %err, "control plane namespace snapshot failed");
                }
            }
        }
    }

    // 3. Seed caches after namespaces.
    if !state.seeded.caches {
        match fetch_cache_snapshot(client, base_url, bearer).await {
            Ok(snapshot) => {
                for cache in snapshot.items {
                    let metadata = CacheMetadata {
                        consistency: read_consistency(cache.consistency.as_deref())?,
                    };
                    apply_cache_upsert(
                        broker,
                        cache.tenant_id,
                        cache.namespace,
                        cache.cache,
                        metadata,
                    )
                    .await?;
                }
                state.next_cache_seq = snapshot.next_seq;
                state.seeded.caches = true;
            }
            Err(err) => {
                if log_as_debug {
                    tracing::debug!(error = %err, "control plane cache snapshot failed");
                } else {
                    tracing::warn!(error = %err, "control plane cache snapshot failed");
                }
            }
        }
    }

    // 4. Seed streams after caches.
    if !state.seeded.streams {
        match fetch_snapshot(client, base_url, bearer).await {
            Ok(snapshot) => {
                // Deletes first: the data of every incarnation a tombstone
                // covers goes, whether or not this broker heard of it.
                for tombstone in snapshot.tombstones {
                    broker
                        .delete_stream_through(
                            &tombstone.tenant_id,
                            &tombstone.namespace,
                            &tombstone.stream,
                            tombstone.incarnation,
                        )
                        .await?;
                }
                for stream in snapshot.items {
                    let metadata = stream.metadata()?;
                    apply_stream_upsert(
                        broker,
                        stream.tenant_id,
                        stream.namespace,
                        stream.stream,
                        metadata,
                    )
                    .await?;
                }
                state.next_stream_seq = snapshot.next_seq;
                state.seeded.streams = true;
            }
            Err(err) => {
                if log_as_debug {
                    tracing::debug!(error = %err, "control plane snapshot failed");
                } else {
                    tracing::warn!(error = %err, "control plane snapshot failed");
                }
            }
        }
    }

    // === Incremental change feed application ===
    // 1. Apply tenant changes first (ensures proper revocation/creation).
    for _ in 0..MAX_PAGES_PER_FEED {
        match fetch_tenant_changes(client, base_url, bearer, state.next_tenant_seq).await {
            Ok(changes) => {
                if !accept(
                    &mut state.behind.tenants,
                    &mut state.seeded.tenants,
                    state.next_tenant_seq,
                    changes.next_seq,
                    "tenant",
                ) {
                    break;
                }
                // Follow until a page comes back empty: a full page stops short of the tail.
                let more = !changes.items.is_empty() && changes.next_seq > state.next_tenant_seq;
                for change in changes.items {
                    let applied = change.seq;
                    match change.op {
                        TenantChangeOp::Created => {
                            if let Some(tenant) = change.tenant {
                                broker.register_tenant(tenant.tenant_id).await?;
                            }
                        }
                        TenantChangeOp::Deleted => {
                            let _ = broker.remove_tenant(&change.tenant_id).await?;
                        }
                    }
                    if let Some(seq) = applied {
                        state.next_tenant_seq = state.next_tenant_seq.max(seq + 1);
                    }
                }
                state.next_tenant_seq = state.next_tenant_seq.max(changes.next_seq);
                if !more {
                    break;
                }
            }
            Err(err) => {
                if log_as_debug {
                    tracing::debug!(error = %err, "control plane tenant change poll failed");
                } else {
                    tracing::warn!(error = %err, "control plane tenant change poll failed");
                }
                break;
            }
        }
    }

    // 2. Apply namespace changes after tenants.
    for _ in 0..MAX_PAGES_PER_FEED {
        match fetch_namespace_changes(client, base_url, bearer, state.next_namespace_seq).await {
            Ok(changes) => {
                if !accept(
                    &mut state.behind.namespaces,
                    &mut state.seeded.namespaces,
                    state.next_namespace_seq,
                    changes.next_seq,
                    "namespace",
                ) {
                    break;
                }
                // Follow until a page comes back empty: a full page stops short of the tail.
                let more = !changes.items.is_empty() && changes.next_seq > state.next_namespace_seq;
                for change in changes.items {
                    let applied = change.seq;
                    match change.op {
                        NamespaceChangeOp::Created => {
                            if let Some(namespace) = change.namespace {
                                apply_namespace_create(
                                    broker,
                                    namespace.tenant_id,
                                    namespace.namespace,
                                )
                                .await?;
                            }
                        }
                        NamespaceChangeOp::Deleted => {
                            match broker
                                .remove_namespace(&change.key.tenant_id, &change.key.namespace)
                                .await
                            {
                                Ok(_) => {}
                                Err(BrokerError::TenantNotFound(_)) => {}
                                Err(err) => return Err(err.into()),
                            }
                        }
                    }
                    if let Some(seq) = applied {
                        state.next_namespace_seq = state.next_namespace_seq.max(seq + 1);
                    }
                }
                state.next_namespace_seq = state.next_namespace_seq.max(changes.next_seq);
                if !more {
                    break;
                }
            }
            Err(err) => {
                if log_as_debug {
                    tracing::debug!(error = %err, "control plane namespace change poll failed");
                } else {
                    tracing::warn!(error = %err, "control plane namespace change poll failed");
                }
                break;
            }
        }
    }

    // 3. Apply cache changes after namespaces.
    //    (Caches depend on tenant/namespace existence.)
    for _ in 0..MAX_PAGES_PER_FEED {
        match fetch_cache_changes(client, base_url, bearer, state.next_cache_seq).await {
            Ok(changes) => {
                if !accept(
                    &mut state.behind.caches,
                    &mut state.seeded.caches,
                    state.next_cache_seq,
                    changes.next_seq,
                    "cache",
                ) {
                    break;
                }
                // Follow until a page comes back empty: a full page stops short of the tail.
                let more = !changes.items.is_empty() && changes.next_seq > state.next_cache_seq;
                for change in changes.items {
                    let applied = change.seq;
                    match change.op {
                        CacheChangeOp::Created | CacheChangeOp::Updated => {
                            if let Some(cache) = change.cache {
                                let metadata = CacheMetadata {
                                    consistency: read_consistency(cache.consistency.as_deref())?,
                                };
                                apply_cache_upsert(
                                    broker,
                                    cache.tenant_id,
                                    cache.namespace,
                                    cache.cache,
                                    metadata,
                                )
                                .await?;
                            }
                        }
                        CacheChangeOp::Deleted => {
                            match broker
                                .remove_cache(
                                    &change.key.tenant_id,
                                    &change.key.namespace,
                                    &change.key.cache,
                                )
                                .await
                            {
                                Ok(_) => {}
                                Err(BrokerError::TenantNotFound(_)) => {}
                                Err(BrokerError::NamespaceNotFound { .. }) => {}
                                Err(err) => return Err(err.into()),
                            }
                        }
                    }
                    if let Some(seq) = applied {
                        state.next_cache_seq = state.next_cache_seq.max(seq + 1);
                    }
                }
                state.next_cache_seq = state.next_cache_seq.max(changes.next_seq);
                if !more {
                    break;
                }
            }
            Err(err) => {
                if log_as_debug {
                    tracing::debug!(error = %err, "control plane cache change poll failed");
                } else {
                    tracing::warn!(error = %err, "control plane cache change poll failed");
                }
                break;
            }
        }
    }

    // 4. Apply stream changes last so existence checks have up-to-date scopes.
    for _ in 0..MAX_PAGES_PER_FEED {
        match fetch_changes(client, base_url, bearer, state.next_stream_seq).await {
            Ok(changes) => {
                if !accept(
                    &mut state.behind.streams,
                    &mut state.seeded.streams,
                    state.next_stream_seq,
                    changes.next_seq,
                    "stream",
                ) {
                    break;
                }
                // Follow until a page comes back empty: a full page stops short of the tail.
                let more = !changes.items.is_empty() && changes.next_seq > state.next_stream_seq;
                for change in changes.items {
                    let applied = change.seq;
                    match change.op {
                        StreamChangeOp::Created | StreamChangeOp::Updated => {
                            if let Some(stream) = change.stream {
                                let metadata = stream.metadata()?;
                                apply_stream_upsert(
                                    broker,
                                    stream.tenant_id,
                                    stream.namespace,
                                    stream.stream,
                                    metadata,
                                )
                                .await?;
                            }
                        }
                        StreamChangeOp::Deleted => {
                            // From the finalize of incarnations on, the change
                            // says through which incarnation the name is
                            // deleted. One that says nothing is of a stream from
                            // before them, whose data stays; its payload, when it
                            // has one, names incarnation 0 either way.
                            let incarnation = change.deleted_through.or(change
                                .stream
                                .as_ref()
                                .map(|stream| stream.incarnation)
                                .filter(|incarnation| *incarnation != 0));
                            match broker
                                .remove_stream_from_catalog(
                                    &change.key.tenant_id,
                                    &change.key.namespace,
                                    &change.key.stream,
                                    incarnation,
                                )
                                .await
                            {
                                Ok(_) => {}
                                Err(BrokerError::TenantNotFound(_)) => {}
                                Err(BrokerError::NamespaceNotFound { .. }) => {}
                                Err(err) => return Err(err.into()),
                            }
                        }
                    }
                    if let Some(seq) = applied {
                        state.next_stream_seq = state.next_stream_seq.max(seq + 1);
                    }
                }
                state.next_stream_seq = state.next_stream_seq.max(changes.next_seq);
                if !more {
                    break;
                }
            }
            Err(err) => {
                if log_as_debug {
                    tracing::debug!(error = %err, "control plane change poll failed");
                } else {
                    tracing::warn!(error = %err, "control plane change poll failed");
                }
                break;
            }
        }
    }
    Ok(())
}

/// [`sync_into`] on a state of its own, for tests.
#[cfg(test)]
pub(super) async fn sync_once(
    broker: &Arc<Broker>,
    client: &reqwest::Client,
    base_url: &str,
    bearer: Option<&str>,
    mut state: SyncState,
) -> Result<SyncState> {
    sync_into(broker, client, base_url, bearer, &mut state).await?;
    Ok(state)
}

/// Whether to apply a page whose `next_seq` is `next_seq`, for a feed at
/// `cursor`. A page from behind the cursor came from a lagging control-plane
/// member: applying it would replay changes already applied, so it is skipped.
/// Once the feed has answered only from behind for long enough to be a reset,
/// its history started over and the changes before `next_seq` are not ones
/// this broker has seen, so the feed is marked unseeded and the next pass
/// takes a new snapshot.
fn accept(
    behind: &mut BehindCursor,
    seeded: &mut bool,
    cursor: u64,
    next_seq: u64,
    feed: &str,
) -> bool {
    match behind.judge(next_seq < cursor, tokio::time::Instant::now()) {
        Verdict::Fresh => true,
        Verdict::Stale => {
            tracing::debug!(
                feed,
                cursor,
                next_seq,
                "control plane answered from behind the cursor; ignored"
            );
            false
        }
        Verdict::Reset => {
            tracing::warn!(
                feed,
                cursor,
                next_seq,
                "control plane change feed stayed behind the cursor; taking it as a reset and resnapshotting",
            );
            *seeded = false;
            false
        }
    }
}
