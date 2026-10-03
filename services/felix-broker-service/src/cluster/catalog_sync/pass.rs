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
///   - Each fetch is best-effort: errors are logged and the corresponding cursor is not advanced.
///   - On success, the cursor is advanced to the new `next_seq`.
///   - Deletion events use `remove_*` and ignore missing entries.
///
/// - The ordering ensures that removal of higher-level resources (e.g., tenant deletion)
///   occurs before dependent resources, preventing orphaned objects.
pub(super) async fn sync_once(
    broker: &Arc<Broker>,
    client: &reqwest::Client,
    base_url: &str,
    bearer: Option<&str>,
    mut state: SyncState,
) -> Result<SyncState> {
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
    match fetch_tenant_changes(client, base_url, bearer, state.next_tenant_seq).await {
        Ok(changes) => {
            for change in changes.items {
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
            }
            state.next_tenant_seq = changes.next_seq;
        }
        Err(err) => {
            if log_as_debug {
                tracing::debug!(error = %err, "control plane tenant change poll failed");
            } else {
                tracing::warn!(error = %err, "control plane tenant change poll failed");
            }
        }
    }

    // 2. Apply namespace changes after tenants.
    match fetch_namespace_changes(client, base_url, bearer, state.next_namespace_seq).await {
        Ok(changes) => {
            for change in changes.items {
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
            }
            state.next_namespace_seq = changes.next_seq;
        }
        Err(err) => {
            if log_as_debug {
                tracing::debug!(error = %err, "control plane namespace change poll failed");
            } else {
                tracing::warn!(error = %err, "control plane namespace change poll failed");
            }
        }
    }

    // 3. Apply cache changes after namespaces.
    //    (Caches depend on tenant/namespace existence.)
    match fetch_cache_changes(client, base_url, bearer, state.next_cache_seq).await {
        Ok(changes) => {
            for change in changes.items {
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
            }
            state.next_cache_seq = changes.next_seq;
        }
        Err(err) => {
            if log_as_debug {
                tracing::debug!(error = %err, "control plane cache change poll failed");
            } else {
                tracing::warn!(error = %err, "control plane cache change poll failed");
            }
        }
    }

    // 4. Apply stream changes last so existence checks have up-to-date scopes.
    match fetch_changes(client, base_url, bearer, state.next_stream_seq).await {
        Ok(changes) => {
            for change in changes.items {
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
                        match broker
                            .remove_stream(
                                &change.key.tenant_id,
                                &change.key.namespace,
                                &change.key.stream,
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
            }
            state.next_stream_seq = changes.next_seq;
        }
        Err(err) => {
            if log_as_debug {
                tracing::debug!(error = %err, "control plane change poll failed");
            } else {
                tracing::warn!(error = %err, "control plane change poll failed");
            }
        }
    }
    Ok(state)
}
