//! Applying catalog entries to the broker's registries.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use felix_broker::{Broker, BrokerError, CacheMetadata, ConsistencyLevel, StreamMetadata};

pub(super) async fn apply_namespace_create(
    broker: &Arc<Broker>,
    tenant_id: String,
    namespace: String,
) -> Result<()> {
    match broker
        .register_namespace(tenant_id.clone(), namespace.clone())
        .await
    {
        Ok(_) => Ok(()),
        Err(BrokerError::TenantNotFound(_)) => {
            broker.register_tenant(tenant_id.clone()).await?;
            broker.register_namespace(tenant_id, namespace).await?;
            Ok(())
        }
        Err(err) => Err(err.into()),
    }
}

pub(super) async fn apply_cache_upsert(
    broker: &Arc<Broker>,
    tenant_id: String,
    namespace: String,
    cache: String,
    metadata: CacheMetadata,
) -> Result<()> {
    match broker
        .register_cache(
            tenant_id.clone(),
            namespace.clone(),
            cache.clone(),
            metadata.clone(),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(BrokerError::TenantNotFound(_)) => {
            broker.register_tenant(tenant_id.clone()).await?;
            broker
                .register_namespace(tenant_id.clone(), namespace.clone())
                .await?;
            broker
                .register_cache(tenant_id, namespace, cache, metadata)
                .await?;
            Ok(())
        }
        Err(BrokerError::NamespaceNotFound { .. }) => {
            broker
                .register_namespace(tenant_id.clone(), namespace.clone())
                .await?;
            broker
                .register_cache(tenant_id, namespace, cache, metadata)
                .await?;
            Ok(())
        }
        Err(err) => Err(err.into()),
    }
}

pub(super) async fn apply_stream_upsert(
    broker: &Arc<Broker>,
    tenant_id: String,
    namespace: String,
    stream: String,
    metadata: StreamMetadata,
) -> Result<()> {
    let outcome = match broker
        .register_stream(
            tenant_id.clone(),
            namespace.clone(),
            stream.clone(),
            metadata.clone(),
        )
        .await
    {
        // The control plane can send a stream before its parents; create them
        // and retry rather than dropping the update.
        Err(BrokerError::TenantNotFound(_)) => {
            broker.register_tenant(tenant_id.clone()).await?;
            broker
                .register_namespace(tenant_id.clone(), namespace.clone())
                .await?;
            broker
                .register_stream(
                    tenant_id.clone(),
                    namespace.clone(),
                    stream.clone(),
                    metadata,
                )
                .await
        }
        Err(BrokerError::NamespaceNotFound { .. }) => {
            broker
                .register_namespace(tenant_id.clone(), namespace.clone())
                .await?;
            broker
                .register_stream(
                    tenant_id.clone(),
                    namespace.clone(),
                    stream.clone(),
                    metadata,
                )
                .await
        }
        other => other,
    };

    match outcome {
        Ok(_) => Ok(()),
        // A broker with no durable storage configured cannot host this stream,
        // and never will until it is restarted with different configuration.
        // Skipping it loudly beats failing the sync, which would stop the
        // watcher applying *every other* stream in the catalog and turn one
        // misconfigured stream into a broker-wide outage.
        //
        // The stream stays unregistered, so publishes to it fail with
        // `StreamNotFound`: no false durability guarantee is ever offered.
        Err(BrokerError::DurableStorageNotConfigured { .. }) => {
            tracing::error!(
                tenant_id = %tenant_id,
                namespace = %namespace,
                stream = %stream,
                "skipping stream: this broker has no durable storage configured"
            );
            metrics::counter!("felix_broker_durable_stream_skipped_total").increment(1);
            Ok(())
        }
        // A storage *failure* is the opposite case and must not be skipped.
        // Corruption or an I/O error means the log is in an unknown state;
        // swallowing it here would advance the control-plane cursor, leave the
        // stream permanently absent, and keep the broker reporting ready while
        // a durable stream silently does not exist. Fail the sync so the error
        // is visible and the cursor does not move past it.
        Err(err @ (BrokerError::Storage(_) | BrokerError::StorageFull(_))) => {
            tracing::error!(
                tenant_id = %tenant_id,
                namespace = %namespace,
                stream = %stream,
                error = %err,
                "durable storage failed while registering a stream"
            );
            metrics::counter!("felix_broker_durable_stream_failed_total").increment(1);
            Err(err.into())
        }
        Err(err) => Err(err.into()),
    }
}

/// Read a consistency level the control plane sent.
///
/// **An unrecognised level is refused, never defaulted.** Falling back to
/// `Leader` would take a stream the operator asked to be quorum-replicated and
/// serve it at the weaker guarantee, silently: the acknowledgement would keep
/// its meaning on paper and lose it in fact. A broker that does not understand
/// what it was asked for has to say so.
pub(super) fn read_consistency(value: Option<&str>) -> Result<ConsistencyLevel> {
    match value {
        None | Some("Leader") => Ok(ConsistencyLevel::Leader),
        Some("Quorum") => Ok(ConsistencyLevel::Quorum),
        Some(other) => Err(anyhow!(
            "unknown consistency level {other:?}; this broker understands Leader and Quorum"
        )),
    }
}
