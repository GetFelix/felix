//! Acting as another principal over the same connections.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};

use anyhow::Result;
use felix_transport::QuicConnection;
use tracing::debug;

use super::Client;
use super::connect::{
    WorkerSettings, note_connection, open_cache_worker, shard_streams, spawn_publish_worker,
    stream_widths,
};
use crate::auth::{StaticToken, TokenProvider};
use crate::publish::PublishAdmission;

impl Client {
    /// A client that acts as another principal over this client's connections.
    ///
    /// Every stream the returned client opens authenticates with `tenant_id`
    /// and a token from `tokens`, and the broker checks each of its requests
    /// against that token's grants and nothing else. It opens no connections
    /// of its own: a gateway serving many users holds one [`Client`] and one
    /// of these per user, and its connection count does not grow with them.
    ///
    /// It opens one publish stream and one cache stream before returning, so
    /// a refused token fails here. Subscriptions, watches and group requests
    /// open their own streams as they would on any client. Dropping it closes
    /// its streams and leaves the connections, and every other identity on
    /// them, alone.
    ///
    /// The broker ends a stream that carried a refused publish or cache
    /// request, on this client as on any other. With one stream of each, a
    /// refused request leaves this client unable to publish or reach the
    /// cache, so a gateway that forwards requests its users may not make
    /// should replace the client after a refusal.
    ///
    /// Publish admission is per identity, so one user's unanswered publishes
    /// cannot hold back another's. The connections' flow-control windows are
    /// shared, so a user whose subscription is not read can still slow the
    /// others' deliveries; drain each one promptly or drop it.
    pub async fn with_identity(
        &self,
        tenant_id: impl Into<String>,
        tokens: Arc<dyn TokenProvider>,
    ) -> Result<Client> {
        let auth_tenant_id = tenant_id.into();
        let credentials = Arc::new(
            self.credentials
                .for_identity(auth_tenant_id.clone(), tokens),
        );
        let settings: WorkerSettings = self.worker_settings;
        let mut worker_connections: Vec<QuicConnection> = Vec::new();

        let opened = self.publish_node.open_as(&credentials).await?;
        debug!("identity publish stream authenticated");
        let server_features = opened.negotiated.server_features;
        let server_features_hi = opened.negotiated.server_features_hi;
        note_connection(&mut worker_connections, opened.lease.connection());
        let publish_worker = spawn_publish_worker(opened, &self.runtime_config, settings);

        let cache_worker = open_cache_worker(
            &self.cache_node,
            &credentials,
            &self.cache_conn_counts,
            &mut worker_connections,
            self.runtime_config.max_frame_bytes,
        )
        .await?;

        let publish_shard_streams = shard_streams(
            &self.publish_node,
            &credentials,
            &worker_connections,
            self.runtime_config,
            settings,
        );
        let cache_request_counter = Arc::new(AtomicU64::new(1));
        let publish_widths = stream_widths(
            &self.event_node,
            &credentials,
            &auth_tenant_id,
            server_features,
            &cache_request_counter,
            self.runtime_config.max_frame_bytes,
        );
        Ok(Client {
            dialled: self.dialled,
            publish_node: Arc::clone(&self.publish_node),
            cache_node: Arc::clone(&self.cache_node),
            event_node: Arc::clone(&self.event_node),
            worker_connections,
            publish_workers: Arc::new(vec![publish_worker]),
            publish_sharding: self.publish_sharding,
            publish_admission: Arc::new(PublishAdmission::new(settings.publish_inflight_bytes)),
            publish_stream_hasher: ahash::RandomState::new(),
            publish_shard_streams,
            publish_widths,
            cache_workers: vec![cache_worker],
            cache_request_counter,
            cache_worker_rr: AtomicUsize::new(0),
            cache_conn_counts: Arc::clone(&self.cache_conn_counts),
            event_conn_counts: Arc::clone(&self.event_conn_counts),
            auth_tenant_id,
            credentials,
            runtime_config: self.runtime_config,
            worker_settings: settings,
            server_features,
            server_features_hi,
        })
    }

    /// [`Client::with_identity`] with a fixed token, for one that will not
    /// outlive the returned client.
    pub async fn with_identity_token(
        &self,
        tenant_id: impl Into<String>,
        token: impl Into<String>,
    ) -> Result<Client> {
        self.with_identity(tenant_id, Arc::new(StaticToken(token.into())))
            .await
    }
}

#[cfg(test)]
mod tests;
