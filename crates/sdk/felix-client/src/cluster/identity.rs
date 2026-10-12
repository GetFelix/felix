//! A cluster client acting as another principal over the same connections.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;

use super::ClusterClient;
use super::nodes::Nodes;
use crate::auth::{StaticToken, TokenProvider};

impl ClusterClient {
    /// A cluster client that acts as another principal over this one's
    /// connections, and follows shards and failovers as this one does.
    ///
    /// It is [`Client::with_identity`](crate::Client::with_identity) for
    /// every broker this client reaches: each stream it opens authenticates
    /// with `tenant_id` and a token from `tokens`, and the broker checks each
    /// request against that token's grants alone. When it needs a broker,
    /// such as the new owner of a moved shard or a replacement for a failed
    /// one, it uses this client's connection to that broker, opening one only
    /// if this client has none, so a gateway serving many users holds one
    /// connection per broker however many users it serves.
    ///
    /// Routing state (shard owners, widths, group and cache routes) is its
    /// own, learned from its own requests. It starts from this client's
    /// endpoints and connects to the broker this client is using now, opening
    /// one publish stream and one cache stream there before returning, so a
    /// refused token fails here. Dropping it closes its streams and leaves the
    /// connections, and every other identity on them, alone.
    ///
    /// The limits are those of `Client::with_identity`: per-identity publish
    /// admission on the client, and on each broker a per-identity
    /// subscription cap and publish byte budget under a ceiling the identities
    /// on a connection share. A refused publish or cache request ends the
    /// stream it was sent on, so a gateway that forwards requests its users
    /// may not make should replace the client after a refusal.
    pub async fn with_identity(
        &self,
        tenant_id: impl Into<String>,
        tokens: Arc<dyn TokenProvider>,
    ) -> Result<ClusterClient> {
        let nodes = Arc::new(Nodes::for_identity(&self.nodes, tenant_id.into(), tokens));
        let mut endpoints = self.endpoints().await;
        // The broker in use first, so an identity starts where its parent is.
        let in_use = self.client().await.dialled();
        endpoints.sort_by_key(|addr| *addr != in_use);
        let client = nodes.connect_any(&endpoints).await?;
        let checked = client.supports_publish_width();
        let holder = Arc::new(RwLock::new(client));
        Ok(ClusterClient {
            seeds: self.seeds.clone(),
            endpoints: RwLock::new(self.endpoints().await),
            policy: self.policy.clone(),
            widths: super::cluster_widths(&holder, checked),
            client: holder,
            owners: RwLock::new(HashMap::new()),
            group_routes: RwLock::new(HashMap::new()),
            cache_routes: RwLock::new(HashMap::new()),
            nodes,
        })
    }

    /// [`ClusterClient::with_identity`] with a fixed token, for one that will
    /// not outlive the returned client.
    pub async fn with_identity_token(
        &self,
        tenant_id: impl Into<String>,
        token: impl Into<String>,
    ) -> Result<ClusterClient> {
        self.with_identity(tenant_id, Arc::new(StaticToken(token.into())))
            .await
    }
}
