//! Publishing and subscribing through a chosen broker.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};

use super::Cluster;
use crate::client;

impl Cluster {
    /// An authenticated client connected to one named broker.
    ///
    /// Exposed for tests that care about the connection itself rather than
    /// what is published over it -- which listener the pools landed on, for
    /// instance.
    pub async fn client_on(&self, node_id: &str) -> Result<felix_client::Client> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        client::connect(node.client_addr, &self.tenant_id, &self.client_token()).await
    }

    /// How wide a named broker believes `stream` is.
    ///
    /// This is the width that broker will route a keyed publish with, read
    /// through the same routing snapshot the publish path reads — so it is the
    /// signal to wait on before publishing keys that are expected to spread.
    pub async fn stream_shards_via(&self, node_id: &str, stream: &str) -> Result<u32> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client =
            client::connect(node.client_addr, &self.tenant_id, &self.client_token()).await?;
        client
            .stream_shards(&self.tenant_id, &self.namespace, stream)
            .await
            .with_context(|| format!("ask {node_id} how wide {stream} is"))
    }

    /// Publish one record through a named broker, whether or not it owns the
    /// shard.
    pub async fn publish_via(&self, node_id: &str, stream: &str, payload: Vec<u8>) -> Result<()> {
        self.publish_via_token(node_id, stream, payload, &self.client_token())
            .await
    }

    /// Publish one record through a named broker and return the offset its
    /// ack reported, `None` when the broker did not report one.
    pub async fn publish_via_at(
        &self,
        node_id: &str,
        stream: &str,
        payload: Vec<u8>,
    ) -> Result<Option<u64>> {
        self.publish_through(node_id, stream, payload, &self.client_token())
            .await
    }

    /// Publish through a named broker with a chosen credential.
    pub async fn publish_via_token(
        &self,
        node_id: &str,
        stream: &str,
        payload: Vec<u8>,
        token: &str,
    ) -> Result<()> {
        self.publish_through(node_id, stream, payload, token)
            .await
            .map(|_| ())
    }

    async fn publish_through(
        &self,
        node_id: &str,
        stream: &str,
        payload: Vec<u8>,
        token: &str,
    ) -> Result<Option<u64>> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client = client::connect(node.client_addr, &self.tenant_id, token).await?;
        let publisher = client.publisher().await.context("open publisher")?;
        // Acked, because an unacked publish would make every failure look like
        // success and the wait above would pass instantly.
        publisher
            .publish(
                &self.tenant_id,
                &self.namespace,
                stream,
                payload,
                felix_wire::AckMode::PerMessage,
            )
            .await
            .with_context(|| format!("publish to {stream} via {node_id}"))
    }

    /// Publish through whichever broker in the cluster answers, the way an
    /// application with a seed list would.
    pub async fn publish_via_any(&self, stream: &str, payload: Vec<u8>) -> Result<()> {
        let client =
            client::connect_any(&self.broker_addrs(), &self.tenant_id, &self.client_token())
                .await?;
        let publisher = client.publisher().await.context("open publisher")?;
        publisher
            .publish(
                &self.tenant_id,
                &self.namespace,
                stream,
                payload,
                felix_wire::AckMode::PerMessage,
            )
            .await
            .with_context(|| format!("publish to {stream} through a seed endpoint"))
            .map(|_| ())
    }

    /// Publish with a routing key, through a named broker.
    pub async fn publish_keyed_via(
        &self,
        node_id: &str,
        stream: &str,
        key: &[u8],
        payload: Vec<u8>,
    ) -> Result<()> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client =
            client::connect(node.client_addr, &self.tenant_id, &self.client_token()).await?;
        let publisher = client.publisher().await.context("open publisher")?;
        publisher
            .publish_keyed(
                &self.tenant_id,
                &self.namespace,
                stream,
                bytes::Bytes::copy_from_slice(key),
                payload,
                felix_wire::AckMode::PerMessage,
            )
            .await
            .with_context(|| format!("publish to {stream} key {key:?} via {node_id}"))
            .map(|_| ())
    }

    /// Publish with a routing key, waiting out the window where two brokers
    /// disagree about who owns a shard.
    ///
    /// A broker that has the placement forwards to the shard's owner. If that
    /// owner has not applied the assignment yet it refuses — *"owner broker-2
    /// redirected to generation 0, not ahead of 0"* — and the publish fails.
    /// The window is real, it closes on its own as the shard feed catches up,
    /// and nothing in the client retries it (#269).
    ///
    /// So a test that needs every record to land has to wait the window out
    /// rather than treat the first attempt as the answer. In practice only the
    /// first publish after a cluster starts or a shard moves pays anything.
    ///
    /// Bounded on purpose: routing that never converges is a bug, and the
    /// caller should still see a failure rather than hang.
    pub async fn publish_keyed_via_settled(
        &self,
        node_id: &str,
        stream: &str,
        key: &[u8],
        payload: Vec<u8>,
        within: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + within;
        loop {
            match self
                .publish_keyed_via(node_id, stream, key, payload.clone())
                .await
            {
                Ok(()) => return Ok(()),
                Err(err) if Instant::now() >= deadline => {
                    // Routing that never converges is a bug somewhere, and the
                    // brokers' own logs are the only place its shape survives —
                    // the harness's tempdir takes them when the test ends.
                    let mut evidence = String::new();
                    for node in &self.nodes {
                        let tail = node.log_lines_matching(
                            &[
                                "shard",
                                "route",
                                "assignment",
                                "watch",
                                "forward",
                                "lease",
                                "open",
                            ],
                            40,
                        );
                        if !tail.is_empty() {
                            evidence.push_str(&format!("\n--- {} ---\n{tail}", node.node_id));
                        }
                    }
                    return Err(err.context(format!(
                        "routing did not settle within {within:?}{evidence}"
                    )));
                }
                Err(_) => {}
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Subscribe on a named broker and return the client and subscription.
    ///
    /// Both are returned because dropping the client closes the connection the
    /// subscription is delivered on.
    pub async fn subscribe_on(
        &self,
        node_id: &str,
        stream: &str,
    ) -> Result<(felix_client::Client, felix_client::Subscription)> {
        self.subscribe_on_with_token(node_id, stream, &self.client_token())
            .await
    }

    /// Subscribe on a named broker with a chosen credential.
    pub async fn subscribe_on_with_token(
        &self,
        node_id: &str,
        stream: &str,
        token: &str,
    ) -> Result<(felix_client::Client, felix_client::Subscription)> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client = client::connect(node.client_addr, &self.tenant_id, token).await?;
        let subscription = client
            .subscribe(&self.tenant_id, &self.namespace, stream)
            .await
            .with_context(|| format!("subscribe to {stream} on {node_id}"))?;
        Ok((client, subscription))
    }

    /// Subscribe on a named broker, replaying from the start of the stream.
    ///
    /// A failure test needs this: the records it cares about were published
    /// before the fault, and a live subscription would not replay them. Asking
    /// for history is also the stronger check — it reads what the broker has on
    /// disk rather than what it happens to fan out next.
    pub async fn replay_on(
        &self,
        node_id: &str,
        stream: &str,
    ) -> Result<(felix_client::Client, felix_client::Subscription)> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client =
            client::connect(node.client_addr, &self.tenant_id, &self.client_token()).await?;
        let subscription = client
            .subscribe_from(
                &self.tenant_id,
                &self.namespace,
                stream,
                Some(felix_client::StartPosition::Earliest),
            )
            .await
            .with_context(|| format!("replay {stream} on {node_id}"))?;
        Ok((client, subscription))
    }

    /// Replay one shard of a stream from the broker that owns it.
    pub async fn replay_shard(
        &self,
        node_id: &str,
        stream: &str,
        shard: u32,
    ) -> Result<(felix_client::Client, felix_client::Subscription)> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client =
            client::connect(node.client_addr, &self.tenant_id, &self.client_token()).await?;
        let subscription = client
            .subscribe_shard(
                &self.tenant_id,
                &self.namespace,
                stream,
                shard,
                Some(felix_client::StartPosition::Earliest),
            )
            .await
            .with_context(|| format!("replay {stream} shard {shard} on {node_id}"))?;
        Ok((client, subscription))
    }
}
