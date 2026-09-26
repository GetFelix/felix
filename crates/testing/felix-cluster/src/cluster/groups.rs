//! Consumer-group operations through a chosen broker.

use anyhow::{Result, anyhow};

use super::Cluster;
use crate::client;

impl Cluster {
    /// Poll a consumer group through a named broker, whether or not it leads
    /// the shard.
    ///
    /// Going through a non-owner is the case worth testing: only the broker
    /// that leads a shard may serve its groups, because the claim and the
    /// acknowledgement have to reach the same in-flight state.
    pub async fn group_poll_via(
        &self,
        node_id: &str,
        stream: &str,
        shard: u32,
        group: &str,
        max_records: u32,
    ) -> Result<Vec<Vec<u8>>> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client = client::connect(node.client_addr, &self.tenant_id, &self.client_token).await?;
        let records = client
            .group_poll(
                &self.tenant_id,
                &self.namespace,
                stream,
                shard,
                group,
                max_records,
            )
            .await?;
        Ok(records
            .into_iter()
            .map(|record| record.payload.to_vec())
            .collect())
    }

    /// Take records for a group through a named broker, with their offsets.
    ///
    /// [`Cluster::group_poll_via`] hands back payloads alone; a test that
    /// acknowledges or hands back specific records needs the offsets too.
    pub async fn group_poll_records_via(
        &self,
        node_id: &str,
        stream: &str,
        shard: u32,
        group: &str,
        max_records: u32,
    ) -> Result<Vec<felix_wire::GroupRecord>> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client = client::connect(node.client_addr, &self.tenant_id, &self.client_token).await?;
        client
            .group_poll(
                &self.tenant_id,
                &self.namespace,
                stream,
                shard,
                group,
                max_records,
            )
            .await
    }

    /// Finish one record for a group, through a named broker.
    pub async fn group_ack_via(
        &self,
        node_id: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> Result<()> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client = client::connect(node.client_addr, &self.tenant_id, &self.client_token).await?;
        client
            .group_ack(
                &self.tenant_id,
                &self.namespace,
                stream,
                shard,
                group,
                offset,
            )
            .await
    }

    /// Hand one record back through a named broker, for immediate redelivery.
    pub async fn group_nack_via(
        &self,
        node_id: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> Result<()> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client = client::connect(node.client_addr, &self.tenant_id, &self.client_token).await?;
        client
            .group_nack(
                &self.tenant_id,
                &self.namespace,
                stream,
                shard,
                group,
                offset,
            )
            .await
    }

    /// The offsets a group gave up on, through a named broker.
    pub async fn group_dead_letters_via(
        &self,
        node_id: &str,
        stream: &str,
        shard: u32,
        group: &str,
    ) -> Result<Vec<u64>> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client = client::connect(node.client_addr, &self.tenant_id, &self.client_token).await?;
        client
            .group_dead_letters(&self.tenant_id, &self.namespace, stream, shard, group)
            .await
    }

    /// Put one dead letter back in the queue, through a named broker.
    pub async fn group_redrive_via(
        &self,
        node_id: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> Result<()> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client = client::connect(
            node.client_addr,
            &self.tenant_id,
            &self.group_operator_token,
        )
        .await?;
        client
            .group_redrive(
                &self.tenant_id,
                &self.namespace,
                stream,
                shard,
                group,
                offset,
            )
            .await
    }
}
