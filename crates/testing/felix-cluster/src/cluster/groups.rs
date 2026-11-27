//! Consumer-group operations through a chosen broker.

use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};

use super::Cluster;
use felix_wire::RetryClass;

use crate::client;

/// How long a refused group call is sent again before the refusal is returned.
const RETRY_BUDGET: Duration = Duration::from_secs(30);

/// A new leader refuses group calls until it has committed in its term: a read until it
/// has a mark, a write while its engine has not settled a batch. Clients send those
/// again, so a test that calls right after a move or failover does too. Any other error
/// is returned at once, and so is the last refusal once the budget is spent.
async fn retry_refused<T, F, Fut>(mut call: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let deadline = Instant::now() + crate::wait::budget(RETRY_BUDGET);
    loop {
        match call().await {
            Err(err) if is_retryable(&err) && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            other => return other,
        }
    }
}

/// Only the classes that promise nothing was applied and that waiting can clear.
/// `Redirect` says to ask another broker, and these helpers are pinned to one, so tests
/// that probe a non-owner see that refusal at once. `OutcomeUnknown` and an error with
/// no broker code (a lost connection) may have been applied, so they stay final.
fn is_retryable(err: &anyhow::Error) -> bool {
    err.downcast_ref::<felix_client::BrokerError>()
        .is_some_and(|broker| {
            matches!(
                broker.retry,
                RetryClass::Retry | RetryClass::RetryAfter | RetryClass::Redirect
            )
        })
}

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
        let client =
            client::connect(node.client_addr, &self.tenant_id, &self.client_token()).await?;
        let records = retry_refused(|| {
            client.group_poll(
                &self.tenant_id,
                &self.namespace,
                stream,
                shard,
                group,
                max_records,
            )
        })
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
        let client =
            client::connect(node.client_addr, &self.tenant_id, &self.client_token()).await?;
        retry_refused(|| {
            client.group_poll(
                &self.tenant_id,
                &self.namespace,
                stream,
                shard,
                group,
                max_records,
            )
        })
        .await
    }

    /// Poll as a named member through a named broker, asking for its
    /// earlier claims back when `reclaim` is set.
    #[allow(clippy::too_many_arguments)]
    pub async fn group_poll_as_via(
        &self,
        node_id: &str,
        stream: &str,
        shard: u32,
        group: &str,
        consumer: &str,
        reclaim: bool,
        max_records: u32,
    ) -> Result<Vec<felix_wire::GroupRecord>> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client =
            client::connect(node.client_addr, &self.tenant_id, &self.client_token()).await?;
        let member = felix_client::GroupMember {
            consumer: consumer.to_owned(),
            reclaim,
        };
        retry_refused(|| {
            client.group_poll_as(
                &self.tenant_id,
                &self.namespace,
                stream,
                shard,
                group,
                Some(&member),
                max_records,
                Duration::ZERO,
            )
        })
        .await
    }

    /// Extend the claim `record` was delivered under, through a named broker.
    pub async fn group_extend_via(
        &self,
        node_id: &str,
        stream: &str,
        shard: u32,
        group: &str,
        record: &felix_wire::GroupRecord,
        by: Duration,
    ) -> Result<Duration> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client =
            client::connect(node.client_addr, &self.tenant_id, &self.client_token()).await?;
        retry_refused(|| {
            client.group_extend(
                &self.tenant_id,
                &self.namespace,
                stream,
                shard,
                group,
                record,
                by,
            )
        })
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
        let client =
            client::connect(node.client_addr, &self.tenant_id, &self.client_token()).await?;
        retry_refused(|| {
            client.group_ack(
                &self.tenant_id,
                &self.namespace,
                stream,
                shard,
                group,
                offset,
            )
        })
        .await
    }

    /// Hand one record back through a named broker, for immediate redelivery.
    pub async fn group_nack_via(
        &self,
        node_id: &str,
        stream: &str,
        shard: u32,
        group: &str,
        record: &felix_wire::GroupRecord,
    ) -> Result<()> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client =
            client::connect(node.client_addr, &self.tenant_id, &self.client_token()).await?;
        retry_refused(|| {
            client.group_nack(
                &self.tenant_id,
                &self.namespace,
                stream,
                shard,
                group,
                record,
            )
        })
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
        let client =
            client::connect(node.client_addr, &self.tenant_id, &self.client_token()).await?;
        retry_refused(|| {
            client.group_dead_letters(&self.tenant_id, &self.namespace, stream, shard, group)
        })
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
            &self.group_operator_token(),
        )
        .await?;
        retry_refused(|| {
            client.group_redrive(
                &self.tenant_id,
                &self.namespace,
                stream,
                shard,
                group,
                offset,
            )
        })
        .await
    }
}

#[cfg(test)]
mod tests;
