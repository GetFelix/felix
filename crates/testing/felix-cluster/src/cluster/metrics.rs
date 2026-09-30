//! Reading what the brokers report about themselves.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};

use super::Cluster;
use crate::wait;

impl Cluster {
    /// The replicas `node_id` has stopped shipping to as a leader, from its
    /// `/replication/halted` listing. A halted replica is out of every quorum
    /// until it is rebuilt.
    pub(crate) async fn halted_replicas(&self, node_id: &str) -> Result<Vec<HaltedReplica>> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        self.http
            .get(format!("http://{}/replication/halted", node.metrics_addr))
            .send()
            .await
            .context("read /replication/halted")?
            .json()
            .await
            .context("decode /replication/halted")
    }

    /// Read one counter or gauge from a broker's `/metrics`.
    ///
    /// `None` when the metric has never been recorded, which for a counter is
    /// the difference between "zero so far" and "this code path never ran" —
    /// the distinction a cross-broker test is usually making.
    pub async fn metric(&self, node_id: &str, name: &str) -> Result<Option<f64>> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let body = self
            .http
            .get(format!("http://{}/metrics", node.metrics_addr))
            .send()
            .await
            .context("scrape metrics")?
            .text()
            .await
            .context("read metrics body")?;

        let mut total = None;
        for line in body.lines() {
            if line.starts_with('#') {
                continue;
            }
            // `name`, `name{labels}`, then the value. Summed across label sets,
            // because a caller asking "did this happen" does not care which
            // label it happened under.
            let Some(rest) = line.strip_prefix(name) else {
                continue;
            };
            if !(rest.starts_with(' ') || rest.starts_with('{')) {
                continue;
            }
            if let Some(value) = rest.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()) {
                *total.get_or_insert(0.0) += value;
            }
        }
        Ok(total)
    }

    /// Wait until the broker owning `stream` reports a replication pass, and
    /// return it.
    ///
    /// Tests kill a leader to watch a failover, and a leader that has shipped
    /// nothing tests startup instead. This is the wait that makes the shard
    /// healthy first.
    ///
    /// The owner is re-resolved on every poll rather than captured once. A
    /// caller publishing through an ordinary client does not choose which
    /// broker the bytes land on, and placement may move between the publish and
    /// this wait; a counter read from the wrong broker stays zero forever, and
    /// no timeout rescues that. Returning the node that actually reported is
    /// the point — kill *that* one.
    ///
    /// On timeout it reports every broker's counter, because "replication never
    /// ran" and "the wrong broker was being read" are the two explanations and
    /// the message should say which.
    pub async fn wait_for_replication(&self, stream: &str, timeout: Duration) -> Result<String> {
        let budget = wait::budget(timeout);
        let deadline = Instant::now() + budget;
        loop {
            if let Ok(owner) = self.owner(stream).await
                && let Ok(Some(shipped)) = self
                    .metric(&owner, "felix_broker_replication_shipped_total")
                    .await
                && shipped > 0.0
            {
                return Ok(owner);
            }
            if Instant::now() >= deadline {
                let mut seen = Vec::new();
                for node_id in self.node_ids() {
                    let shipped = self
                        .metric(&node_id, "felix_broker_replication_shipped_total")
                        .await
                        .ok()
                        .flatten();
                    seen.push(format!("{node_id}={shipped:?}"));
                }
                bail!(
                    "no owner of {stream} reported replication within {budget:?}; \
                     owner now {:?}, shipped per node: {}",
                    self.owner(stream).await.ok(),
                    seen.join(" "),
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// One entry of a broker's `/replication/halted`.
#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct HaltedReplica {
    /// `stream` or `cache`.
    pub(crate) kind: String,
    pub(crate) stream: String,
    pub(crate) shard: u32,
    /// The follower that stopped.
    pub(crate) node_id: String,
    pub(crate) reason: String,
}
