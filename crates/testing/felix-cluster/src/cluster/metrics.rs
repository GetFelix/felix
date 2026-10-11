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

    /// Wait until a follower of `stream`'s shard 0 holds everything its leader
    /// has, as the control plane has heard it, and return the leader.
    ///
    /// Tests kill or pause a leader to watch a failover, and failover keeps
    /// only what the leader last reported a follower holds. So this waits for
    /// a report at the leader's current generation, made at or past the tail
    /// the leader has now, that names a caught-up follower whose position is
    /// that tail. Records acknowledged before the call are then on a follower
    /// placement would promote.
    ///
    /// The owner and its tail are read again on every poll: placement may
    /// move between a publish and this wait, and a report about the wrong
    /// broker would never satisfy it. The timeout says which broker led, its
    /// tail, and what the last report claimed about each replica.
    pub async fn wait_for_replication(&self, stream: &str, timeout: Duration) -> Result<String> {
        let budget = wait::budget(timeout);
        let deadline = Instant::now() + budget;
        loop {
            let why = match self.replication_level(stream).await {
                Ok((owner, Ok(_))) => return Ok(owner),
                Ok((owner, Err(short))) => format!("leader {owner}: {short}"),
                Err(err) => format!("{err:#}"),
            };
            if Instant::now() >= deadline {
                bail!(
                    "{stream} shard 0 had no follower holding its leader's tail within {budget:?}; {why}"
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// The owner of `stream`'s shard 0, and whether its last report names a
    /// follower that holds the owner's tail as it stands now.
    async fn replication_level(&self, stream: &str) -> Result<(String, Result<String, String>)> {
        let owner = self.owner(stream).await.context("resolve the owner")?;
        let node = self
            .node(&owner)
            .ok_or_else(|| anyhow!("unknown node {owner}"))?;
        let client =
            crate::client::connect(node.client_addr, &self.tenant_id, &self.inspector_token())
                .await?;
        let view = client
            .inspect_shard(
                felix_client::ShardKind::Stream,
                &self.tenant_id,
                &self.namespace,
                stream,
                0,
            )
            .await
            .with_context(|| format!("inspect {stream} shard 0 on {owner}"))?;
        drop(client);
        if view.role != "leader" {
            return Ok((owner, Err(format!("its own role is {}", view.role))));
        }
        let Some(tail) = view.tail else {
            return Ok((owner, Err("its log is not open".to_string())));
        };
        let report = self.replica_report(stream, 0).await?;
        Ok((owner, holds_tail(report.as_ref(), view.generation, tail)))
    }
}

/// The follower `report` names as caught up and holding `tail`, or why there
/// is none.
///
/// `caught_up` alone is measured against what the leader had acknowledged when
/// it reported, which may be short of `tail`, so the follower's own position
/// has to reach it too.
fn holds_tail(
    report: Option<&felix_controlplane_service::model::ReplicaReport>,
    generation: Option<u64>,
    tail: u64,
) -> Result<String, String> {
    let Some(report) = report else {
        return Err(format!("tail {tail}, and no replica report yet"));
    };
    let summary = || {
        format!(
            "tail {tail} at generation {generation:?}; last report: generation {}, \
             leader at {:?}, caught up {:?}, replica offsets {:?}",
            report.generation, report.leader_offset, report.caught_up, report.offsets
        )
    };
    if generation.is_some_and(|generation| generation != report.generation) {
        return Err(format!("report is from another generation: {}", summary()));
    }
    if report.leader_offset.is_none_or(|reported| reported < tail) {
        return Err(format!("report predates the tail: {}", summary()));
    }
    report
        .caught_up
        .iter()
        .find(|follower| report.offsets.get(*follower).is_some_and(|at| *at >= tail))
        .cloned()
        .ok_or_else(|| format!("no caught-up replica at the tail: {}", summary()))
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

#[cfg(test)]
mod tests;
