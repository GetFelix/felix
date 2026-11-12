//! Finalizing fleet features, the way an operator does after an upgrade.

use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};

use super::Cluster;
use crate::wait;

impl Cluster {
    /// Finalize `features` on the control plane and wait until every broker
    /// reports each of them enabled. A broker that never turns one on fails
    /// here, before anything relies on it.
    ///
    /// The store's finalize skips the operator's prerequisite check, so each
    /// feature's prerequisites are finalized first, as an operator must:
    /// `joint_fence` without `stream_incarnations` leaves every stream
    /// created afterwards unable to open (#1862).
    pub async fn finalize_fleet_features(
        &self,
        features: &[&str],
        timeout: Duration,
    ) -> Result<()> {
        if features.is_empty() {
            return Ok(());
        }
        let store = &self
            .control_plane
            .as_ref()
            .context("finalizing fleet features needs the control plane")?
            .store;
        let mut ordered: Vec<&str> = Vec::new();
        for feature in features {
            let needed = felix_common::fleet::prerequisites(feature)
                .iter()
                .map(|needed| needed.name());
            for name in needed.chain([*feature]) {
                if !ordered.contains(&name) {
                    ordered.push(name);
                }
            }
        }
        for feature in &ordered {
            store
                .finalize_fleet_feature(feature)
                .await
                .map_err(|err| anyhow!("finalize {feature}: {err}"))?;
        }
        for id in self.node_ids() {
            wait::until(timeout, &format!("{id} to enable {ordered:?}"), || {
                let id = id.clone();
                let ordered = &ordered;
                async move {
                    self.fleet_features_enabled(&id)
                        .await
                        .is_ok_and(|enabled| ordered.iter().all(|f| enabled.contains(*f)))
                }
            })
            .await?;
        }
        Ok(())
    }

    /// The fleet features `node_id` reports enabled, by the `feature` label
    /// of its `felix_broker_fleet_feature_enabled` gauge.
    async fn fleet_features_enabled(&self, node_id: &str) -> Result<BTreeSet<String>> {
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
        Ok(body
            .lines()
            .filter_map(|line| line.strip_prefix("felix_broker_fleet_feature_enabled{"))
            .filter(|rest| {
                rest.rsplit(' ')
                    .next()
                    .and_then(|value| value.parse::<f64>().ok())
                    == Some(1.0)
            })
            .filter_map(|rest| {
                let (_, after) = rest.split_once("feature=\"")?;
                Some(after.split('"').next()?.to_string())
            })
            .collect())
    }
}
