//! Finalizing fleet features, the way an operator does after an upgrade.

use std::time::Duration;

use anyhow::{Context, Result, anyhow};

use felix_controlplane_service::store::ControlPlaneStore;

use super::Cluster;
use crate::wait;

impl Cluster {
    /// Finalize `features` on the control plane and wait until every broker
    /// reports them all enabled. A broker that never turns one on fails here,
    /// before anything relies on it.
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
        for feature in features {
            store
                .finalize_fleet_feature(feature)
                .await
                .map_err(|err| anyhow!("finalize {feature}: {err}"))?;
        }
        let enabled = features.len() as f64;
        for id in self.node_ids() {
            wait::until(timeout, &format!("{id} to enable {features:?}"), || {
                let id = id.clone();
                async move {
                    self.metric(&id, "felix_broker_fleet_feature_enabled")
                        .await
                        .ok()
                        .flatten()
                        == Some(enabled)
                }
            })
            .await?;
        }
        Ok(())
    }
}
