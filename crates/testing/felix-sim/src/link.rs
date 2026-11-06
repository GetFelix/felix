//! One broker's view of the control plane, which a fault can cut.
//!
//! [`NodeLink`] wraps the shared [`SimLink`](crate::controlplane::SimLink)
//! with a switch for this broker alone. Cut, a call goes nowhere and fails
//! after its timeout, as a request into a dead network does; the shared
//! control plane stays up for everyone else. The catalog fetch goes through
//! the same switch.

use std::future::Future;
use std::time::Duration;

use anyhow::{Result, anyhow};
use felix_replication::controlplane_link::{ControlPlaneLink, LinkResponse};
use serde::Serialize;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::controlplane::SimLink;

/// What the broker's HTTP client waits for an answer when the call names no
/// timeout of its own.
pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// The switch that cuts one broker off from the control plane.
#[derive(Clone)]
pub struct Cut {
    cut: watch::Sender<bool>,
}

impl Cut {
    pub(crate) fn new() -> Self {
        Self {
            cut: watch::channel(false).0,
        }
    }

    pub(crate) fn set(&self, cut: bool) {
        self.cut.send_replace(cut);
    }

    pub(crate) fn is_cut(&self) -> bool {
        *self.cut.borrow()
    }

    /// Run `call` under this switch and `limit`. A call that starts while the
    /// switch is on, or is still running when it goes on, is lost: it fails
    /// when `limit` runs out, counted from when it started.
    pub(crate) async fn through<T>(
        &self,
        limit: Duration,
        call: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let started = Instant::now();
        let mut cut = self.cut.subscribe();
        let timed_out = || anyhow!("request timed out after {limit:?} (simulated)");
        // Biased, so a link that is already cut loses the call even if the
        // call would finish at once.
        tokio::select! {
            biased;
            () = async { let _ = cut.wait_for(|cut| *cut).await; } => {
                tokio::time::sleep_until(started + limit).await;
                Err(timed_out())
            }
            answer = tokio::time::timeout(limit, call) => answer.map_err(|_| timed_out())?,
        }
    }
}

/// [`ControlPlaneLink`] for one broker: the shared link, behind its [`Cut`].
#[derive(Clone)]
pub struct NodeLink {
    inner: SimLink,
    cut: Cut,
}

impl NodeLink {
    pub(crate) fn new(inner: SimLink, cut: Cut) -> Self {
        Self { inner, cut }
    }
}

impl ControlPlaneLink for NodeLink {
    fn endpoint(&self) -> String {
        self.inner.endpoint()
    }

    async fn register(&self, bearer: &str, body: &(impl Serialize + Sync)) -> Result<LinkResponse> {
        let call = self.inner.register(bearer, body);
        self.cut.through(REQUEST_TIMEOUT, call).await
    }

    async fn node(&self, bearer: &str, node_id: &str) -> Result<LinkResponse> {
        let call = self.inner.node(bearer, node_id);
        self.cut.through(REQUEST_TIMEOUT, call).await
    }

    async fn heartbeat(
        &self,
        bearer: &str,
        node_id: &str,
        body: &(impl Serialize + Sync),
        timeout: Duration,
    ) -> Result<LinkResponse> {
        let call = self.inner.heartbeat(bearer, node_id, body, timeout);
        self.cut.through(timeout, call).await
    }

    async fn lifecycle(&self, bearer: &str, node_id: &str, action: &str) -> Result<LinkResponse> {
        let call = self.inner.lifecycle(bearer, node_id, action);
        self.cut.through(REQUEST_TIMEOUT, call).await
    }

    async fn node_catalog_page(
        &self,
        bearer: Option<&str>,
        cursor: Option<&str>,
    ) -> Result<LinkResponse> {
        let call = self.inner.node_catalog_page(bearer, cursor);
        self.cut.through(REQUEST_TIMEOUT, call).await
    }

    async fn assignment_snapshot(
        &self,
        bearer: Option<&str>,
        timeout: Duration,
    ) -> Result<LinkResponse> {
        let call = self.inner.assignment_snapshot(bearer, timeout);
        self.cut.through(timeout, call).await
    }

    async fn assignment_changes(
        &self,
        bearer: Option<&str>,
        since: u64,
        wait: Duration,
        timeout: Duration,
    ) -> Result<LinkResponse> {
        let call = self.inner.assignment_changes(bearer, since, wait, timeout);
        self.cut.through(timeout, call).await
    }

    async fn replica_status(
        &self,
        bearer: Option<&str>,
        node_id: &str,
        body: &(impl Serialize + Sync),
    ) -> Result<LinkResponse> {
        let call = self.inner.replica_status(bearer, node_id, body);
        self.cut.through(REQUEST_TIMEOUT, call).await
    }
}

#[cfg(test)]
mod tests;
