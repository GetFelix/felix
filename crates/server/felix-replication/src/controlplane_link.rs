//! How a broker talks to the control plane.
//!
//! Every call a broker makes to the control plane goes through
//! [`ControlPlaneLink`]: registration and heartbeats, the assignment watch,
//! the node catalog and replica reports. [`HttpControlPlaneLink`] is the one
//! production implementation. The trait exists so a test harness can answer
//! the same calls in-process, with faults of its own, instead of over a socket.
//!
//! A method returns the control plane's answer as a status and a body and
//! leaves the meaning of a status to the caller, so each caller keeps its own
//! error mapping. Fleet features have no call of their own: registration and
//! heartbeat answers carry them.
use std::future::Future;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use reqwest::StatusCode;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// The control plane's answer to one call.
#[derive(Debug, Clone)]
pub struct LinkResponse {
    pub status: StatusCode,
    pub body: Bytes,
}

impl LinkResponse {
    /// The body as text, for an error message.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// The body decoded as JSON.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_slice(&self.body).context("decode response")
    }
}

/// The broker's calls to the control plane.
///
/// `Err` means no answer came back (connection, timeout). An answer with any
/// status is `Ok`. `bearer` is the node credential, read by the caller on
/// every call so a refreshed token is used from the next one.
pub trait ControlPlaneLink: Send + Sync + 'static {
    /// Where the control plane is, for error messages.
    fn endpoint(&self) -> String;

    /// `POST /v1/nodes`.
    fn register(
        &self,
        bearer: &str,
        body: &(impl Serialize + Sync),
    ) -> impl Future<Output = Result<LinkResponse>> + Send;

    /// `GET /v1/nodes/{node_id}`.
    fn node(
        &self,
        bearer: &str,
        node_id: &str,
    ) -> impl Future<Output = Result<LinkResponse>> + Send;

    /// `POST /v1/nodes/{node_id}/heartbeat`, abandoned after `timeout`.
    fn heartbeat(
        &self,
        bearer: &str,
        node_id: &str,
        body: &(impl Serialize + Sync),
        timeout: Duration,
    ) -> impl Future<Output = Result<LinkResponse>> + Send;

    /// `POST /v1/nodes/{node_id}/{action}`, for `drain` and `deregister`.
    fn lifecycle(
        &self,
        bearer: &str,
        node_id: &str,
        action: &str,
    ) -> impl Future<Output = Result<LinkResponse>> + Send;

    /// One page of `GET /v1/nodes`, starting at `cursor`.
    fn node_catalog_page(
        &self,
        bearer: Option<&str>,
        cursor: Option<&str>,
    ) -> impl Future<Output = Result<LinkResponse>> + Send;

    /// `GET /v1/shard-assignments/snapshot`, abandoned after `timeout`.
    fn assignment_snapshot(
        &self,
        bearer: Option<&str>,
        timeout: Duration,
    ) -> impl Future<Output = Result<LinkResponse>> + Send;

    /// `GET /v1/shard-assignments/changes`, a long-poll the control plane may
    /// hold for up to `wait`, abandoned after `timeout`.
    fn assignment_changes(
        &self,
        bearer: Option<&str>,
        since: u64,
        wait: Duration,
        timeout: Duration,
    ) -> impl Future<Output = Result<LinkResponse>> + Send;

    /// `POST /v1/nodes/{node_id}/replica-status`.
    fn replica_status(
        &self,
        bearer: Option<&str>,
        node_id: &str,
        body: &(impl Serialize + Sync),
    ) -> impl Future<Output = Result<LinkResponse>> + Send;
}

/// [`ControlPlaneLink`] over the control plane's REST API.
#[derive(Clone)]
pub struct HttpControlPlaneLink {
    client: reqwest::Client,
    base_url: String,
}

impl HttpControlPlaneLink {
    pub fn new(client: reqwest::Client, base_url: &str) -> Self {
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }
}

fn bearer_if(request: reqwest::RequestBuilder, bearer: Option<&str>) -> reqwest::RequestBuilder {
    match bearer {
        Some(bearer) => request.bearer_auth(bearer),
        None => request,
    }
}

async fn finish(request: reqwest::RequestBuilder) -> Result<LinkResponse> {
    let response = request.send().await?;
    let status = response.status();
    // Callers that decode the body fail on the empty one, as they did on a
    // failed read. The rest judge by status alone and never read it.
    let body = response.bytes().await.unwrap_or_default();
    Ok(LinkResponse { status, body })
}

impl ControlPlaneLink for HttpControlPlaneLink {
    fn endpoint(&self) -> String {
        self.base_url.clone()
    }

    async fn register(&self, bearer: &str, body: &(impl Serialize + Sync)) -> Result<LinkResponse> {
        // Proves this broker may claim the `node_id` in the body: the control
        // plane authorises the identity against the credential.
        finish(
            self.client
                .post(self.url("/v1/nodes"))
                .bearer_auth(bearer)
                .json(body),
        )
        .await
    }

    async fn node(&self, bearer: &str, node_id: &str) -> Result<LinkResponse> {
        finish(
            self.client
                .get(self.url(&format!("/v1/nodes/{node_id}")))
                .bearer_auth(bearer),
        )
        .await
    }

    async fn heartbeat(
        &self,
        bearer: &str,
        node_id: &str,
        body: &(impl Serialize + Sync),
        timeout: Duration,
    ) -> Result<LinkResponse> {
        finish(
            self.client
                .post(self.url(&format!("/v1/nodes/{node_id}/heartbeat")))
                .timeout(timeout)
                .bearer_auth(bearer)
                .json(body),
        )
        .await
    }

    async fn lifecycle(&self, bearer: &str, node_id: &str, action: &str) -> Result<LinkResponse> {
        finish(
            self.client
                .post(self.url(&format!("/v1/nodes/{node_id}/{action}")))
                .bearer_auth(bearer),
        )
        .await
    }

    async fn node_catalog_page(
        &self,
        bearer: Option<&str>,
        cursor: Option<&str>,
    ) -> Result<LinkResponse> {
        let mut request = self.client.get(self.url("/v1/nodes"));
        if let Some(cursor) = cursor {
            request = request.query(&[("cursor", cursor)]);
        }
        finish(bearer_if(request, bearer)).await
    }

    async fn assignment_snapshot(
        &self,
        bearer: Option<&str>,
        timeout: Duration,
    ) -> Result<LinkResponse> {
        let request = self
            .client
            .get(self.url("/v1/shard-assignments/snapshot"))
            .timeout(timeout);
        finish(bearer_if(request, bearer)).await
    }

    async fn assignment_changes(
        &self,
        bearer: Option<&str>,
        since: u64,
        wait: Duration,
        timeout: Duration,
    ) -> Result<LinkResponse> {
        let request = self
            .client
            .get(self.url(&format!(
                "/v1/shard-assignments/changes?since={since}&wait_ms={}",
                wait.as_millis()
            )))
            .timeout(timeout);
        finish(bearer_if(request, bearer)).await
    }

    async fn replica_status(
        &self,
        bearer: Option<&str>,
        node_id: &str,
        body: &(impl Serialize + Sync),
    ) -> Result<LinkResponse> {
        let request = self
            .client
            .post(self.url(&format!("/v1/nodes/{node_id}/replica-status")))
            .json(body);
        finish(bearer_if(request, bearer)).await
    }
}

#[cfg(test)]
mod tests;
