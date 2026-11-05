//! Reads of the control plane's snapshots and change feeds.
//!
//! The reads go through [`CatalogFetch`], so the sync pass is the same code
//! over HTTP in a broker and over an in-process link in the simulator.

use std::future::Future;

use anyhow::{Context, Result};
use bytes::Bytes;
use serde::de::DeserializeOwned;

use super::wire::*;

/// Where the catalog sync reads from: one GET of a control-plane path.
///
/// `path` starts with `/` and includes the query, e.g.
/// `/v1/streams/changes?since=7`. A non-2xx answer is an `Err`.
pub(crate) trait CatalogFetch: Send + Sync {
    fn get(&self, path: &str, bearer: Option<&str>) -> impl Future<Output = Result<Bytes>> + Send;
}

/// [`CatalogFetch`] over the control plane's REST API.
pub(crate) struct HttpSource {
    client: reqwest::Client,
    base_url: String,
}

impl HttpSource {
    pub(crate) fn new(client: reqwest::Client, base_url: impl Into<String>) -> Self {
        Self {
            client,
            base_url: base_url.into(),
        }
    }
}

impl CatalogFetch for HttpSource {
    async fn get(&self, path: &str, bearer: Option<&str>) -> Result<Bytes> {
        let url = format!("{}{path}", self.base_url.trim_end_matches('/'));
        let request = self.client.get(url);
        let request = match bearer {
            Some(bearer) => request.bearer_auth(bearer),
            None => request,
        };
        let response = request
            .send()
            .await
            .context("request")?
            .error_for_status()
            .context("status")?;
        response.bytes().await.context("body")
    }
}

async fn fetch_json<T: DeserializeOwned>(
    source: &impl CatalogFetch,
    path: &str,
    bearer: Option<&str>,
    what: &str,
) -> Result<T> {
    let body = source
        .get(path, bearer)
        .await
        .with_context(|| format!("{what} request"))?;
    serde_json::from_slice(&body).with_context(|| format!("{what} body"))
}

/// Fetches the full snapshot from `/v1/streams/snapshot`.
pub(super) async fn fetch_snapshot(
    source: &impl CatalogFetch,
    bearer: Option<&str>,
) -> Result<StreamSnapshotResponse> {
    fetch_json(source, "/v1/streams/snapshot", bearer, "snapshot").await
}

/// Fetches the full cache snapshot from `/v1/caches/snapshot`.
pub(super) async fn fetch_cache_snapshot(
    source: &impl CatalogFetch,
    bearer: Option<&str>,
) -> Result<CacheSnapshotResponse> {
    fetch_json(source, "/v1/caches/snapshot", bearer, "cache snapshot").await
}

/// Fetches the full tenant snapshot from `/v1/tenants/snapshot`.
pub(super) async fn fetch_tenant_snapshot(
    source: &impl CatalogFetch,
    bearer: Option<&str>,
) -> Result<TenantSnapshotResponse> {
    fetch_json(source, "/v1/tenants/snapshot", bearer, "tenant snapshot").await
}

/// Fetches the full namespace snapshot from `/v1/namespaces/snapshot`.
pub(super) async fn fetch_namespace_snapshot(
    source: &impl CatalogFetch,
    bearer: Option<&str>,
) -> Result<NamespaceSnapshotResponse> {
    fetch_json(
        source,
        "/v1/namespaces/snapshot",
        bearer,
        "namespace snapshot",
    )
    .await
}

/// Fetches the changes feed from `/v1/streams/changes?since=<seq>`.
pub(super) async fn fetch_changes(
    source: &impl CatalogFetch,
    bearer: Option<&str>,
    since: u64,
) -> Result<StreamChangesResponse> {
    fetch_json(
        source,
        &format!("/v1/streams/changes?since={since}"),
        bearer,
        "changes",
    )
    .await
}

/// Fetches the cache changes feed from `/v1/caches/changes?since=<seq>`.
pub(super) async fn fetch_cache_changes(
    source: &impl CatalogFetch,
    bearer: Option<&str>,
    since: u64,
) -> Result<CacheChangesResponse> {
    fetch_json(
        source,
        &format!("/v1/caches/changes?since={since}"),
        bearer,
        "cache changes",
    )
    .await
}

/// Fetches the tenant changes feed from `/v1/tenants/changes?since=<seq>`.
pub(super) async fn fetch_tenant_changes(
    source: &impl CatalogFetch,
    bearer: Option<&str>,
    since: u64,
) -> Result<TenantChangesResponse> {
    fetch_json(
        source,
        &format!("/v1/tenants/changes?since={since}"),
        bearer,
        "tenant changes",
    )
    .await
}

/// Fetches the namespace changes feed from `/v1/namespaces/changes?since=<seq>`.
pub(super) async fn fetch_namespace_changes(
    source: &impl CatalogFetch,
    bearer: Option<&str>,
    since: u64,
) -> Result<NamespaceChangesResponse> {
    fetch_json(
        source,
        &format!("/v1/namespaces/changes?since={since}"),
        bearer,
        "namespace changes",
    )
    .await
}
