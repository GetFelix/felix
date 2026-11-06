//! A control-plane link that answers from a table, to show each caller needs
//! nothing but the trait.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use felix_replication::controlplane_link::{ControlPlaneLink, LinkResponse};
use reqwest::StatusCode;
use serde::Serialize;

/// Records the name of each call and answers it with the JSON set for that
/// name, or a 503 when none is set.
#[derive(Clone, Default)]
pub(crate) struct FakeLink {
    calls: Arc<Mutex<Vec<&'static str>>>,
    replies: Arc<Mutex<HashMap<&'static str, serde_json::Value>>>,
    statuses: Arc<Mutex<HashMap<&'static str, StatusCode>>>,
}

impl FakeLink {
    pub(crate) fn reply(self, call: &'static str, body: serde_json::Value) -> Self {
        self.replies.lock().unwrap().insert(call, body);
        self
    }

    /// Answer `call` with an empty body and this status.
    pub(crate) fn status(self, call: &'static str, status: StatusCode) -> Self {
        self.statuses.lock().unwrap().insert(call, status);
        self
    }

    pub(crate) fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().unwrap().clone()
    }

    fn answer(&self, call: &'static str) -> Result<LinkResponse> {
        self.calls.lock().unwrap().push(call);
        if let Some(status) = self.statuses.lock().unwrap().get(call) {
            return Ok(LinkResponse {
                status: *status,
                body: Default::default(),
            });
        }
        Ok(match self.replies.lock().unwrap().get(call) {
            Some(body) => LinkResponse {
                status: StatusCode::OK,
                body: serde_json::to_vec(body).unwrap().into(),
            },
            None => LinkResponse {
                status: StatusCode::SERVICE_UNAVAILABLE,
                body: Default::default(),
            },
        })
    }
}

impl ControlPlaneLink for FakeLink {
    fn endpoint(&self) -> String {
        "fake".to_string()
    }

    async fn register(&self, _: &str, _: &(impl Serialize + Sync)) -> Result<LinkResponse> {
        self.answer("register")
    }

    async fn node(&self, _: &str, _: &str) -> Result<LinkResponse> {
        self.answer("node")
    }

    async fn heartbeat(
        &self,
        _: &str,
        _: &str,
        _: &(impl Serialize + Sync),
        _: Duration,
    ) -> Result<LinkResponse> {
        self.answer("heartbeat")
    }

    async fn lifecycle(&self, _: &str, _: &str, _: &str) -> Result<LinkResponse> {
        self.answer("lifecycle")
    }

    async fn node_catalog_page(&self, _: Option<&str>, _: Option<&str>) -> Result<LinkResponse> {
        self.answer("node_catalog_page")
    }

    async fn assignment_snapshot(&self, _: Option<&str>, _: Duration) -> Result<LinkResponse> {
        self.answer("assignment_snapshot")
    }

    async fn assignment_changes(
        &self,
        _: Option<&str>,
        _: u64,
        _: Duration,
        _: Duration,
    ) -> Result<LinkResponse> {
        self.answer("assignment_changes")
    }

    async fn replica_status(
        &self,
        _: Option<&str>,
        _: &str,
        _: &(impl Serialize + Sync),
    ) -> Result<LinkResponse> {
        self.answer("replica_status")
    }

    async fn confirmed_assignment(
        &self,
        _: Option<&str>,
        _: &felix_router::ShardKey,
        _: Duration,
    ) -> Result<LinkResponse> {
        self.answer("confirmed_assignment")
    }
}
