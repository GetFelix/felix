//! What an operator does to placement, through the control plane's operator
//! API: move a shard, cancel a move, drain a node and put it back, take a node
//! out of the cluster, and abandon a shard's log. Each is the request `felixctl`
//! sends, except putting a node back (`PATCH /v1/nodes/{id}`) and deleting its
//! record (`DELETE /v1/nodes/{id}`), which `felixctl` does not wrap.
//!
//! Each goes through the fault gate like any other call, so an operator
//! talking to a paused control plane waits and one talking to a downed one is
//! refused. A refusal (any status but success) is an [`Answer`], not an error:
//! the nemesis records it and moves on, as an operator reading a 409 would.

use anyhow::Result;
use axum::http::Method;
use felix_controlplane_service::api::types::ShardMoveRequest;
use felix_controlplane_service::model::{
    NodeLifecycle, NodePatchRequest, ShardAssignmentChange, ShardKey as CpShardKey,
    ShardKind as CpShardKind,
};
use felix_controlplane_service::store::ControlPlaneStore;
use felix_replication::controlplane_link::LinkResponse;

use super::SimControlPlane;
use felix_broker_service::sim::{ShardKey, ShardKind};

/// How the control plane answered an operator request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub ok: bool,
    /// The status and, for a refusal, the body, for the fault log.
    pub text: String,
}

impl Answer {
    fn from(response: &LinkResponse) -> Self {
        let ok = response.status.is_success();
        let text = if ok {
            response.status.to_string()
        } else {
            format!("{} {}", response.status, response.text())
        };
        Self { ok, text }
    }

    fn unreachable(err: &anyhow::Error) -> Self {
        Self {
            ok: false,
            text: format!("no answer: {err:#}"),
        }
    }
}

impl SimControlPlane {
    async fn operator(
        &self,
        method: Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Answer {
        match self.request(method, path, body).await {
            Ok(response) => Answer::from(&response),
            Err(err) => Answer::unreachable(&err),
        }
    }

    /// `felixctl shard move`: move `key`'s leadership to `destination`.
    pub async fn move_shard(&self, key: &ShardKey, destination: &str) -> Result<Answer> {
        let body = ShardMoveRequest {
            key: cp_key(key),
            destination: destination.to_string(),
            dry_run: false,
        };
        Ok(self
            .operator(
                Method::POST,
                "/v1/shard-moves",
                Some(serde_json::to_value(body)?),
            )
            .await)
    }

    /// `felixctl shard move cancel`.
    pub async fn cancel_move(&self, key: &ShardKey) -> Answer {
        self.operator(Method::DELETE, &shard_path("/v1/shard-moves", key), None)
            .await
    }

    /// `felixctl placement abandon --yes`. This loses the shard's records.
    pub async fn abandon(&self, key: &ShardKey) -> Answer {
        self.operator(
            Method::POST,
            &shard_path("/v1/placement/abandon", key),
            None,
        )
        .await
    }

    /// `felixctl node drain`: placement moves the node's shards away, and
    /// replaces the copies it follows with copies elsewhere.
    pub async fn drain(&self, node_id: &str) -> Answer {
        self.operator(Method::POST, &format!("/v1/nodes/{node_id}/drain"), None)
            .await
    }

    /// Cancel a drain: `PATCH /v1/nodes/{id}` back to `live`.
    pub async fn undrain(&self, node_id: &str) -> Result<Answer> {
        let patch = NodePatchRequest {
            region: None,
            labels: None,
            capacity: None,
            lifecycle: Some(NodeLifecycle::Live),
        };
        Ok(self
            .operator(
                Method::PATCH,
                &format!("/v1/nodes/{node_id}"),
                Some(serde_json::to_value(patch)?),
            )
            .await)
    }

    /// `felixctl node deregister`.
    pub async fn deregister(&self, node_id: &str) -> Answer {
        self.operator(
            Method::POST,
            &format!("/v1/nodes/{node_id}/deregister"),
            None,
        )
        .await
    }

    /// `DELETE /v1/nodes/{id}`: remove the node's record. Refused while it
    /// runs or while any shard names it.
    pub async fn remove_node(&self, node_id: &str) -> Answer {
        self.operator(Method::DELETE, &format!("/v1/nodes/{node_id}"), None)
            .await
    }

    /// Assignment changes after `since`, read from the store so a fault on
    /// the control plane does not hide them. Returns them and the next seq.
    pub async fn assignment_changes(&self, since: u64) -> (Vec<ShardAssignmentChange>, u64) {
        match self.store().shard_assignment_changes(since).await {
            Ok(page) => (page.items, page.next_seq),
            Err(_) => (Vec::new(), since),
        }
    }
}

fn cp_key(key: &ShardKey) -> CpShardKey {
    CpShardKey {
        tenant_id: key.tenant_id.clone(),
        namespace: key.namespace.clone(),
        stream: key.stream.clone(),
        shard: key.shard,
        kind: match key.kind {
            ShardKind::Stream => CpShardKind::Stream,
            ShardKind::Cache => CpShardKind::Cache,
        },
    }
}

/// `prefix/{tenant}/{namespace}/{name}/{shard}?kind=...`, as `felixctl`
/// builds it.
fn shard_path(prefix: &str, key: &ShardKey) -> String {
    let kind = match key.kind {
        ShardKind::Stream => "stream",
        ShardKind::Cache => "cache",
    };
    format!(
        "{prefix}/{}/{}/{}/{}?kind={kind}",
        key.tenant_id, key.namespace, key.stream, key.shard
    )
}
