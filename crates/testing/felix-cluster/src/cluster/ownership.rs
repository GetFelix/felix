//! Who leads what, as the control plane records it.

use std::collections::HashMap;

use anyhow::{Context, Result, anyhow, bail};

use super::Cluster;

impl Cluster {
    /// Which broker leads each shard, keyed by
    /// `kind/tenant/namespace/name/shard` where `kind` is `stream` or `cache`.
    pub async fn shard_owners(&self) -> Result<HashMap<String, String>> {
        // `ShardAssignment` flattens its key, so the JSON is flat too.
        #[derive(serde::Deserialize)]
        struct Assignment {
            tenant_id: String,
            namespace: String,
            stream: String,
            shard: u32,
            leader: String,
            /// Absent on an assignment written before caches were placed.
            #[serde(default)]
            kind: Option<String>,
        }

        let items: Vec<Assignment> = self
            .get_all(&format!(
                "{}/v1/shard-assignments",
                self.control_plane_url()
            ))
            .await?;
        Ok(items
            .into_iter()
            .map(|a| {
                // The kind leads the key: a cache and a stream may share a name,
                // and collapsing the two here would undercount the shards the
                // readiness gate is waiting for.
                let kind = a.kind.as_deref().unwrap_or("stream");
                (
                    format!(
                        "{kind}/{}/{}/{}/{}",
                        a.tenant_id, a.namespace, a.stream, a.shard
                    ),
                    a.leader,
                )
            })
            .collect())
    }

    /// Replicated shards that could not fail over yet, because their leader has
    /// not reported a caught-up replica at the current generation. See
    /// [`ControlPlane::unreported_shards`](crate::ControlPlane::unreported_shards).
    pub async fn unreported_shards(&self) -> Result<Vec<String>> {
        self.control_plane().unreported_shards().await
    }

    /// The last replica report the control plane holds for shard `shard` of
    /// `stream`: the leader's tail and how far each replica had got. Failover
    /// promotes the replica furthest ahead in it.
    pub async fn replica_report(
        &self,
        stream: &str,
        shard: u32,
    ) -> Result<Option<felix_controlplane_service::model::ReplicaReport>> {
        self.control_plane()
            .replica_report(&self.tenant_id, &self.namespace, stream, shard)
            .await
    }

    /// [`Self::replica_report`] for shard `shard` of cache `cache`.
    pub async fn cache_replica_report(
        &self,
        cache: &str,
        shard: u32,
    ) -> Result<Option<felix_controlplane_service::model::ReplicaReport>> {
        self.control_plane()
            .replica_report_of(
                felix_controlplane_service::model::ShardKind::Cache,
                &self.tenant_id,
                &self.namespace,
                cache,
                shard,
            )
            .await
    }

    /// Who owns each shard of `stream`, by shard index.
    ///
    /// The point of a multi-shard test: a stream placed across brokers has
    /// several owners, and which shard a key lands on decides which of them a
    /// publish reaches.
    pub async fn shard_owners_for(&self, stream: &str) -> Result<HashMap<u32, String>> {
        let owners = self.shard_owners().await?;
        let prefix = format!("stream/{}/{}/{}/", self.tenant_id, self.namespace, stream);
        Ok(owners
            .into_iter()
            .filter_map(|(key, node)| {
                let shard = key.strip_prefix(&prefix)?.parse().ok()?;
                Some((shard, node))
            })
            .collect())
    }

    /// Every shard of one cache, mapped to the node that leads it.
    pub async fn cache_shard_owners(&self, cache: &str) -> Result<HashMap<u32, String>> {
        let owners = self.shard_owners().await?;
        let prefix = format!("cache/{}/{}/{cache}/", self.tenant_id, self.namespace);
        Ok(owners
            .into_iter()
            .filter_map(|(key, node)| {
                let shard = key.strip_prefix(&prefix)?.parse().ok()?;
                Some((shard, node))
            })
            .collect())
    }

    /// The node leading one shard of one stream or cache.
    pub async fn shard_owner_of(&self, kind: &str, name: &str, shard: u32) -> Result<String> {
        let owners = self.shard_owners().await?;
        let key = format!(
            "{kind}/{}/{}/{name}/{shard}",
            self.tenant_id, self.namespace
        );
        owners
            .get(&key)
            .cloned()
            .ok_or_else(|| anyhow!("no owner for {key}"))
    }

    /// The node that owns `stream`'s shard 0.
    pub async fn owner(&self, stream: &str) -> Result<String> {
        self.shard_owner_of("stream", stream, 0).await
    }

    /// The node that owns `stream`'s shard 0, and one that does not.
    ///
    /// This pair is the whole point of a multi-node harness: it is what lets a
    /// test publish somewhere the data does not belong.
    pub async fn owner_and_non_owner(&self, stream: &str) -> Result<(String, String)> {
        let owner = self.owner(stream).await?;
        let other = self
            .nodes
            .iter()
            .find(|node| node.node_id != owner && node.is_running())
            .ok_or_else(|| anyhow!("no running broker other than the owner {owner}"))?
            .node_id
            .clone();
        Ok((owner, other))
    }

    /// Full assignment detail: leader and generation, keyed by
    /// `tenant/namespace/stream/shard`.
    ///
    /// The generation is what a cross-broker failure needs in its diagnosis: a
    /// publish refused as stale and one refused because the shard moved look the
    /// same without it.
    pub async fn shard_assignments(&self) -> Result<HashMap<String, Assignment>> {
        // `ShardAssignment` flattens its key, so the JSON is flat too.
        #[derive(serde::Deserialize)]
        struct Row {
            tenant_id: String,
            namespace: String,
            stream: String,
            shard: u32,
            leader: String,
            generation: u64,
            #[serde(default)]
            replicas: Vec<String>,
        }

        let items: Vec<Row> = self
            .get_all(&format!(
                "{}/v1/shard-assignments",
                self.control_plane_url()
            ))
            .await?;
        Ok(items
            .into_iter()
            .map(|row| {
                (
                    format!(
                        "{}/{}/{}/{}",
                        row.tenant_id, row.namespace, row.stream, row.shard
                    ),
                    Assignment {
                        leader: row.leader,
                        generation: row.generation,
                        replicas: row.replicas,
                    },
                )
            })
            .collect())
    }

    /// The successor staged for each shard, keyed like [`Cluster::shard_owners`].
    /// `None` for a shard with no move in progress.
    pub async fn shard_successors(&self) -> Result<HashMap<String, Option<String>>> {
        #[derive(serde::Deserialize)]
        struct Row {
            tenant_id: String,
            namespace: String,
            stream: String,
            shard: u32,
            #[serde(default)]
            kind: Option<String>,
            #[serde(default)]
            successor: Option<String>,
        }
        let items: Vec<Row> = self
            .get_all(&format!(
                "{}/v1/shard-assignments",
                self.control_plane_url()
            ))
            .await?;
        Ok(items
            .into_iter()
            .map(|row| {
                let kind = row.kind.as_deref().unwrap_or("stream");
                (
                    format!(
                        "{kind}/{}/{}/{}/{}",
                        row.tenant_id, row.namespace, row.stream, row.shard
                    ),
                    row.successor,
                )
            })
            .collect())
    }

    /// Whether a stream's shard is fenced mid-move: its assignment is
    /// `draining`, so the leader has stopped serving and no successor leads yet.
    pub async fn shard_fenced(&self, stream: &str, shard: u32) -> Result<bool> {
        #[derive(serde::Deserialize)]
        struct Row {
            tenant_id: String,
            namespace: String,
            stream: String,
            shard: u32,
            #[serde(default)]
            kind: Option<String>,
            #[serde(default)]
            state: Option<String>,
        }
        let items: Vec<Row> = self
            .get_all(&format!(
                "{}/v1/shard-assignments",
                self.control_plane_url()
            ))
            .await?;
        Ok(items.iter().any(|row| {
            row.kind.as_deref().unwrap_or("stream") == "stream"
                && row.tenant_id == self.tenant_id
                && row.namespace == self.namespace
                && row.stream == stream
                && row.shard == shard
                && row.state.as_deref() == Some("draining")
        }))
    }

    /// Node ids the control plane currently considers placeable.
    pub async fn placeable_nodes(&self) -> Result<Vec<String>> {
        #[derive(serde::Deserialize)]
        struct Item {
            node: Node,
            placement: Placement,
        }
        #[derive(serde::Deserialize)]
        struct Node {
            node_id: String,
        }
        #[derive(serde::Deserialize)]
        struct Placement {
            eligible: bool,
        }

        let items: Vec<Item> = self
            .get_all(&format!("{}/v1/nodes", self.control_plane_url()))
            .await?;
        Ok(items
            .into_iter()
            .filter(|item| item.placement.eligible)
            .map(|item| item.node.node_id)
            .collect())
    }

    /// Every item of a paged control-plane listing, following `next_cursor`.
    async fn get_all<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<Vec<T>> {
        #[derive(serde::Deserialize)]
        struct Page<T> {
            items: Vec<T>,
            #[serde(default)]
            next_cursor: Option<String>,
        }
        let mut items = Vec::new();
        let mut next = url.to_string();
        loop {
            let page: Page<T> = self.get(&next).await?;
            items.extend(page.items);
            match page.next_cursor {
                Some(cursor) => next = format!("{url}?cursor={cursor}"),
                None => return Ok(items),
            }
        }
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T> {
        let response = self
            .http
            .get(url)
            .bearer_auth(&self.admin_token)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("GET {url}: {status}: {body}");
        }
        response.json().await.context("decode response")
    }
}

/// Who leads a shard, at which generation, and who follows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    pub leader: String,
    pub generation: u64,
    pub replicas: Vec<String>,
}
