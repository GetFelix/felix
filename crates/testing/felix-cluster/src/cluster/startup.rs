//! Bringing a cluster up, and deciding when it is usable.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};

use super::{Cluster, READY_TIMEOUT};
use crate::node::{FAILURE_LOG_LINES, broker_binary, spawn_broker};
use crate::proxy::Links;
use crate::{ClusterConfig, ControlPlane, wait};

/// How many times a broker may be started before the cluster gives up on it.
///
/// More than one because port selection is inherently racy: a port is probed,
/// released, and only then handed to the child, and anything on the machine can
/// take it in between. Three attempts make that vanishingly unlikely without
/// masking a broker that is genuinely misconfigured — which fails identically
/// every time and still surfaces, with its log.
pub(super) const MAX_SPAWN_ATTEMPTS: u32 = 3;

impl Cluster {
    /// Start a cluster and wait until every part of it is usable.
    ///
    /// "Usable" means each broker answers `/ready`, the control plane considers
    /// each one placeable, every stream's shards have a leader, and the leader
    /// has opened them. A publish issued the moment this returns is expected to
    /// succeed.
    pub async fn start(config: ClusterConfig) -> Result<Self> {
        if config.nodes == 0 {
            bail!("a cluster needs at least one broker");
        }
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .no_proxy()
            .build()
            .context("build harness HTTP client")?;

        let control_plane = ControlPlane::start(&config.tenant_id).await?;
        let client_token = control_plane.client_token(&config.tenant_id)?;
        let admin_token = control_plane.admin_token(&config.tenant_id)?;
        let operator_token = control_plane.operator_token(&config.tenant_id)?;
        let subscribe_only_token = control_plane.subscribe_only_token(&config.tenant_id)?;
        let group_operator_token = control_plane.group_operator_token(&config.tenant_id)?;

        // Metadata first: a broker syncs streams at startup, and one that starts
        // before its streams exist has to wait for the next sync to become
        // useful. Creating them up front means readiness means what it says.
        seed_metadata(&http, &control_plane, &config, &admin_token).await?;

        let root = tempfile::tempdir().context("create cluster data root")?;
        let binary = broker_binary()?;
        let links = if config.proxy_links {
            Some(Links::start(control_plane.addr()?)?)
        } else {
            None
        };
        let mut nodes = Vec::with_capacity(config.nodes);
        for index in 0..config.nodes {
            nodes.push(
                spawn_broker(
                    &binary,
                    &control_plane,
                    &config,
                    root.path(),
                    index,
                    links.as_ref(),
                )
                .with_context(|| format!("start broker {index}"))?,
            );
        }

        let mut cluster = Self {
            control_plane: Some(control_plane),
            nodes,
            tenant_id: config.tenant_id.clone(),
            namespace: config.namespace.clone(),
            client_token,
            admin_token,
            operator_token,
            subscribe_only_token,
            group_operator_token,
            http,
            binary: binary.clone(),
            config: config.clone(),
            links,
            faults: Default::default(),
            crashed_control_plane: None,
            _root: root,
        };

        // Two phases: the first needs the child handles, to tell "not ready yet"
        // from "already exited"; the rest only observes the cluster.
        let ready = async {
            cluster.await_brokers_ready().await?;
            cluster.await_cluster_ready(&config).await
        }
        .await;
        // The cluster is dropped here without a panic, so its `Drop` prints
        // nothing, and the logs go with the data root.
        if let Err(err) = ready {
            let logs: Vec<String> = cluster
                .nodes
                .iter()
                .map(|node| node.failure_log(FAILURE_LOG_LINES))
                .collect();
            bail!("{err:#}\n--- broker logs ---\n{}", logs.join("\n"));
        }
        Ok(cluster)
    }

    /// Wait for every broker process to report ready.
    async fn await_brokers_ready(&mut self) -> Result<()> {
        // Written as a loop rather than through `wait::until` so a broker that
        // has already exited can be reported as such, with its status, instead
        // of timing out.
        for index in 0..self.nodes.len() {
            let budget = wait::budget(READY_TIMEOUT);
            let deadline = Instant::now() + budget;
            let mut attempts = 1;
            loop {
                // Re-read each pass: a respawn gives this broker new ports, and
                // a URL captured before the loop would keep polling the address
                // the dead process had.
                let url = format!("http://{}/ready", self.nodes[index].metrics_addr);
                if let Some(status) = self.nodes[index].exited() {
                    // Ports are handed to the child after being probed and
                    // released, so another process can take one in between.
                    // Losing that race is not a cluster failure, it is a retry
                    // with different ports.
                    if attempts < MAX_SPAWN_ATTEMPTS {
                        attempts += 1;
                        self.respawn(index)?;
                        continue;
                    }
                    let node_id = &self.nodes[index].node_id;
                    let reason = self.nodes[index].failure_reason();
                    bail!(
                        "{node_id} exited before becoming ready after \
                         {MAX_SPAWN_ATTEMPTS} attempts ({status}){reason}"
                    );
                }
                let ok = matches!(
                    self.http.get(&url).send().await,
                    Ok(response) if response.status().is_success()
                );
                if ok {
                    break;
                }
                if Instant::now() >= deadline {
                    let node_id = &self.nodes[index].node_id;
                    bail!("timed out after {budget:?} waiting for {node_id} to be ready");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        Ok(())
    }

    /// Start one broker again, with fresh ports.
    ///
    /// Keeps the node id and data directory: this is the same broker having
    /// another go, not a different one, and a durable log it already wrote must
    /// still be there.
    fn respawn(&mut self, index: usize) -> Result<()> {
        let node_id = self.nodes[index].node_id.clone();
        tracing::warn!(%node_id, "broker exited during start-up; starting it again");
        let control_plane = self
            .control_plane
            .as_ref()
            .ok_or_else(|| anyhow!("control plane is gone"))?;
        let replacement = spawn_broker(
            &self.binary,
            control_plane,
            &self.config,
            self._root.path(),
            index,
            self.links.as_ref(),
        )?;
        self.nodes[index] = replacement;
        Ok(())
    }

    /// Wait until the cluster as a whole can serve: every broker registered and
    /// placeable, every shard led, and a publish accepted.
    async fn await_cluster_ready(&self, config: &ClusterConfig) -> Result<()> {
        let expected: Vec<String> = self.nodes.iter().map(|n| n.node_id.clone()).collect();
        wait::until(
            READY_TIMEOUT,
            "every broker registered and placeable",
            || {
                let this = self;
                let expected = expected.clone();
                async move {
                    match this.placeable_nodes().await {
                        Ok(live) => expected.iter().all(|id| live.contains(id)),
                        Err(_) => false,
                    }
                }
            },
        )
        .await?;

        // Placement is stepped rather than waited on, then the result is
        // verified: a leader for every shard of every stream.
        let total_shards: usize = config
            .streams
            .iter()
            .map(|spec| spec.shards as usize)
            .chain(config.caches.iter().map(|spec| spec.shards as usize))
            .sum();
        wait::until(
            READY_TIMEOUT,
            "every shard assigned a leader",
            || async move {
                self.control_plane().place_shards().await;
                match self.shard_owners().await {
                    Ok(owners) => owners.len() >= total_shards,
                    Err(_) => false,
                }
            },
        )
        .await?;

        // An assignment is not the same as a broker that has opened the shard.
        // That gap is exactly where a publish is refused as `NotReady`, and no
        // control-plane state distinguishes the two — so the only honest check
        // is a publish that succeeds.
        //
        // Placement is stepped inside the wait, not only before it. Brokers
        // register as they start, so a pass that ran while only some of them
        // had can leave a node over its share, and the next pass moves shards
        // off it. A move is several passes with a catch-up between them, and
        // the shard does not serve between its fence and its cut-over — so a
        // probe loop that did not step placement would wait out a move that
        // nothing was advancing.
        for spec in &config.streams {
            let stream = &spec.name;
            let stream = stream.clone();
            wait::until(
                READY_TIMEOUT,
                &format!("a publish to {stream} to be accepted"),
                || {
                    let stream = stream.clone();
                    async move {
                        self.control_plane().place_shards().await;
                        self.probe_publish(&stream).await.is_ok()
                    }
                },
            )
            .await?;
        }

        // The probe above is unkeyed, so it lands on shard 0 the instant shard 0
        // opens and says nothing about whether a broker knows how *wide* a
        // stream is. That matters because a keyed publish resolves its shard
        // from the width the broker's routing snapshot reports, and that width
        // is inferred from the assignments it has applied so far — a broker
        // holding only shard 0's assignment reports width 1 and routes *every*
        // key to shard 0. The publish succeeds, so nothing retries and nothing
        // reports an error; the records are simply all in one shard.
        //
        // So "usable" has to include "every broker routes keys at the stream's
        // full width". `stream_shards` reads the very same snapshot the publish
        // path does, which makes it the only honest check.
        for spec in &config.streams {
            let stream = spec.name.clone();
            let expected = spec.shards;
            wait::until(
                READY_TIMEOUT,
                &format!("every broker to route {stream} at {expected} shards"),
                || {
                    let stream = stream.clone();
                    async move {
                        self.control_plane().place_shards().await;
                        for node in &self.nodes {
                            if !node.is_running() {
                                continue;
                            }
                            match self.stream_shards_via(&node.node_id, &stream).await {
                                Ok(width) if width == expected => {}
                                _ => return false,
                            }
                        }
                        true
                    }
                },
            )
            .await?;
        }
        // Nothing mid-move. A cluster that came up staggered rebalances, and
        // a test that began publishing into the middle of that would be
        // racing it rather than testing what it came for.
        wait::until(READY_TIMEOUT, "placement to settle", || async {
            let outcome = self.control_plane().place_shards().await;
            outcome.moved == 0 && outcome.waiting == 0
        })
        .await?;
        // Settled is not failover-ready. A shard that has just opened, often
        // after fencing its replicas, has not reported yet, and a leader killed
        // before its first report leaves a shard placement will never promote.
        // The probe above only proves shard 0 serves.
        wait::until(
            READY_TIMEOUT,
            "every replicated shard's leader to report its replicas",
            || async {
                self.control_plane()
                    .unreported_shards()
                    .await
                    .is_ok_and(|unreported| unreported.is_empty())
            },
        )
        .await?;
        Ok(())
    }

    /// Publish one record through an arbitrary broker.
    ///
    /// Deliberately not through the owner: routing a publish from a non-owner is
    /// the thing that has to work, so the readiness check exercises it rather
    /// than the easy path.
    async fn probe_publish(&self, stream: &str) -> Result<()> {
        let node = self
            .nodes
            .iter()
            .find(|node| node.is_running())
            .ok_or_else(|| anyhow!("no running broker"))?;
        self.publish_via(&node.node_id, stream, b"harness-probe".to_vec())
            .await
    }
}

/// Create the tenant, namespace, and streams the cluster serves.
async fn seed_metadata(
    http: &reqwest::Client,
    control_plane: &ControlPlane,
    config: &ClusterConfig,
    token: &str,
) -> Result<()> {
    let base = &control_plane.base_url;
    // The tenant itself was seeded with the control plane: see
    // `ControlPlane::seed_tenant` for why it does not go through the API.
    control_plane.seed_tenant(&config.tenant_id).await?;

    post(
        http,
        &format!("{base}/v1/tenants/{}/namespaces", config.tenant_id),
        token,
        serde_json::json!({
            "tenant_id": config.tenant_id,
            "namespace": config.namespace,
            "display_name": config.namespace,
        }),
    )
    .await
    .context("create namespace")?;

    for spec in &config.streams {
        let stream = &spec.name;
        post(
            http,
            &format!(
                "{base}/v1/tenants/{}/namespaces/{}/streams",
                config.tenant_id, config.namespace
            ),
            token,
            serde_json::json!({
                "tenant_id": config.tenant_id,
                "namespace": config.namespace,
                "stream": stream,
                "kind": "Stream",
                "shards": spec.shards,
                "replication_factor": spec.replication_factor,
                "retention": { "max_age_seconds": null, "max_size_bytes": null },
                "consistency": spec.consistency,
                "region": spec.region,
                "delivery": "AtLeastOnce",
                // Durable, so the brokers gate readiness on having actually
                // recovered their logs. An ephemeral stream would let a broker
                // report ready before it could serve anything.
                "durable": true,
            }),
        )
        .await
        .with_context(|| format!("create stream {stream}"))?;
    }

    for spec in &config.caches {
        let cache = &spec.name;
        post(
            http,
            &format!(
                "{base}/v1/tenants/{}/namespaces/{}/caches",
                config.tenant_id, config.namespace
            ),
            token,
            serde_json::json!({
                "cache": cache,
                "display_name": cache,
                "shards": spec.shards,
                "replication_factor": spec.replication_factor,
                "consistency": spec.consistency,
            }),
        )
        .await
        .with_context(|| format!("create cache {cache}"))?;
    }
    Ok(())
}

async fn post(
    http: &reqwest::Client,
    url: &str,
    token: &str,
    body: serde_json::Value,
) -> Result<()> {
    let response = http
        .post(url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    let status = response.status();
    // Idempotent by design: a harness restarted against a warm control plane
    // should not fail because the tenant already exists.
    if status.is_success() || status == reqwest::StatusCode::CONFLICT {
        return Ok(());
    }
    let detail = response.text().await.unwrap_or_default();
    bail!("POST {url}: {status}: {detail}")
}
