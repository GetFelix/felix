//! The control-plane commands, run in process the way `main` runs them,
//! against the cluster harness's control plane with no brokers. Each check
//! reads the result back through the REST API rather than trusting what the
//! command printed.

use clap::Parser;
use felix_cluster::controlplane::ControlPlane;
use serde_json::Value;

use crate::cli::Cli;
use crate::error::exit_for;
use crate::output::Output;

const TENANT: &str = "t1";

/// A control plane holding [`TENANT`], and an empty config file so no
/// profile on the machine running the tests is read.
struct Harness {
    control_plane: ControlPlane,
    admin: String,
    cluster_admin: String,
    operator: String,
    dir: tempfile::TempDir,
}

impl Harness {
    async fn start() -> Self {
        let control_plane = ControlPlane::start(TENANT).await.expect("control plane");
        let credentials = control_plane.credentials(TENANT);
        Self {
            admin: credentials.admin_token(),
            cluster_admin: credentials.cluster_admin_token(),
            operator: credentials.operator_token(),
            control_plane,
            dir: tempfile::tempdir().expect("tempdir"),
        }
    }

    /// Run `felixctl args` as `token`; `Ok` or the exit status it would end
    /// with.
    async fn run_as(&self, token: &str, args: &[&str]) -> Result<(), u8> {
        let config = self.dir.path().join("config.toml");
        let mut argv = vec![
            "felixctl".to_string(),
            "--config".into(),
            config.display().to_string(),
            "--tenant".into(),
            TENANT.into(),
            "--controlplane-url".into(),
            self.control_plane.base_url.clone(),
            "--controlplane-token".into(),
            token.into(),
        ];
        argv.extend(args.iter().map(|arg| arg.to_string()));
        let cli = Cli::try_parse_from(argv).unwrap_or_else(|err| panic!("{args:?}: {err}"));
        let out = Output { json: cli.json };
        crate::run(cli, &out)
            .await
            .map_err(|err| exit_for(&err).code())
    }

    async fn run(&self, args: &[&str]) -> Result<(), u8> {
        self.run_as(&self.admin, args).await
    }

    /// GET `path` as the cluster admin, `None` for a 404.
    async fn get(&self, path: &str) -> Option<Value> {
        self.get_as(&self.cluster_admin, path).await
    }

    /// GET `path` as `token`, `None` for a 404.
    async fn get_as(&self, token: &str, path: &str) -> Option<Value> {
        let response = reqwest::Client::new()
            .get(format!("{}{path}", self.control_plane.base_url))
            .bearer_auth(token)
            .send()
            .await
            .expect("GET");
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return None;
        }
        assert!(
            response.status().is_success(),
            "{path}: {}",
            response.status()
        );
        Some(response.json().await.expect("JSON"))
    }

    /// The item of the listing at `path` whose `field` is `id`.
    async fn listed(&self, path: &str, field: &str, id: &str) -> Option<Value> {
        let page = self.get(path).await.expect("listing");
        let (items, _) = super::page_parts(page).expect("a page");
        items.into_iter().find(|item| item[field] == id)
    }
}

/// **Namespaces, streams and caches are created, changed and deleted as the
/// commands say**, a second create is refused, a delete off a terminal needs
/// `--yes`, and a missing one is exit 5.
#[tokio::test]
async fn the_namespace_stream_and_cache_lifecycle() {
    let h = Harness::start().await;
    let ns = "/v1/tenants/t1/namespaces/scratch";
    let namespace = || h.listed("/v1/tenants/t1/namespaces", "namespace", "scratch");

    h.run(&["namespace", "create", "scratch"])
        .await
        .expect("create");
    assert!(namespace().await.is_some());
    assert_eq!(
        h.run(&["namespace", "create", "scratch"]).await,
        Err(4),
        "a second create is refused"
    );
    h.run(&["namespace", "ls"]).await.expect("ls");
    h.run(&["namespace", "ls", "--json"])
        .await
        .expect("ls json");
    h.run(&["namespace", "info", "scratch"])
        .await
        .expect("info");
    assert_eq!(h.run(&["namespace", "info", "missing"]).await, Err(5));

    let with = |args: &[&'static str]| {
        let mut all = vec!["-n", "scratch"];
        all.extend_from_slice(args);
        all
    };
    h.run(&with(&["stream", "create", "events", "--shards", "2"]))
        .await
        .expect("stream create");
    let stream = h
        .get(&format!("{ns}/streams/events"))
        .await
        .expect("stream");
    assert_eq!(stream["shards"], 2);
    h.run(&with(&["stream", "create", "events", "--shards", "2"]))
        .await
        .expect("the same create again finds it there");
    assert_eq!(
        h.run(&with(&["stream", "create", "events", "--shards", "3"]))
            .await,
        Err(4),
        "a create that differs is refused"
    );
    h.run(&with(&[
        "stream",
        "set",
        "events",
        "--retention-secs",
        "3600",
    ]))
    .await
    .expect("set");
    let stream = h
        .get(&format!("{ns}/streams/events"))
        .await
        .expect("stream");
    assert_eq!(stream["retention"]["max_age_seconds"], 3600);
    h.run(&with(&["stream", "ls"])).await.expect("stream ls");
    h.run(&with(&["stream", "info", "events", "--json"]))
        .await
        .expect("stream info");
    assert_eq!(h.run(&with(&["stream", "info", "nope"])).await, Err(5));

    h.run(&with(&["cache", "create", "kv", "--shards", "2"]))
        .await
        .expect("cache create");
    h.run(&with(&[
        "cache",
        "set",
        "kv",
        "--display-name",
        "Key value",
    ]))
    .await
    .expect("cache set");
    let cache = h.get(&format!("{ns}/caches/kv")).await.expect("cache");
    assert_eq!(cache["display_name"], "Key value");
    assert_eq!(cache["shards"], 2);
    h.run(&with(&["cache", "ls"])).await.expect("cache ls");
    h.run(&with(&["cache", "info", "kv"]))
        .await
        .expect("cache info");
    assert_eq!(
        h.run(&with(&["cache", "rm", "kv"])).await,
        Err(2),
        "no --yes off a terminal"
    );
    assert!(h.get(&format!("{ns}/caches/kv")).await.is_some());
    h.run(&with(&["cache", "rm", "kv", "--yes"]))
        .await
        .expect("cache rm");
    assert!(h.get(&format!("{ns}/caches/kv")).await.is_none());
    assert_eq!(h.run(&with(&["cache", "info", "kv"])).await, Err(5));

    h.run(&with(&["stream", "rm", "events", "--yes", "--json"]))
        .await
        .expect("stream rm");
    assert!(h.get(&format!("{ns}/streams/events")).await.is_none());

    h.run(&["namespace", "rm", "scratch", "--yes"])
        .await
        .expect("namespace rm");
    assert!(namespace().await.is_none());
    h.control_plane.shutdown().await;
}

/// **Tenants need cluster-wide rights**: the tenant admin is refused (exit
/// 4), the cluster admin creates, lists and deletes one.
#[tokio::test]
async fn tenants_are_managed_by_the_cluster_admin_only() {
    let h = Harness::start().await;
    assert_eq!(h.run(&["tenant", "ls"]).await, Err(4));
    assert_eq!(h.run(&["tenant", "create", "acme"]).await, Err(4));

    let admin = h.cluster_admin.clone();
    h.run_as(
        &admin,
        &["tenant", "create", "acme", "--display-name", "Acme"],
    )
    .await
    .expect("create");
    let acme = || h.listed("/v1/tenants", "tenant_id", "acme");
    assert_eq!(acme().await.expect("tenant")["display_name"], "Acme");
    h.run_as(&admin, &["tenant", "ls"]).await.expect("ls");
    h.run_as(&admin, &["tenant", "ls", "--json"])
        .await
        .expect("ls json");
    h.run_as(&admin, &["tenant", "info", "acme"])
        .await
        .expect("info");
    assert_eq!(h.run_as(&admin, &["tenant", "info", "nope"]).await, Err(5));
    assert_eq!(
        h.run_as(&admin, &["tenant", "rm", "acme"]).await,
        Err(2),
        "no --yes off a terminal"
    );
    h.run_as(&admin, &["tenant", "rm", "acme", "--yes"])
        .await
        .expect("rm");
    assert!(acme().await.is_none());
    h.control_plane.shutdown().await;
}

/// **With no brokers, listings are empty rather than errors, and a node or
/// shard that is not there is exit 5.**
#[tokio::test]
async fn nodes_and_shards_without_brokers() {
    let h = Harness::start().await;
    let operator = h.operator.clone();
    h.run_as(&operator, &["node", "ls"]).await.expect("node ls");
    h.run_as(&operator, &["node", "ls", "--json"])
        .await
        .expect("node ls json");
    assert_eq!(
        h.run_as(&operator, &["node", "info", "no-such-node"]).await,
        Err(5)
    );
    assert_eq!(
        h.run_as(&operator, &["node", "drain", "no-such-node", "--yes"])
            .await,
        Err(5)
    );
    h.run_as(&operator, &["shard", "ls"])
        .await
        .expect("shard ls");
    h.run_as(&operator, &["shard", "ls", "--leader", "nobody", "--json"])
        .await
        .expect("shard ls by leader");
    h.control_plane.shutdown().await;
}

/// **Placement pauses and resumes, its read views answer with nothing
/// placed, and abandoning a shard's log is never done without `--yes`.**
#[tokio::test]
async fn placement_commands_without_brokers() {
    let h = Harness::start().await;
    let operator = h.operator.clone();
    h.run_as(&operator, &["placement", "pause"])
        .await
        .expect("pause");
    h.run_as(&operator, &["placement", "plan"])
        .await
        .expect("plan");
    h.run_as(&operator, &["placement", "replication"])
        .await
        .expect("replication");
    h.run_as(&operator, &["placement", "resume", "--json"])
        .await
        .expect("resume");
    assert_eq!(
        h.run_as(&operator, &["placement", "abandon", "orders", "0"])
            .await,
        Err(2)
    );
    assert!(
        h.run_as(&operator, &["placement", "abandon", "orders", "0", "--yes"])
            .await
            .is_err(),
        "no such shard to abandon"
    );
    h.control_plane.shutdown().await;
}

/// **RBAC rules and role assignments are added, listed and removed**, a
/// rule naming another tenant is refused before it is sent, and a removal
/// off a terminal needs `--yes`.
#[tokio::test]
async fn rbac_policies_and_groupings_round_trip() {
    let h = Harness::start().await;
    let rbac = h.control_plane.credentials(TENANT).rbac_admin_token();
    let policy = [
        "role:reader",
        "stream:t1/payments/orders",
        "stream.subscribe",
    ];
    let policies = "/v1/tenants/t1/rbac/policies";
    let has_policy = || async {
        let page = h
            .get_as(&rbac, &format!("{policies}?limit=1000"))
            .await
            .expect("policies");
        let (items, _) = super::page_parts(page).expect("a page");
        items.iter().any(|p| {
            p["subject"] == policy[0] && p["object"] == policy[1] && p["action"] == policy[2]
        })
    };

    let mut add = vec!["rbac", "policy", "add"];
    add.extend_from_slice(&policy);
    h.run_as(&rbac, &add).await.expect("policy add");
    assert!(has_policy().await);
    h.run_as(&rbac, &["rbac", "policy", "ls"])
        .await
        .expect("ls");
    h.run_as(
        &rbac,
        &["rbac", "policy", "ls", "--subject", "role:reader", "--json"],
    )
    .await
    .expect("ls by subject");
    assert_eq!(
        h.run_as(
            &rbac,
            &[
                "rbac",
                "policy",
                "add",
                "role:x",
                "stream:t2/payments/orders",
                "stream.subscribe"
            ]
        )
        .await,
        Err(2),
        "another tenant's object is refused"
    );
    let groupings = "/v1/tenants/t1/rbac/groupings";
    let has_grouping = || async {
        let page = h
            .get_as(&rbac, &format!("{groupings}?limit=1000"))
            .await
            .expect("groupings");
        let (items, _) = super::page_parts(page).expect("a page");
        items
            .iter()
            .any(|g| g["user"] == "p:alice" && g["role"] == "role:reader")
    };
    h.run_as(
        &rbac,
        &["rbac", "grouping", "add", "p:alice", "role:reader"],
    )
    .await
    .expect("grouping add");
    assert!(has_grouping().await);
    h.run_as(&rbac, &["rbac", "grouping", "ls", "--role", "role:reader"])
        .await
        .expect("grouping ls");
    h.run_as(
        &rbac,
        &["rbac", "grouping", "ls", "--user", "p:alice", "--json"],
    )
    .await
    .expect("grouping ls json");
    h.run_as(
        &rbac,
        &["rbac", "grouping", "rm", "p:alice", "role:reader", "--yes"],
    )
    .await
    .expect("grouping rm");
    assert!(!has_grouping().await);

    // A role is removed with its rules once nobody holds it.
    let mut rm = vec!["rbac", "policy", "rm"];
    rm.extend_from_slice(&policy);
    assert_eq!(
        h.run_as(&rbac, &rm).await,
        Err(2),
        "no --yes off a terminal"
    );
    assert!(has_policy().await);
    rm.push("--yes");
    h.run_as(&rbac, &rm).await.expect("policy rm");
    assert!(!has_policy().await);
    h.control_plane.shutdown().await;
}

/// **The audit log needs cluster-wide rights**: a tenant admin is refused
/// with exit 4, not shown an empty log. Features list, and a finalize off a
/// terminal needs `--yes` before anything is sent.
#[tokio::test]
async fn audit_refuses_a_tenant_admin_and_features_list() {
    let h = Harness::start().await;
    assert_eq!(h.run(&["audit", "ls"]).await, Err(4));
    assert_eq!(
        h.run(&["audit", "ls", "--limit", "1", "--json"]).await,
        Err(4)
    );
    h.run(&["features", "ls"]).await.expect("features");
    h.run(&["features", "ls", "--json"])
        .await
        .expect("features json");
    let dry_run = h
        .run(&["features", "finalize", "no_such_feature", "--dry-run"])
        .await;
    assert_eq!(dry_run, Err(4), "an unknown feature is refused");
    assert_eq!(
        h.run(&["features", "finalize", "no_such_feature"]).await,
        Err(2),
        "no --yes off a terminal"
    );
    h.control_plane.shutdown().await;
}

/// **A registered node is listed, drained and deregistered by name**, each
/// change visible in its record, and each needs `--yes` off a terminal. Its
/// data directory is declared dead only once it has left.
#[tokio::test]
async fn a_node_is_drained_and_deregistered() {
    let h = Harness::start().await;
    let node = serde_json::from_value(serde_json::json!({
        "node_id": "broker-x",
        "spec": {
            "advertise_addr": "127.0.0.1:9",
            "client_addr": "127.0.0.1:10",
            "region": "local",
        },
        "status": {
            "lifecycle": "live",
            "last_heartbeat_at_millis": 1,
            "registered_at_millis": 1,
            "storage_id": "0123456789abcdef0123456789abcdef",
        },
    }))
    .expect("a node");
    h.control_plane
        .store
        .register_node(node)
        .await
        .expect("register");
    let lifecycle = || async {
        let node = h.get("/v1/nodes/broker-x").await.expect("node");
        node["status"]["lifecycle"]
            .as_str()
            .or_else(|| node["node"]["status"]["lifecycle"].as_str())
            .map(str::to_string)
    };
    let operator = h.operator.clone();
    h.run_as(&operator, &["node", "ls"]).await.expect("ls");
    h.run_as(&operator, &["node", "info", "broker-x"])
        .await
        .expect("info");
    h.run_as(&operator, &["node", "info", "broker-x", "--json"])
        .await
        .expect("info json");

    assert_eq!(
        h.run_as(&operator, &["node", "drain", "broker-x"]).await,
        Err(2)
    );
    assert_eq!(lifecycle().await.as_deref(), Some("live"));
    h.run_as(&operator, &["node", "drain", "broker-x", "--yes"])
        .await
        .expect("drain");
    assert_eq!(lifecycle().await.as_deref(), Some("draining"));

    assert_eq!(
        h.run_as(&operator, &["node", "declare-dead", "broker-x"])
            .await,
        Err(2)
    );
    assert_eq!(
        h.run_as(&operator, &["node", "declare-dead", "broker-x", "--yes"])
            .await,
        Err(4),
        "refused while the node is still a member"
    );

    h.run_as(
        &operator,
        &["node", "deregister", "broker-x", "--yes", "--json"],
    )
    .await
    .expect("deregister");
    assert_eq!(lifecycle().await.as_deref(), Some("left"));
    h.run_as(&operator, &["node", "declare-dead", "broker-x", "--yes"])
        .await
        .expect("declared dead once it has left");
    h.control_plane.shutdown().await;
}

/// **A re-form plan with no Raft groups finishes its survey with nothing to
/// re-form**, and applying a plan that does not exist is refused.
#[tokio::test]
async fn a_reform_plan_with_nothing_to_reform() {
    let h = Harness::start().await;
    let operator = h.operator.clone();
    let planned = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        h.run_as(&operator, &["raft", "reform", "plan"]),
    )
    .await
    .expect("the survey finishes");
    assert_eq!(planned, Ok(()));
    let planned = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        h.run_as(&operator, &["raft", "reform", "plan", "--json"]),
    )
    .await
    .expect("the survey finishes");
    assert_eq!(planned, Ok(()));
    let applied = h
        .run_as(&operator, &["raft", "reform", "apply", "no-such-plan"])
        .await;
    assert_eq!(applied, Err(4), "no such plan");
    h.control_plane.shutdown().await;
}
