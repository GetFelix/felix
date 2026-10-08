//! `rbac policy` and `rbac grouping` against a real control plane.

use felix_cluster::{Cluster, ClusterConfig};
use serial_test::serial;

use crate::{Env, Run};

const ROLE: &str = "role:felixctl-test";

async fn rbac(env: &Env, cluster: &Cluster, args: &[&str]) -> Run {
    let token = cluster.credentials().rbac_admin_token();
    let mut all = vec!["rbac"];
    all.extend_from_slice(args);
    all.extend_from_slice(&["--controlplane-token", &token]);
    env.felixctl(cluster, &all).await
}

#[tokio::test]
#[serial]
async fn granting_listing_and_revoking() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let env = Env::new(&cluster);
    let key_prefix = format!(
        "cache:{}/{}/sessions/user:*",
        cluster.tenant_id, cluster.namespace
    );

    let run = rbac(
        &env,
        &cluster,
        &["policy", "add", ROLE, &key_prefix, "cache.read"],
    )
    .await
    .ok();
    assert!(run.stdout.contains("added policy"), "{}", run.stdout);
    let run = rbac(
        &env,
        &cluster,
        &["policy", "ls", "--subject", ROLE, "--json"],
    )
    .await
    .ok();
    assert_eq!(
        run.json()["policies"],
        serde_json::json!([{"subject": ROLE, "object": key_prefix, "action": "cache.read"}]),
        "{}",
        run.stdout
    );

    // Refused here, before the request: a key object with a manage action.
    let run = rbac(
        &env,
        &cluster,
        &["policy", "add", ROLE, &key_prefix, "cache.manage"],
    )
    .await;
    assert_eq!(run.code, 2, "{}", run.stderr);
    assert!(
        run.stderr.contains("cache.read or cache.write"),
        "{}",
        run.stderr
    );

    // Refused by the control plane, with its reason.
    let stream = format!("stream:{}/{}/orders", cluster.tenant_id, cluster.namespace);
    let run = rbac(
        &env,
        &cluster,
        &["policy", "add", ROLE, &stream, "stream.fly"],
    )
    .await;
    assert_eq!(run.code, 4, "{}", run.stderr);
    assert!(run.stderr.contains("invalid action"), "{}", run.stderr);

    rbac(&env, &cluster, &["grouping", "add", "p:alice", ROLE])
        .await
        .ok();
    let run = rbac(
        &env,
        &cluster,
        &["grouping", "ls", "--role", ROLE, "--json"],
    )
    .await
    .ok();
    assert_eq!(
        run.json()["groupings"],
        serde_json::json!([{"user": "p:alice", "role": ROLE}]),
        "{}",
        run.stdout
    );

    // stdin is not a terminal, so removal needs --yes.
    let run = rbac(&env, &cluster, &["grouping", "rm", "p:alice", ROLE]).await;
    assert_eq!(run.code, 2, "{}", run.stderr);
    rbac(
        &env,
        &cluster,
        &["grouping", "rm", "p:alice", ROLE, "--yes"],
    )
    .await
    .ok();
    let run = rbac(
        &env,
        &cluster,
        &["grouping", "ls", "--role", ROLE, "--json"],
    )
    .await
    .ok();
    assert_eq!(run.json()["groupings"], serde_json::json!([]));

    rbac(
        &env,
        &cluster,
        &["policy", "rm", ROLE, &key_prefix, "cache.read", "--yes"],
    )
    .await
    .ok();
    let run = rbac(
        &env,
        &cluster,
        &["policy", "rm", ROLE, &key_prefix, "cache.read", "--yes"],
    )
    .await;
    assert_eq!(run.code, 5, "{}", run.stderr);
}
