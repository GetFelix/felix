//! The control-plane commands: listings, and the writes.

use felix_cluster::{CacheSpec, Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

use crate::{Env, Run};

#[tokio::test]
#[serial]
async fn listing_and_inspecting_the_control_plane() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new("orders", 2)],
        caches: vec![CacheSpec::new("sessions", 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let env = Env::new(&cluster);
    let tenant = cluster.tenant_id.clone();

    // Listing tenants needs cluster-wide tenant.manage, which the admin token
    // lacks: the refusal is a server error.
    let run = env.felixctl(&cluster, &["tenant", "ls"]).await;
    assert_eq!(run.code, 4, "{}", run.stderr);

    let cluster_admin = cluster.credentials().cluster_admin_token();
    let run = env
        .felixctl(
            &cluster,
            &[
                "tenant",
                "ls",
                "--json",
                "--controlplane-token",
                &cluster_admin,
            ],
        )
        .await
        .ok();
    let tenants = run.json()["tenants"].as_array().unwrap().clone();
    assert!(
        tenants.iter().any(|t| t["tenant_id"] == tenant.as_str()),
        "{}",
        run.stdout
    );
    let run = env
        .felixctl(
            &cluster,
            &[
                "tenant",
                "info",
                &tenant,
                "--controlplane-token",
                &cluster_admin,
            ],
        )
        .await
        .ok();
    assert!(run.stdout.contains(&tenant), "{}", run.stdout);
    let run = env
        .felixctl(
            &cluster,
            &[
                "tenant",
                "info",
                "nope",
                "--controlplane-token",
                &cluster_admin,
            ],
        )
        .await;
    assert_eq!(run.code, 5, "{}", run.stderr);

    let run = env
        .felixctl(&cluster, &["namespace", "ls", "--json"])
        .await
        .ok();
    assert!(
        run.json()["namespaces"]
            .as_array()
            .unwrap()
            .iter()
            .any(|ns| ns["namespace"] == cluster.namespace.as_str()),
        "{}",
        run.stdout
    );
    env.felixctl(&cluster, &["namespace", "info", &cluster.namespace])
        .await
        .ok();

    let run = env.felixctl(&cluster, &["stream", "ls"]).await.ok();
    assert!(run.stdout.starts_with("STREAM"), "{}", run.stdout);
    assert!(run.stdout.contains("orders"), "{}", run.stdout);
    let run = env
        .felixctl(&cluster, &["stream", "info", "orders", "--json"])
        .await
        .ok();
    assert_eq!(run.json()["shards"], 2);
    let run = env.felixctl(&cluster, &["stream", "info", "missing"]).await;
    assert_eq!(run.code, 5, "{}", run.stderr);

    let run = env
        .felixctl(&cluster, &["cache", "ls", "--json"])
        .await
        .ok();
    assert_eq!(run.json()["caches"][0]["cache"], "sessions");
    let run = env
        .felixctl(&cluster, &["cache", "info", "sessions", "--json"])
        .await
        .ok();
    assert_eq!(run.json()["shards"], 1);

    let run = env.felixctl(&cluster, &["node", "ls", "--json"]).await.ok();
    let nodes = run.json()["nodes"].as_array().unwrap().clone();
    assert_eq!(nodes.len(), 1, "{}", run.stdout);
    let node_id = nodes[0]["node"]["node_id"].as_str().unwrap().to_string();
    let run = env
        .felixctl(&cluster, &["node", "info", &node_id])
        .await
        .ok();
    assert!(run.stdout.contains(&node_id), "{}", run.stdout);
    let run = env
        .felixctl(&cluster, &["node", "info", "no-such-node"])
        .await;
    assert_eq!(run.code, 5, "{}", run.stderr);

    let run = env
        .felixctl(&cluster, &["shard", "ls", "--name", "orders", "--json"])
        .await
        .ok();
    let shards = run.json()["shards"].as_array().unwrap().clone();
    assert_eq!(shards.len(), 2, "{}", run.stdout);
    assert!(shards.iter().all(|s| s["leader"] == node_id.as_str()));
    let run = env
        .felixctl(
            &cluster,
            &["shard", "ls", "--leader", "no-such-node", "--json"],
        )
        .await
        .ok();
    assert_eq!(run.json()["shards"].as_array().unwrap().len(), 0);
}

#[tokio::test]
#[serial]
async fn creating_changing_and_deleting_through_the_control_plane() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new("orders", 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let env = Env::new(&cluster);

    env.felixctl(&cluster, &["namespace", "create", "scratch"])
        .await
        .ok();
    let run = env
        .felixctl(&cluster, &["namespace", "create", "scratch"])
        .await;
    assert_eq!(run.code, 4, "a second create is refused: {}", run.stderr);

    let scratch = ["-n", "scratch"];
    let with = |args: &[&'static str]| -> Vec<&'static str> {
        args.iter().chain(&scratch).copied().collect()
    };

    let run = env
        .felixctl(
            &cluster,
            &with(&["stream", "create", "events", "--shards", "2", "--json"]),
        )
        .await
        .ok();
    assert_eq!(run.json()["shards"], 2, "{}", run.stdout);
    let run = env
        .felixctl(
            &cluster,
            &with(&["stream", "create", "events", "--shards", "2"]),
        )
        .await
        .ok();
    assert!(run.stdout.contains("already exists"), "{}", run.stdout);
    let run = env
        .felixctl(
            &cluster,
            &with(&["stream", "create", "events", "--shards", "3"]),
        )
        .await;
    assert_eq!(
        run.code, 4,
        "different settings are refused: {}",
        run.stderr
    );

    let run = env
        .felixctl(
            &cluster,
            &with(&["stream", "set", "events", "--retention-secs", "3600"]),
        )
        .await
        .ok();
    assert!(run.stdout.contains("retention:"), "{}", run.stdout);
    let run = env
        .felixctl(&cluster, &with(&["stream", "info", "events", "--json"]))
        .await
        .ok();
    assert_eq!(run.json()["retention"]["max_age_seconds"], 3600);

    env.felixctl(&cluster, &with(&["cache", "create", "kv", "--shards", "2"]))
        .await
        .ok();
    let run = env
        .felixctl(
            &cluster,
            &with(&["cache", "set", "kv", "--display-name", "Key value"]),
        )
        .await
        .ok();
    assert!(
        run.stdout.contains("display_name: kv -> Key value"),
        "{}",
        run.stdout
    );

    // Stdin is not a terminal here, so a delete without --yes stops.
    let run = env.felixctl(&cluster, &with(&["cache", "rm", "kv"])).await;
    assert_eq!(run.code, 2, "{}", run.stderr);
    env.felixctl(&cluster, &with(&["cache", "info", "kv"]))
        .await
        .ok();
    env.felixctl(&cluster, &with(&["cache", "rm", "kv", "--yes"]))
        .await
        .ok();
    let run = env
        .felixctl(&cluster, &with(&["cache", "info", "kv"]))
        .await;
    assert_eq!(run.code, 5, "{}", run.stderr);

    let run = env
        .felixctl(
            &cluster,
            &with(&["stream", "rm", "events", "--yes", "--json"]),
        )
        .await
        .ok();
    assert_eq!(run.json()["deleted"], "stream");
    env.felixctl(&cluster, &["namespace", "rm", "scratch", "--yes"])
        .await
        .ok();
    let run = env
        .felixctl(&cluster, &["namespace", "info", "scratch"])
        .await;
    assert_eq!(run.code, 5, "{}", run.stderr);

    let cluster_admin = cluster.credentials().cluster_admin_token();
    let as_cluster_admin = |args: &[&'static str]| -> Vec<String> {
        args.iter()
            .map(|arg| arg.to_string())
            .chain(["--controlplane-token".to_string(), cluster_admin.clone()])
            .collect()
    };
    for args in [
        as_cluster_admin(&["tenant", "create", "acme"]),
        as_cluster_admin(&["tenant", "rm", "acme", "--yes"]),
    ] {
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        env.felixctl(&cluster, &args).await.ok();
    }
}

#[tokio::test]
#[serial]
async fn moving_shards_draining_nodes_and_pausing_placement() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::new("orders", 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let env = Env::new(&cluster);
    let run = as_operator(&env, &cluster, vec!["placement", "pause", "--json"])
        .await
        .ok();
    assert_eq!(run.json()["paused"], true);

    let run = as_operator(
        &env,
        &cluster,
        vec!["shard", "ls", "--name", "orders", "--json"],
    )
    .await
    .ok();
    let leader = run.json()["shards"][0]["leader"]
        .as_str()
        .expect("a leader")
        .to_string();
    let run = as_operator(&env, &cluster, vec!["node", "ls", "--json"])
        .await
        .ok();
    let other = run.json()["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|node| node["node"]["node_id"].as_str().unwrap().to_string())
        .find(|node| *node != leader)
        .expect("a second node");

    let run = as_operator(
        &env,
        &cluster,
        vec![
            "shard",
            "move",
            "orders",
            "0",
            "--to",
            &other,
            "--dry-run",
            "--json",
        ],
    )
    .await
    .ok();
    assert_eq!(run.json()["dry_run"], true, "{}", run.stdout);
    let run = as_operator(
        &env,
        &cluster,
        vec!["shard", "move", "orders", "0", "--to", &other, "--dry-run"],
    )
    .await
    .ok();
    assert!(run.stdout.contains("dry run"), "{}", run.stdout);
    // Nothing is moving, so there is nothing to cancel.
    let run = as_operator(
        &env,
        &cluster,
        vec!["shard", "move", "cancel", "orders", "0"],
    )
    .await;
    assert_eq!(run.code, 4, "{}", run.stderr);
    // The leader is serving, so its log is not stranded.
    let run = as_operator(
        &env,
        &cluster,
        vec!["placement", "abandon", "orders", "0", "--yes"],
    )
    .await;
    assert_eq!(run.code, 4, "{}", run.stderr);

    let run = as_operator(&env, &cluster, vec!["placement", "resume", "--json"])
        .await
        .ok();
    assert_eq!(run.json()["paused"], false);

    let run = as_operator(&env, &cluster, vec!["node", "drain", &other]).await;
    assert_eq!(run.code, 2, "no --yes off a terminal: {}", run.stderr);
    let run = as_operator(&env, &cluster, vec!["node", "drain", &other, "--yes"])
        .await
        .ok();
    assert_eq!(run.stdout.trim(), format!("node {other}: draining"));
    let run = as_operator(
        &env,
        &cluster,
        vec!["node", "deregister", &other, "--yes", "--json"],
    )
    .await
    .ok();
    assert_eq!(run.json()["status"]["lifecycle"], "left", "{}", run.stdout);
    let run = as_operator(
        &env,
        &cluster,
        vec!["node", "drain", "no-such-node", "--yes"],
    )
    .await;
    assert_eq!(run.code, 5, "{}", run.stderr);
}

/// Run `felixctl` with a token allowed `node.manage`, which moves, drains and
/// pausing placement need.
async fn as_operator(env: &Env, cluster: &Cluster, mut args: Vec<&str>) -> Run {
    let operator = cluster.operator_token();
    args.extend(["--controlplane-token", operator.as_str()]);
    env.felixctl(cluster, &args).await
}
