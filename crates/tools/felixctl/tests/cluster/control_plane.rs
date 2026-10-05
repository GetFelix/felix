//! The read-only control-plane commands.

use felix_cluster::{CacheSpec, Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

use crate::Env;

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
