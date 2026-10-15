//! A saved context carries the connection, and flags and the environment
//! override it.

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

use crate::Env;

#[tokio::test]
#[serial]
async fn a_context_carries_the_connection() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new("orders", 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let env = Env::new(&cluster);
    let token_file = env.dir.path().join("token.jwt");
    std::fs::write(&token_file, cluster.client_token()).expect("write token");

    let brokers: Vec<String> = cluster
        .broker_addrs()
        .iter()
        .map(|addr| addr.to_string())
        .collect();
    let args = |list: &[&str]| list.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    env.run(
        &args(&[
            "context",
            "add",
            "local",
            "--brokers",
            &brokers.join(","),
            "--tenant",
            &cluster.tenant_id,
            "--namespace",
            &cluster.namespace,
            "--token-file",
            &token_file.display().to_string(),
            "--ca-file",
            &env.ca.display().to_string(),
            "--controlplane-url",
            cluster.control_plane_url(),
            "--controlplane-token",
            &cluster.admin_token(),
        ]),
        &[],
        None,
    )
    .await
    .ok();

    let run = env
        .run(&args(&["context", "ls", "--json"]), &[], None)
        .await
        .ok();
    let contexts = run.json()["contexts"].as_array().unwrap().clone();
    assert_eq!(contexts[0]["name"], "local");
    assert_eq!(contexts[0]["current"], true);
    assert_eq!(contexts[0]["controlplane_token"], "<redacted>");

    // No connection flags at all.
    env.run(&args(&["pub", "orders", "via-context"]), &[], None)
        .await
        .ok();
    let offset = env.first_after_probes(&cluster, "orders").await.to_string();
    let run = env
        .run(
            &args(&["sub", "orders", "--from", &offset, "--count", "1"]),
            &[],
            None,
        )
        .await
        .ok();
    assert_eq!(run.stdout, "via-context\n");
    env.run(&args(&["stream", "info", "orders"]), &[], None)
        .await
        .ok();

    // The environment overrides the context, and a flag overrides both.
    let run = env
        .run(
            &args(&["stream", "info", "orders"]),
            &[("FELIX_NAMESPACE", "elsewhere")],
            None,
        )
        .await;
    assert_ne!(
        run.code, 0,
        "the namespace from the environment was ignored"
    );
    let mut flagged = args(&["stream", "info", "orders", "--namespace"]);
    flagged.push(cluster.namespace.clone());
    env.run(&flagged, &[("FELIX_NAMESPACE", "elsewhere")], None)
        .await
        .ok();

    let run = env
        .run(
            &args(&["pub", "orders", "x"]),
            &[("FELIX_CONTEXT", "missing")],
            None,
        )
        .await;
    assert_eq!(run.code, 5, "{}", run.stderr);
}
