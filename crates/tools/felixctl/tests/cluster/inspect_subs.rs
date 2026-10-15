//! `felixctl inspect subs` against a cluster: a live subscriber is listed
//! with its connection and principal by whichever broker serves it.

use std::process::Stdio;
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

use crate::Env;

#[tokio::test]
#[serial]
async fn listing_a_live_subscriber() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum("orders", 1, 3)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let env = Env::new(&cluster);

    // A subscriber that stays connected until the test ends.
    let mut args = vec!["sub".to_string(), "orders".to_string()];
    args.extend(env.flags(&cluster));
    let _subscriber = tokio::process::Command::new(env!("CARGO_BIN_EXE_felixctl"))
        .args(&args)
        .env_clear()
        .env("HOME", env.dir.path())
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("FELIX_CLI_CONFIG", env.config())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn subscriber");

    // A tenant's token is refused: subscribers of every tenant are
    // cluster scope.
    let run = env.felixctl(&cluster, &["inspect", "subs"]).await;
    assert_eq!(run.code, 4, "{}", run.stderr);
    assert!(run.stderr.contains("node.view:cluster:*"), "{}", run.stderr);

    let inspector = cluster.inspector_token();
    let path = format!("{}/{}/orders", cluster.tenant_id, cluster.namespace);
    let args = [
        "inspect",
        "subs",
        path.as_str(),
        "--json",
        "--token",
        inspector.as_str(),
    ];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let (node, listed) = loop {
        let pages = env.felixctl(&cluster, &args).await.ok().json_lines();
        assert_eq!(pages.len(), 3, "one page per broker: {pages:?}");
        let found = pages.iter().find_map(|page| {
            let subscription = page["subscriptions"]
                .as_array()?
                .iter()
                .find(|s| s["principal"] == "p:harness-client")?;
            Some((page["node_id"].clone(), subscription.clone()))
        });
        if let Some(found) = found {
            break found;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no subscriber listed: {pages:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert!(node.is_string(), "{node}");
    assert_eq!(listed["stream"], "orders", "{listed}");
    assert_eq!(listed["shard"], 0, "{listed}");
    assert_eq!(listed["policy"], "drop_new", "{listed}");
    assert!(listed["connection"].is_u64(), "{listed}");
    assert!(listed["peer"].is_string(), "{listed}");

    // A subscriber that keeps up has dropped nothing.
    let run = env
        .felixctl(
            &cluster,
            &[
                "inspect",
                "subs",
                "--dropping",
                "--token",
                inspector.as_str(),
            ],
        )
        .await
        .ok();
    assert!(run.stdout.contains("no subscriptions"), "{}", run.stdout);
}
