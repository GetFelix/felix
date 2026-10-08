//! `felixctl inspect shard` against a replicated shard: the leader serves, and
//! every replica is accounted for, from the leader's view and its own.

use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

use crate::Env;

#[tokio::test]
#[serial]
async fn inspecting_a_replicated_shard() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum("orders", 1, 3)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let env = Env::new(&cluster);
    env.felixctl(&cluster, &["pub", "orders", "hello"])
        .await
        .ok();

    // A tenant's token is refused: inspection is cluster scope.
    let run = env
        .felixctl(&cluster, &["inspect", "shard", "orders", "--shard", "0"])
        .await;
    assert_eq!(run.code, 4, "{}", run.stderr);
    assert!(run.stderr.contains("node.view:cluster:*"), "{}", run.stderr);

    let inspector = cluster.inspector_token();
    let path = format!("{}/{}/orders", cluster.tenant_id, cluster.namespace);
    let args = [
        "inspect",
        "shard",
        path.as_str(),
        "--shard",
        "0",
        "--json",
        "--token",
        inspector.as_str(),
    ];
    // Followers start stalled until their first answer, so wait for the
    // leader to report both shipping.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let report = loop {
        let report = env.felixctl(&cluster, &args).await.ok().json();
        let shipping = report["replicas"].as_array().is_some_and(|replicas| {
            replicas.len() == 2 && replicas.iter().all(|r| r["state"] == "shipping")
        });
        if shipping || tokio::time::Instant::now() > deadline {
            break report;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert_eq!(report["leader"]["serving"], true, "{report}");
    assert_eq!(report["leader"]["phase"], "active", "{report}");
    assert_eq!(report["unreachable"], serde_json::json!([]), "{report}");
    let leader = report["assignment"]["leader"].as_str().expect("a leader");
    assert_eq!(report["leader"]["node_id"], leader);
    for replica in report["replicas"].as_array().expect("replicas") {
        assert_eq!(replica["state"], "shipping", "{report}");
        assert!(replica["own"].is_object(), "{report}");
    }

    let run = env
        .felixctl(
            &cluster,
            &["inspect", "shard", "orders", "--token", inspector.as_str()],
        )
        .await
        .ok();
    assert!(run.stdout.contains("serving"), "{}", run.stdout);
    assert!(run.stdout.contains("REPLICA"), "{}", run.stdout);
}
