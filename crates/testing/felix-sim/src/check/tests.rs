use std::time::Duration;

use tokio::time::Instant;

use super::*;
use crate::cluster::SimConfig;
use crate::nemesis::Plan;

fn config() -> SimConfig {
    SimConfig {
        brokers: 3,
        shards: 4,
        cache_shards: 2,
        settle_within: Duration::from_secs(10),
        ..SimConfig::for_seed(3)
    }
}

fn context(cluster: &Arc<SimCluster>) -> CheckContext {
    CheckContext {
        cluster: Arc::clone(cluster),
        point: Point::Healed,
        healed_at: cluster.elapsed(),
        history: History::default(),
        plan: Plan::default(),
    }
}

#[tokio::test(start_paused = true)]
async fn a_healthy_cluster_settles() {
    let cluster = SimCluster::start(3, config()).await.unwrap();
    let found = ElectionSettles::default().check(&context(&cluster)).await;
    assert!(found.is_empty(), "{}", found[0]);
}

#[tokio::test(start_paused = true)]
async fn shards_that_cannot_elect_are_reported_with_a_census() {
    let cluster = SimCluster::start(3, config()).await.unwrap();
    // Two of three down: no shard can reach a quorum of its replicas.
    cluster.crash(0).await;
    cluster.crash(1).await;
    let ctx = context(&cluster);
    let began = Instant::now();
    let found = ElectionSettles::default().check(&ctx).await;
    assert_eq!(found.len(), 1);
    assert!(began.elapsed() >= Duration::from_secs(10));
    let text = found[0].to_string();
    assert!(text.contains("election-settles"), "{text}");
    assert!(text.contains("not served by exactly one broker"), "{text}");
    // The census names every node and the control plane's assignment.
    for node in ["b1", "b2", "b3"] {
        assert!(text.contains(node), "{text}");
    }
    assert!(text.contains("control plane:"), "{text}");
    assert!(text.contains("down"), "{text}");
}

#[test]
fn a_finding_prints_its_checker_summary_and_detail() {
    let finding = Finding::new("example", "two leaders").with("table");
    assert_eq!(finding.to_string(), "[example] two leaders\ntable\n");
}
