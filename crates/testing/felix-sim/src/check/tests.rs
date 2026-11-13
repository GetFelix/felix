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
        recorder: Recorder::new(4),
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

#[tokio::test(start_paused = true)]
async fn shards_with_a_majority_of_doubted_copies_are_held_not_reported() {
    let cluster = SimCluster::start(3, config()).await.unwrap();
    // Two of three killed: every shard's set has two copies that restart
    // doubted, so no attestation majority exists. The same two stopped
    // cleanly (above) is a stall.
    cluster.kill(0).await;
    cluster.kill(1).await;
    let found = ElectionSettles::default().check(&context(&cluster)).await;
    assert!(found.is_empty(), "{}", found[0]);
}

#[test]
fn a_finding_prints_its_checker_summary_and_detail() {
    let finding = Finding::new("example", "two leaders").with("table");
    assert_eq!(finding.to_string(), "[example] two leaders\ntable\n");
}

#[test]
fn a_shard_held_for_two_unclean_kills_is_not_reported() {
    use crate::cluster::stream_key;
    let stuck = vec![(stream_key(0), vec![]), (stream_key(1), vec![])];
    let held = BTreeSet::from(["stream/1".to_string()]);
    let left = without_held(stuck, &held);
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].0, stream_key(0));
}

/// At the end, an acknowledged record no fault took must be on some disk,
/// whether or not its shard could be read back.
#[tokio::test(start_paused = true)]
async fn an_acknowledged_record_on_no_disk_at_the_end_is_a_finding() {
    use felix_cluster::history::model::{Action, AppendOutcome, Op};
    use felix_cluster::history::rng::Rng;

    let cluster = SimCluster::start(3, config()).await.unwrap();
    let recorder = crate::client::Recorder::new(cluster.config.shards);
    let mut client =
        crate::client::SimClient::new(0, cluster.clone(), recorder.clone(), Rng::new(9));
    client.append(0).await;
    let end = |history| CheckContext {
        point: Point::End,
        history,
        ..context(&cluster)
    };
    let failing = |found: &[Finding]| found.iter().any(|f| f.severity == Severity::Failure);

    let found = AckedOnSomeDisk.check(&end(recorder.history())).await;
    assert!(!failing(&found), "{found:?}");

    recorder.push(Op {
        process: 1,
        invoke: 0,
        complete: 1,
        action: Action::Append {
            list: crate::client::list_name(1),
            value: 1 << 40,
            outcome: AppendOutcome::Ok { offset: Some(0) },
        },
    });
    let found = AckedOnSomeDisk.check(&end(recorder.history())).await;
    assert!(failing(&found), "{found:?}");
}
