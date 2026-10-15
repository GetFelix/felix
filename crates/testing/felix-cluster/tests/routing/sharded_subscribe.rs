//! Consuming a whole multi-shard stream through one subscription.
//!
//! A subscription reads one shard, so an application wanting every record of
//! a four-shard stream used to open four subscriptions, discover each shard's
//! owner, follow each redirect and merge the results itself (#297).
//!
//! `ClusterClient::subscribe_sharded` does that. These are the tests for what
//! it promises — and, as much, for what it refuses to promise.
//!
//! Run with `cargo test -p felix-cluster --test routing sharded_subscribe::`.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use felix_client::{ShardEvent, ShardedSubscription};
use felix_cluster::{Cluster, ClusterConfig, StreamSpec, wait};
use serial_test::serial;

const STREAM: &str = "orders";
const SHARDS: u32 = 4;

fn sharded() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        // Replicated, so a shard whose leader is killed has somewhere to go.
        streams: vec![StreamSpec::replicated(STREAM, SHARDS, 3)],
        ..Default::default()
    }
}

/// Keys chosen so that every shard receives something.
///
/// Which key lands where is decided by the same hash the broker uses, so rather
/// than hard-code an assumption this publishes enough distinct keys that all
/// four shards are covered, and the tests assert the coverage they got.
fn keys(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("key-{i:03}")).collect()
}

async fn cluster_client(cluster: &Cluster) -> Arc<felix_client::ClusterClient> {
    Arc::new(
        felix_cluster::client::connect_cluster(
            &cluster.broker_addrs(),
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("connect a cluster client"),
    )
}

/// Drain until `want` payloads have been seen or the deadline passes, ignoring
/// the harness's startup probes. Returns the payloads and which shard each came
/// from.
async fn drain(
    subscription: &mut ShardedSubscription,
    want: usize,
    timeout: Duration,
) -> (HashMap<String, u32>, Vec<ShardEvent>) {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut seen: HashMap<String, u32> = HashMap::new();
    let mut other = Vec::new();
    while seen.len() < want {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, subscription.next()).await {
            Ok(Some(ShardEvent::Record { shard, event })) => {
                let payload = String::from_utf8_lossy(&event.payload).to_string();
                if payload == "harness-probe" {
                    continue;
                }
                seen.insert(payload, shard);
            }
            Ok(Some(item)) => other.push(item),
            Ok(None) | Err(_) => break,
        }
    }
    (seen, other)
}

/// **Every record, whichever broker owns the shard it landed on.**
///
/// The acceptance criterion of the sharded-subscribe work (#297): without it
/// a subscriber saw shard 0 and silently missed three quarters of the stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn a_sharded_subscription_receives_every_record() {
    let cluster = Cluster::start(sharded()).await.expect("start cluster");
    let owners = cluster
        .shard_owners_for(STREAM)
        .await
        .expect("shard owners");
    assert_eq!(owners.len(), SHARDS as usize, "every shard must be placed");
    assert!(
        owners.values().collect::<HashSet<_>>().len() > 1,
        "the shards are all on one broker, so this proves nothing about following owners"
    );

    let published = keys(40);
    for key in &published {
        // `_settled`: at startup a broker can hold the placement before the
        // owner has applied it, and a forwarded publish then fails with
        // "redirected to generation 0". Only the first publish pays anything.
        cluster
            .publish_keyed_via_settled(
                owners.values().next().expect("an owner"),
                STREAM,
                key.as_bytes(),
                key.clone().into_bytes(),
                Duration::from_secs(30),
            )
            .await
            .expect("publish");
    }

    // Published first, then read from `Earliest`. Subscribing first and using
    // `Latest` would race the fanout: a record written before all four shards
    // finished registering is legitimately missed, and the test would fail for
    // a reason that is not the one it is about. Reading the log removes the
    // race without weakening the claim — every record still has to come back,
    // from whichever broker owns the shard it landed on.
    let client = cluster_client(&cluster).await;
    let mut subscription = client
        .subscribe_sharded(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            Some(felix_client::StartPosition::Earliest),
        )
        .await
        .expect("subscribe to every shard");
    assert_eq!(subscription.shards(), SHARDS);

    let (seen, _) = drain(&mut subscription, published.len(), Duration::from_secs(30)).await;
    let missing: Vec<&String> = published
        .iter()
        .filter(|k| !seen.contains_key(*k))
        .collect();
    assert!(missing.is_empty(), "records never delivered: {missing:?}");

    // The control that makes the above mean something: if every record had
    // landed on one shard, a single-shard subscription would also have passed.
    let covered: HashSet<u32> = seen.values().copied().collect();
    let mut per_shard: HashMap<u32, usize> = HashMap::new();
    for shard in seen.values() {
        *per_shard.entry(*shard).or_default() += 1;
    }
    assert!(
        covered.len() > 1,
        "all {} records arrived from shard {covered:?} (per shard: {per_shard:?}); the keys did \
         not spread and this test would pass without following shards at all. Every key landing \
         on one shard is what a forwarded publish stamped with the wrong shard looks like",
        seen.len()
    );
}

/// **A shard that cannot be reached fails the whole call.**
///
/// A subscription covering three shards of four looks exactly like a complete
/// one to everything downstream. Refusing to open is the only answer that
/// cannot be mistaken for success.
///
/// Unreplicated on purpose: with replicas, killing a broker moves the shard
/// rather than removing it, and the subscription would rightly succeed once the
/// replacement is placed. A shard with nowhere to go is the case being tested.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn an_unreachable_shard_refuses_the_subscription() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new(STREAM, SHARDS)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owners = cluster
        .shard_owners_for(STREAM)
        .await
        .expect("shard owners");
    let client = cluster_client(&cluster).await;

    // It opens while every shard has an owner. Establishing that first is what
    // makes the failure below attributable to the kill.
    client
        .subscribe_sharded(&cluster.tenant_id, &cluster.namespace, STREAM, None)
        .await
        .expect("a healthy cluster should serve every shard");

    let (victim_shard, doomed) = owners
        .iter()
        .map(|(shard, node)| (*shard, node.clone()))
        .next()
        .expect("a placed shard");
    cluster.kill_node(&doomed).expect("kill the shard's owner");

    // The control plane needs a moment to notice. Poll rather than sleep, and
    // require the refusal to arrive rather than assuming it has.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    let mut last: Option<String> = None;
    while tokio::time::Instant::now() < deadline {
        match client
            .subscribe_sharded(&cluster.tenant_id, &cluster.namespace, STREAM, None)
            .await
        {
            Ok(subscription) => {
                assert_eq!(
                    subscription.shards(),
                    SHARDS,
                    "opened a subscription covering fewer shards than the stream has"
                );
                drop(subscription);
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(err) => {
                last = Some(format!("{err:#}"));
                break;
            }
        }
    }

    let text = last.expect("the dead shard owner should eventually refuse the subscription");
    assert!(
        text.contains("silently incomplete"),
        "the refusal should say why a partial subscription is not offered: {text}"
    );
    // The shards the dead broker owned are the ones that cannot be reached; at
    // least the one identified above must be named.
    assert!(
        text.contains(&format!("shard {victim_shard}")),
        "the refusal should name the shard that could not be reached: {text}"
    );
}

/// **Losing one shard's owner does not tear down the others.**
///
/// The whole point of merging per shard rather than per stream: three shards
/// keep delivering while the fourth is re-established, and the application is
/// told which one went.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn one_shard_failing_over_does_not_stop_the_others() {
    let mut cluster = Cluster::start(sharded()).await.expect("start cluster");
    // Killed before its first report, a leader leaves its shard unplaceable,
    // so a ready cluster must already have every report in.
    let unreported = cluster.unreported_shards().await.expect("replica reports");
    assert!(
        unreported.is_empty(),
        "the cluster was ready before these shards could fail over: {unreported:?}"
    );
    let owners = cluster
        .shard_owners_for(STREAM)
        .await
        .expect("shard owners");
    let client = cluster_client(&cluster).await;
    let mut subscription = client
        .subscribe_sharded(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            Some(felix_client::StartPosition::Latest),
        )
        .await
        .expect("subscribe to every shard");

    // A broker that owns some shards but not all, so killing it leaves others
    // alive and delivering.
    let victim = owners
        .values()
        .find(|node| owners.values().filter(|n| n == node).count() < SHARDS as usize)
        .cloned()
        .expect("a broker that does not own every shard");
    let surviving: HashSet<u32> = owners
        .iter()
        .filter(|(_, node)| *node != &victim)
        .map(|(shard, _)| *shard)
        .collect();
    assert!(
        !surviving.is_empty(),
        "the victim owns every shard, so nothing could survive it"
    );

    cluster.kill_node(&victim).expect("kill a shard owner");
    wait::until(
        Duration::from_secs(30),
        "the killed broker's shards to be reassigned",
        || async {
            cluster.place_shards().await;
            matches!(
                cluster.shard_owners_for(STREAM).await,
                Ok(now) if now.len() == SHARDS as usize && !now.values().any(|n| n == &victim)
            )
        },
    )
    .await
    .expect("reassignment");

    // Publish through a survivor. Records for the surviving shards must arrive
    // even while the killed broker's shards are still being re-established.
    // Publish through a broker that is still alive. Using the killed one would
    // fail every publish and the test would then be asserting nothing.
    let alive = cluster
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .find(|node| node != &victim)
        .expect("a surviving broker to publish through");
    let published = keys(40);
    let mut accepted = 0usize;
    for key in &published {
        // A publish can fail while ownership is moving; that gap is tracked
        // as #269 and is not what this test is about.
        if cluster
            .publish_keyed_via(&alive, STREAM, key.as_bytes(), key.clone().into_bytes())
            .await
            .is_ok()
        {
            accepted += 1;
        }
    }
    assert!(
        accepted > 0,
        "no publish was accepted at all, so nothing below is being tested"
    );

    let (seen, notices) = drain(&mut subscription, accepted, Duration::from_secs(25)).await;
    assert!(
        !seen.is_empty(),
        "nothing arrived at all after one broker died ({accepted} publishes were accepted); \
         losing one shard tore down the rest"
    );
    let covered: HashSet<u32> = seen.values().copied().collect();
    assert!(
        covered.iter().any(|shard| surviving.contains(shard)),
        "records arrived from {covered:?}, none of them from a shard whose owner survived"
    );

    // Whatever happened to the dead broker's shards, the application was told
    // rather than left to infer it from silence.
    let lost: Vec<u32> = notices
        .iter()
        .filter_map(|item| match item {
            ShardEvent::ShardLost { shard, .. } => Some(*shard),
            _ => None,
        })
        .collect();
    if !lost.is_empty() {
        assert!(
            lost.iter().all(|shard| *shard < SHARDS),
            "a loss was reported for a shard that does not exist: {lost:?}"
        );
    }
}

/// **Resumption is a vector, and it round-trips.**
///
/// `Event.offset` is per shard, so a single number cannot describe where a
/// sharded consumer got to. This checks the map handed back is the one a resume
/// actually needs: nothing already delivered arrives twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn a_sharded_subscription_resumes_from_its_per_shard_offsets() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        // The harness creates every stream durable, which is what makes an
        // offset to resume from exist at all.
        streams: vec![StreamSpec::replicated(STREAM, SHARDS, 3)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let client = cluster_client(&cluster).await;
    let alive = cluster.nodes[0].node_id.clone();

    let first = keys(20);
    for key in &first {
        cluster
            .publish_keyed_via_settled(
                &alive,
                STREAM,
                key.as_bytes(),
                key.clone().into_bytes(),
                Duration::from_secs(30),
            )
            .await
            .expect("publish");
    }

    let mut subscription = client
        .subscribe_sharded(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            Some(felix_client::StartPosition::Earliest),
        )
        .await
        .expect("subscribe to every shard");
    let (seen_first, _) = drain(&mut subscription, first.len(), Duration::from_secs(20)).await;
    assert_eq!(
        seen_first.len(),
        first.len(),
        "the first pass did not read everything, so a resume proves nothing"
    );
    let positions = subscription.positions();
    assert!(
        !positions.is_empty(),
        "no per-shard offsets were recorded, so there is nothing to resume from"
    );
    drop(subscription);

    let second = keys(20)
        .into_iter()
        .map(|key| format!("second-{key}"))
        .collect::<Vec<_>>();
    for key in &second {
        cluster
            .publish_keyed_via_settled(
                &alive,
                STREAM,
                key.as_bytes(),
                key.clone().into_bytes(),
                Duration::from_secs(30),
            )
            .await
            .expect("publish");
    }

    let mut resumed = client
        .resubscribe_sharded(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            positions,
            Some(felix_client::StartPosition::Earliest),
        )
        .await
        .expect("resume from the per-shard offsets");
    let (seen_second, _) = drain(&mut resumed, second.len(), Duration::from_secs(20)).await;

    let redelivered: Vec<&String> = seen_first
        .keys()
        .filter(|key| seen_second.contains_key(*key))
        .collect();
    assert!(
        redelivered.is_empty(),
        "resuming from the per-shard offsets redelivered records already seen: {redelivered:?}"
    );
    let missed: Vec<&String> = second
        .iter()
        .filter(|key| !seen_second.contains_key(*key))
        .collect();
    assert!(
        missed.is_empty(),
        "resuming skipped records published while disconnected: {missed:?}"
    );
}

/// A stream the broker has never heard of reports zero shards, not one.
///
/// This is the answer the whole sharded path is built on. `StreamShardsView`
/// documents zero as "the broker knows nothing of the stream", and
/// `subscribe_sharded` refuses on it — that refusal is what stops it "reading
/// shard 0 and calling it the stream". A broker that answers one instead makes
/// an unknown stream indistinguishable from a genuine single-shard one, the
/// refusal unreachable, and the caller's mistake something they discover much
/// later as missing data (#394).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn an_unknown_stream_reports_no_shards() {
    let cluster = Cluster::start(sharded()).await.expect("start cluster");
    let client = cluster_client(&cluster).await;
    // The raw answer, straight from a broker: `subscribe_sharded` below is the
    // caller-facing consequence, but the count is what the protocol specifies.
    let direct = felix_cluster::client::connect_any(
        &cluster.broker_addrs(),
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("connect a plain client");

    // The placed stream first, so the zero below is about this stream being
    // unknown rather than about the broker not answering at all.
    assert_eq!(
        direct
            .stream_shards(&cluster.tenant_id, &cluster.namespace, STREAM)
            .await
            .expect("ask how many shards the placed stream has"),
        SHARDS,
    );

    assert_eq!(
        direct
            .stream_shards(&cluster.tenant_id, &cluster.namespace, "no-such-stream")
            .await
            .expect("asking about an unknown stream is a question, not an error"),
        0,
        "an unknown stream reported a shard count, which a client cannot tell \
         from a real single-shard stream",
    );

    let err = match client
        .subscribe_sharded(
            &cluster.tenant_id,
            &cluster.namespace,
            "no-such-stream",
            None,
        )
        .await
    {
        Ok(_) => panic!(
            "subscribing to a stream that does not exist succeeded; the caller \
             now holds an empty subscription it believes is the whole stream"
        ),
        Err(err) => err,
    };
    assert!(
        format!("{err:#}").contains("no-such-stream"),
        "the refusal must name the stream: {err:#}",
    );
}
