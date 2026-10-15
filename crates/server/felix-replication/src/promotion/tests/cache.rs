//! A promoted cache shard's fence: its cache log and then its counter log,
//! each taking the log furthest ahead among a majority's answers.

use felix_broker::LogKind;
use felix_storage::cache::CacheOp;

use super::*;

/// Replicas that offer everything a cache shard's fence needs.
fn cache_replicas() -> Replicas {
    let mut replicas = Replicas::new();
    for id in ["broker-b", "broker-c"] {
        replicas.get_mut(id).capabilities = REQUIRED_FOR_CACHES;
    }
    replicas
}

async fn cache_leader() -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    (broker_on(dir.path()), dir)
}

fn put(key: &str, value: &str) -> String {
    let op = CacheOp::Put {
        key: key.to_string(),
        value: Bytes::copy_from_slice(value.as_bytes()),
        expires_at_millis: 0,
        version: None,
    };
    String::from_utf8(op.encode().to_vec()).expect("an ascii put encodes as utf8")
}

async fn read(broker: &Broker, key: &str) -> Option<String> {
    broker
        .cache()
        .get(TENANT, NAMESPACE, STREAM, 0, key)
        .await
        .expect("get")
        .map(|value| String::from_utf8(value.to_vec()).expect("utf8"))
}

/// **A promoted cache leader takes the counter log furthest ahead, as well as
/// the cache log.** A counter add acknowledged on broker-b and the old leader
/// is on no other copy; opening without it would lose it from the sum.
#[tokio::test]
async fn the_counter_log_furthest_ahead_is_taken_too() {
    let mut replicas = cache_replicas();
    let first = put("k", "v1");
    replicas
        .get("broker-b")
        .holds_in(LogKind::Cache, "broker-b", 4, 0, &[&first])
        .await;
    replicas
        .get("broker-b")
        .holds_in(LogKind::Counters, "broker-b", 4, 0, &["d1", "d2", "d3"])
        .await;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = cache_leader().await;
    holding(&leader, LogKind::Cache, 4, &[&first]).await;
    holding(&leader, LogKind::Counters, 4, &["d1"]).await;

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key_of(ShardKind::Cache),
        &route_of(ShardKind::Cache),
        true,
    )
    .await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: Some("broker-b".to_string())
        }
    );
    assert_eq!(
        held_in(&leader, LogKind::Counters).await,
        vec!["d1", "d2", "d3"]
    );
    assert_eq!(held_in(&leader, LogKind::Cache).await, vec![first]);
    // Both logs took the fence on broker-b, and the counters were read only
    // once it had.
    let fences = replicas.sent(Kind::Fence);
    assert_eq!(fences, vec!["broker-b", "broker-b"]);
    let counters = replicas
        .get("broker-b")
        .broker
        .shard_log(LogKind::Counters, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    assert_eq!(counters.accepted_generation(), PROMOTED);
}

/// **A cache log the leader takes over its own leaves no trace of the
/// superseded puts in its index.** The leader read `k` from a put that broker-b's newer
/// log replaced with another key's; served from the old index, `k` would
/// read that key's value.
#[tokio::test]
async fn a_superseded_put_is_gone_from_the_index() {
    let mut replicas = cache_replicas();
    let (first, stale, other) = (put("k", "v1"), put("k", "stale"), put("other", "x"));
    for id in ["broker-b", "broker-c"] {
        replicas
            .get(id)
            .holds_in(LogKind::Cache, id, 3, 0, &[&first])
            .await;
        replicas
            .get(id)
            .holds_in(LogKind::Cache, id, 4, 0, &[&first, &other])
            .await;
    }
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = cache_leader().await;
    holding(&leader, LogKind::Cache, 3, &[&first, &stale]).await;
    assert_eq!(read(&leader, "k").await.as_deref(), Some("stale"));

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key_of(ShardKind::Cache),
        &route_of(ShardKind::Cache),
        true,
    )
    .await;

    assert!(
        matches!(
            outcome,
            Outcome::Fenced {
                caught_up_from: Some(_)
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(read(&leader, "k").await.as_deref(), Some("v1"));
    assert_eq!(read(&leader, "other").await.as_deref(), Some("x"));
}

/// **A replica that does not fence caches keeps a cache shard on the lease**,
/// and nobody is sent a fence: a mixed fleet behaves as before.
#[tokio::test]
async fn a_replica_without_the_cache_fence_keeps_a_cache_shard_on_the_lease() {
    let mut replicas = cache_replicas();
    replicas.get_mut("broker-c").capabilities = REQUIRED;
    let (leader, _dir) = cache_leader().await;

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key_of(ShardKind::Cache),
        &route_of(ShardKind::Cache),
        true,
    )
    .await;

    assert_eq!(
        outcome,
        Outcome::Lease {
            lacking: "broker-c".to_string()
        }
    );
    assert!(replicas.sent(Kind::Fence).is_empty());
}

/// **Once caches acknowledge by their followers, a cache shard never opens on
/// the lease**: with no majority it stays closed.
#[tokio::test]
async fn without_the_lease_fallback_a_cache_shard_waits_for_a_majority() {
    let mut replicas = cache_replicas();
    replicas.get_mut("broker-b").reachable = false;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = cache_leader().await;

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key_of(ShardKind::Cache),
        &route_of(ShardKind::Cache),
        false,
    )
    .await;

    assert!(matches!(outcome, Outcome::Pending { .. }), "{outcome:?}");
}

/// Leave `store`'s root unusable, so the next shard it opens fails as an
/// EMFILE or a failed recovery would.
fn break_store(dir: &TempDir, store: &str) {
    let root = dir.path().join(store);
    std::fs::remove_dir_all(&root).expect("remove the store's root");
    std::fs::write(&root, b"not a directory").expect("block the store's root");
}

/// **A cache log this broker cannot open keeps the shard closed once caches
/// acknowledge by their followers.** Opening it on the lease would serve
/// unfenced while the old leader still acknowledges through its followers.
#[tokio::test]
async fn a_cache_log_that_fails_to_open_keeps_the_shard_closed() {
    let replicas = cache_replicas();
    let (leader, dir) = cache_leader().await;
    break_store(&dir, "caches");

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key_of(ShardKind::Cache),
        &route_of(ShardKind::Cache),
        false,
    )
    .await;

    assert!(matches!(outcome, Outcome::Pending { .. }), "{outcome:?}");
}

/// With the lease still in force, the same failure opens on the lease, as a
/// mixed fleet does.
#[tokio::test]
async fn a_cache_log_that_fails_to_open_falls_back_to_the_lease() {
    let replicas = cache_replicas();
    let (leader, dir) = cache_leader().await;
    break_store(&dir, "caches");

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key_of(ShardKind::Cache),
        &route_of(ShardKind::Cache),
        true,
    )
    .await;

    assert!(matches!(outcome, Outcome::Lease { .. }), "{outcome:?}");
}

/// **A counter log this broker cannot open keeps the shard closed too**,
/// rather than opening with the cache log fenced and the counters not.
#[tokio::test]
async fn a_counter_log_that_fails_to_open_keeps_the_shard_closed() {
    let replicas = cache_replicas();
    let (leader, dir) = cache_leader().await;
    break_store(&dir, "counters");

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key_of(ShardKind::Cache),
        &route_of(ShardKind::Cache),
        false,
    )
    .await;

    assert!(matches!(outcome, Outcome::Pending { .. }), "{outcome:?}");
}

/// A cache kept in memory has no log to fence, which is not a failure: it
/// still opens on the lease.
#[tokio::test]
async fn a_cache_kept_in_memory_opens_on_the_lease() {
    let replicas = cache_replicas();
    let leader = Arc::new(Broker::new(Box::new(felix_storage::EphemeralCache::new())));

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key_of(ShardKind::Cache),
        &route_of(ShardKind::Cache),
        false,
    )
    .await;

    assert!(matches!(outcome, Outcome::Lease { .. }), "{outcome:?}");
}
