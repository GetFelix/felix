//! Change feeds every backend must serve the same way.
//!
//! A consumer seeds from a snapshot and then follows `next_seq` page by page.
//! Each step here is a way that can silently lose a change.
use std::collections::BTreeSet;
use std::sync::Arc;

use super::nodes::node;
use crate::model::{
    Cache, ConsistencyLevel, DeliveryGuarantee, Namespace, RetentionPolicy, ShardAssignment,
    ShardKey, ShardKind, ShardState, Stream, StreamKind, Tenant,
};
use crate::store::{ChangeSet, ControlPlaneAuthStore, StoreResult};

const TENANT: &str = "feedtenant";
const NAMESPACE: &str = "feedns";
const STREAM: &str = "feedstream";

/// `limit` must be the store's `changes_limit`.
pub(crate) async fn run_change_feed_contract(store: Arc<dyn ControlPlaneAuthStore>, limit: usize) {
    every_feed_is_followed_past_a_full_page(store.as_ref(), limit).await;
}

/// More changes than fit on a page, followed from a snapshot's `next_seq`,
/// all arrive. A full page whose `next_seq` is the feed's tail rather than the
/// seq after its last change sends the consumer past everything after it.
async fn every_feed_is_followed_past_a_full_page(store: &dyn ControlPlaneAuthStore, limit: usize) {
    let count = limit * 2 + 1;
    let names =
        |prefix: &str| -> Vec<String> { (0..count).map(|i| format!("{prefix}{i:03}")).collect() };

    let tenants = names("feedt");
    let since = store.tenant_snapshot().await.expect("snapshot").next_seq;
    for tenant_id in &tenants {
        store
            .create_tenant(tenant(tenant_id))
            .await
            .expect("tenant");
    }
    store.create_tenant(tenant(TENANT)).await.expect("tenant");
    let seen = follow(
        limit,
        since,
        |since| store.tenant_changes(since),
        |c| c.tenant_id.clone(),
    )
    .await;
    assert_all_seen("tenant", &tenants, &seen);

    let namespaces = names("feedn");
    let since = store.namespace_snapshot().await.expect("snapshot").next_seq;
    for name in namespaces.iter().map(String::as_str).chain([NAMESPACE]) {
        store
            .create_namespace(Namespace {
                tenant_id: TENANT.to_string(),
                namespace: name.to_string(),
                display_name: name.to_string(),
            })
            .await
            .expect("namespace");
    }
    let seen = follow(
        limit,
        since,
        |since| store.namespace_changes(since),
        |c| c.key.namespace.clone(),
    )
    .await;
    assert_all_seen("namespace", &namespaces, &seen);

    let streams = names("feeds");
    let since = store.stream_snapshot().await.expect("snapshot").next_seq;
    for name in &streams {
        store.create_stream(stream(name, 1)).await.expect("stream");
    }
    let seen = follow(
        limit,
        since,
        |since| store.stream_changes(since),
        |c| c.key.stream.clone(),
    )
    .await;
    assert_all_seen("stream", &streams, &seen);

    let caches = names("feedc");
    let since = store.cache_snapshot().await.expect("snapshot").next_seq;
    for name in &caches {
        store.create_cache(cache(name)).await.expect("cache");
    }
    let seen = follow(
        limit,
        since,
        |since| store.cache_changes(since),
        |c| c.key.cache.clone(),
    )
    .await;
    assert_all_seen("cache", &caches, &seen);

    let nodes = names("feednode");
    let since = store.node_snapshot().await.expect("snapshot").next_seq;
    for (i, node_id) in nodes.iter().enumerate() {
        store
            .register_node(node(node_id, 7600 + i as u16))
            .await
            .expect("node");
    }
    let seen = follow(
        limit,
        since,
        |since| store.node_changes(since),
        |c| c.node_id.clone(),
    )
    .await;
    assert_all_seen("node", &nodes, &seen);

    store
        .create_stream(stream(STREAM, count as u32))
        .await
        .expect("sharded stream");
    let shards: Vec<String> = (0..count).map(|shard| shard.to_string()).collect();
    let since = store
        .shard_assignment_snapshot()
        .await
        .expect("snapshot")
        .next_seq;
    for shard in 0..count as u32 {
        store
            .put_shard_assignment(assignment(shard, &nodes[0]))
            .await
            .expect("assignment");
    }
    let seen = follow(
        limit,
        since,
        |since| store.shard_assignment_changes(since),
        |c| c.key.shard.to_string(),
    )
    .await;
    assert_all_seen("shard assignment", &shards, &seen);

    for shard in 0..count as u32 {
        store
            .delete_shard_assignment(&assignment(shard, &nodes[0]).key)
            .await
            .expect("delete assignment");
    }
    for tenant_id in tenants.iter().map(String::as_str).chain([TENANT]) {
        store.delete_tenant(tenant_id).await.expect("delete tenant");
    }
    for node_id in &nodes {
        store.delete_node(node_id).await.expect("delete node");
    }
}

/// Follow a feed from `since` the way a broker does, until a page comes back
/// empty, and return the key of every change on the way.
async fn follow<T, Fetch, Fut>(
    limit: usize,
    mut since: u64,
    mut fetch: Fetch,
    key: impl Fn(&T) -> String,
) -> Vec<String>
where
    Fetch: FnMut(u64) -> Fut,
    Fut: Future<Output = StoreResult<ChangeSet<T>>>,
{
    let mut seen = Vec::new();
    for _ in 0..1_000 {
        let page = fetch(since).await.expect("changes");
        assert!(page.items.len() <= limit, "a page is at most `limit` long");
        if page.items.is_empty() {
            return seen;
        }
        seen.extend(page.items.iter().map(&key));
        since = page.next_seq;
    }
    panic!("the feed never ran dry");
}

fn assert_all_seen(feed: &str, written: &[String], seen: &[String]) {
    let seen: BTreeSet<&String> = seen.iter().collect();
    let missed: Vec<&String> = written.iter().filter(|key| !seen.contains(key)).collect();
    assert!(
        missed.is_empty(),
        "following the {feed} feed page by page missed {missed:?}",
    );
}

fn tenant(tenant_id: &str) -> Tenant {
    Tenant {
        tenant_id: tenant_id.to_string(),
        display_name: tenant_id.to_string(),
    }
}

fn stream(name: &str, shards: u32) -> Stream {
    Stream {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: name.to_string(),
        kind: StreamKind::Stream,
        shards,
        replication_factor: 1,
        retention: RetentionPolicy {
            max_age_seconds: None,
            max_size_bytes: None,
        },
        consistency: ConsistencyLevel::Leader,
        delivery: DeliveryGuarantee::AtMostOnce,
        durable: true,
        region: None,
        routing: Default::default(),
        incarnation: 0,
    }
}

fn cache(name: &str) -> Cache {
    Cache {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        cache: name.to_string(),
        display_name: name.to_string(),
        shards: 1,
        replication_factor: 1,
        consistency: ConsistencyLevel::Leader,
    }
}

fn assignment(shard: u32, leader: &str) -> ShardAssignment {
    ShardAssignment {
        key: ShardKey {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: STREAM.to_string(),
            shard,
            kind: ShardKind::Stream,
        },
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation: 0,
        state: ShardState::Assigning,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
        incarnation: 0,
    }
}
