//! Paged listings every backend must satisfy.
//!
//! Tenants, nodes and assignments are global, and other contracts share the
//! store, so those are checked against the unpaged listing rather than
//! against what this suite created. Only the suite's own names are compared
//! in order: Postgres orders by its collation, so the order of arbitrary
//! names is the backend's business, but plain lowercase ones sort alike
//! everywhere.
use std::collections::BTreeSet;
use std::sync::Arc;

use super::nodes::node;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::model::{
    Cache, ConsistencyLevel, DeliveryGuarantee, Namespace, RetentionPolicy, ShardAssignment,
    ShardKey, ShardKind, ShardState, Stream, StreamKind, Tenant,
};
use crate::store::{ControlPlaneAuthStore, Page, PageRequest, StoreResult};

const TENANT: &str = "pagetenant";
const NAMESPACE: &str = "pagens";
const NODES: [&str; 5] = [
    "pagenode0",
    "pagenode1",
    "pagenode2",
    "pagenode3",
    "pagenode4",
];
/// A stream and a cache of the same name, so a page boundary can fall
/// between two keys that differ only in kind.
const SHARED_NAME: &str = "pageshared";

pub(crate) async fn run_pagination_contract(store: Arc<dyn ControlPlaneAuthStore>) {
    let store = store.as_ref();
    seed(store).await;

    tenants_page_to_the_same_set(store).await;
    namespaces_page_in_order_and_end_on_a_full_page(store).await;
    a_write_behind_the_cursor_is_neither_repeated_nor_returned(store).await;
    caches_page_in_order(store).await;
    nodes_page_to_the_same_set(store).await;
    shard_assignments_page_to_the_same_set(store).await;
    rbac_rules_page_to_the_same_set(store).await;

    cleanup(store).await;
}

/// Read every page of a listing, checking each page's shape on the way.
async fn drain<T, K, Fetch, Fut>(limit: usize, key: impl Fn(&T) -> K, mut fetch: Fetch) -> Vec<T>
where
    Fetch: FnMut(PageRequest<K>) -> Fut,
    Fut: Future<Output = StoreResult<Page<T>>>,
{
    let mut after = None;
    let mut all = Vec::new();
    loop {
        let page = fetch(PageRequest { after, limit }).await.expect("page");
        assert!(page.items.len() <= limit, "a page is at most `limit` long");
        if page.more {
            assert_eq!(page.items.len(), limit, "only the last page may be short");
        }
        after = page.items.last().map(&key);
        let more = page.more;
        all.extend(page.items);
        if !more {
            return all;
        }
    }
}

fn no_repeats<K: Ord + std::fmt::Debug + Clone>(keys: &[K]) -> BTreeSet<K> {
    let set: BTreeSet<K> = keys.iter().cloned().collect();
    assert_eq!(
        set.len(),
        keys.len(),
        "a paged listing repeated an entry: {keys:?}"
    );
    set
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
    }
}

fn cache(name: &str, shards: u32) -> Cache {
    Cache {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        cache: name.to_string(),
        display_name: name.to_string(),
        shards,
        replication_factor: 1,
        consistency: ConsistencyLevel::Leader,
    }
}

fn assignment(kind: ShardKind, shard: u32, leader: &str) -> ShardAssignment {
    ShardAssignment {
        key: ShardKey {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: SHARED_NAME.to_string(),
            shard,
            kind,
        },
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation: 0,
        state: ShardState::Assigning,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
    }
}

async fn seed(store: &dyn ControlPlaneAuthStore) {
    store
        .create_tenant(Tenant {
            tenant_id: TENANT.to_string(),
            display_name: "Paging".to_string(),
        })
        .await
        .expect("tenant");
    for name in ["pagensa", "pagensb", "pagensc", NAMESPACE] {
        store
            .create_namespace(Namespace {
                tenant_id: TENANT.to_string(),
                namespace: name.to_string(),
                display_name: name.to_string(),
            })
            .await
            .expect("namespace");
    }
    for name in ["pagesb", "pagesc", "pagesd", "pagese", "pagesf"] {
        store.create_stream(stream(name, 1)).await.expect("stream");
    }
    store
        .create_stream(stream(SHARED_NAME, 3))
        .await
        .expect("shared stream");
    for name in ["pagecb", "pagecc", "pagecd"] {
        store.create_cache(cache(name, 1)).await.expect("cache");
    }
    store
        .create_cache(cache(SHARED_NAME, 2))
        .await
        .expect("shared cache");
    for (i, id) in NODES.iter().enumerate() {
        store
            .register_node(node(id, 7700 + i as u16))
            .await
            .expect("node");
    }
    for shard in 0..3 {
        store
            .put_shard_assignment(assignment(
                ShardKind::Stream,
                shard,
                NODES[shard as usize % 2],
            ))
            .await
            .expect("stream assignment");
    }
    for shard in 0..2 {
        store
            .put_shard_assignment(assignment(ShardKind::Cache, shard, NODES[0]))
            .await
            .expect("cache assignment");
    }
}

async fn tenants_page_to_the_same_set(store: &dyn ControlPlaneAuthStore) {
    let expected: BTreeSet<String> = store
        .list_tenants()
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.tenant_id)
        .collect();
    for limit in [1, 2, expected.len() + 5] {
        let paged = drain(
            limit,
            |t: &Tenant| t.tenant_id.clone(),
            |page| store.list_tenants_page(page),
        )
        .await;
        let keys: Vec<String> = paged.into_iter().map(|t| t.tenant_id).collect();
        assert_eq!(no_repeats(&keys), expected, "limit {limit}");
    }
}

async fn namespaces_page_in_order_and_end_on_a_full_page(store: &dyn ControlPlaneAuthStore) {
    // Four namespaces at two per page: the second page is full and last, and
    // must say so rather than send the caller to an empty third.
    let first = store
        .list_namespaces_page(TENANT, PageRequest::first(2))
        .await
        .unwrap();
    assert!(first.more);
    let second = store
        .list_namespaces_page(
            TENANT,
            PageRequest {
                after: Some(first.items[1].namespace.clone()),
                limit: 2,
            },
        )
        .await
        .unwrap();
    assert!(!second.more, "a full last page claimed more");
    let names: Vec<String> = first
        .items
        .into_iter()
        .chain(second.items)
        .map(|ns| ns.namespace)
        .collect();
    assert_eq!(names, ["pagens", "pagensa", "pagensb", "pagensc"]);

    // Another tenant's namespaces are not this one's.
    let empty = store
        .list_namespaces_page("pagenosuchtenant", PageRequest::first(10))
        .await
        .unwrap();
    assert!(empty.items.is_empty() && !empty.more);
}

/// The reason for keyset over offset: a create that sorts before the cursor
/// does not shift the rest of the listing under the caller.
async fn a_write_behind_the_cursor_is_neither_repeated_nor_returned(
    store: &dyn ControlPlaneAuthStore,
) {
    let first = store
        .list_streams_page(TENANT, NAMESPACE, PageRequest::first(2))
        .await
        .unwrap();
    let first_names: Vec<&str> = first.items.iter().map(|s| s.stream.as_str()).collect();
    assert_eq!(first_names, ["pagesb", "pagesc"]);

    store
        .create_stream(stream("pagesa", 1))
        .await
        .expect("stream behind the cursor");

    let rest = drain(
        2,
        |s: &Stream| s.stream.clone(),
        |mut page| {
            page.after = page.after.or_else(|| Some("pagesc".to_string()));
            store.list_streams_page(TENANT, NAMESPACE, page)
        },
    )
    .await;
    let rest: Vec<String> = rest.into_iter().map(|s| s.stream).collect();
    assert_eq!(rest, ["pagesd", "pagese", "pagesf", SHARED_NAME]);
}

async fn caches_page_in_order(store: &dyn ControlPlaneAuthStore) {
    let paged = drain(
        3,
        |c: &Cache| c.cache.clone(),
        |page| store.list_caches_page(TENANT, NAMESPACE, page),
    )
    .await;
    let names: Vec<String> = paged.into_iter().map(|c| c.cache).collect();
    assert_eq!(names, ["pagecb", "pagecc", "pagecd", SHARED_NAME]);
}

async fn nodes_page_to_the_same_set(store: &dyn ControlPlaneAuthStore) {
    let expected: BTreeSet<String> = store
        .list_nodes()
        .await
        .unwrap()
        .into_iter()
        .map(|n| n.node_id)
        .collect();
    assert!(expected.len() >= NODES.len());
    let paged = drain(
        2,
        |n: &crate::model::Node| n.node_id.clone(),
        |page| store.list_nodes_page(page),
    )
    .await;
    let keys: Vec<String> = paged.into_iter().map(|n| n.node_id).collect();
    assert_eq!(no_repeats(&keys), expected);
}

async fn shard_assignments_page_to_the_same_set(store: &dyn ControlPlaneAuthStore) {
    let expected: BTreeSet<_> = store
        .list_shard_assignments()
        .await
        .unwrap()
        .iter()
        .map(|a| a.key.page_order())
        .collect();
    // A limit of one puts a page boundary between every pair of keys,
    // including the stream and cache shards that share a name.
    for limit in [1, 2, 4] {
        let paged = drain(
            limit,
            |a: &ShardAssignment| a.key.clone(),
            |page| store.list_shard_assignments_page(None, page),
        )
        .await;
        let keys: Vec<_> = paged.iter().map(|a| a.key.page_order()).collect();
        assert_eq!(no_repeats(&keys), expected, "limit {limit}");
    }

    let expected: BTreeSet<_> = store
        .list_shard_assignments_for_node(NODES[0])
        .await
        .unwrap()
        .iter()
        .map(|a| a.key.page_order())
        .collect();
    // Two stream shards and both cache shards.
    assert_eq!(expected.len(), 4);
    let paged = drain(
        1,
        |a: &ShardAssignment| a.key.clone(),
        |page| store.list_shard_assignments_page(Some(NODES[0]), page),
    )
    .await;
    assert!(paged.iter().all(|a| a.leader == NODES[0]));
    let keys: Vec<_> = paged.iter().map(|a| a.key.page_order()).collect();
    assert_eq!(no_repeats(&keys), expected);
}

async fn rbac_rules_page_to_the_same_set(store: &dyn ControlPlaneAuthStore) {
    for (subject, object, action) in [
        ("role:a", "stream:pagetenant/pagens/x", "stream.publish"),
        ("role:a", "stream:pagetenant/pagens/x", "stream.subscribe"),
        ("role:a", "stream:pagetenant/pagens/y", "stream.publish"),
        ("role:b", "stream:pagetenant/pagens/x", "stream.publish"),
        ("role:b", "cache:pagetenant/pagens/c", "cache.read"),
    ] {
        store
            .add_rbac_policy(
                TENANT,
                PolicyRule {
                    subject: subject.to_string(),
                    object: object.to_string(),
                    action: action.to_string(),
                },
            )
            .await
            .expect("policy");
    }
    for (user, role) in [("p:1", "role:a"), ("p:1", "role:b"), ("p:2", "role:a")] {
        store
            .add_rbac_grouping(
                TENANT,
                GroupingRule {
                    user: user.to_string(),
                    role: role.to_string(),
                },
            )
            .await
            .expect("grouping");
    }

    let policy_key = |p: &PolicyRule| (p.subject.clone(), p.object.clone(), p.action.clone());
    let expected: BTreeSet<_> = store
        .list_rbac_policies(TENANT)
        .await
        .unwrap()
        .iter()
        .map(policy_key)
        .collect();
    let paged = drain(2, PolicyRule::clone, |page| {
        store.list_rbac_policies_page(TENANT, page)
    })
    .await;
    let keys: Vec<_> = paged.iter().map(policy_key).collect();
    assert_eq!(no_repeats(&keys), expected);

    let grouping_key = |g: &GroupingRule| (g.user.clone(), g.role.clone());
    let expected: BTreeSet<_> = store
        .list_rbac_groupings(TENANT)
        .await
        .unwrap()
        .iter()
        .map(grouping_key)
        .collect();
    let paged = drain(1, GroupingRule::clone, |page| {
        store.list_rbac_groupings_page(TENANT, page)
    })
    .await;
    let keys: Vec<_> = paged.iter().map(grouping_key).collect();
    assert_eq!(no_repeats(&keys), expected);
}

/// Leave the global listings as they were, for whichever contract runs next.
async fn cleanup(store: &dyn ControlPlaneAuthStore) {
    for assignment in store.list_shard_assignments().await.unwrap() {
        if assignment.key.tenant_id == TENANT {
            store
                .delete_shard_assignment(&assignment.key)
                .await
                .expect("delete assignment");
        }
    }
    // Takes its namespaces, streams, caches and RBAC rules with it.
    store.delete_tenant(TENANT).await.expect("delete tenant");
    for id in NODES {
        store.delete_node(id).await.expect("delete node");
    }
}
