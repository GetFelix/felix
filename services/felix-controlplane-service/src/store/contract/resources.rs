//! Batch creates every backend must satisfy: all of a batch, or none of it.
use std::sync::Arc;

use crate::model::{
    Cache, CacheKey, ConsistencyLevel, DeliveryGuarantee, Namespace, RetentionPolicy, Stream,
    StreamKey, StreamKind, Tenant,
};
use crate::store::{ControlPlaneStore, StoreError};

const TENANT: &str = "batch-t";
const NAMESPACE: &str = "room";

pub(crate) async fn run_resources_contract(store: Arc<dyn ControlPlaneStore>) {
    store
        .create_tenant(Tenant {
            tenant_id: TENANT.to_string(),
            display_name: "Batch".to_string(),
        })
        .await
        .expect("create tenant");
    store
        .create_namespace(Namespace {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            display_name: "Room".to_string(),
        })
        .await
        .expect("create namespace");
    creates_every_item_with_a_change_each(store.as_ref()).await;
    an_existing_item_fails_the_whole_batch(store.as_ref()).await;
    a_name_given_twice_fails_the_whole_batch(store.as_ref()).await;
    a_missing_namespace_fails_the_whole_batch(store.as_ref()).await;
}

/// A batch racing single creates of one of its names: whichever wins, the
/// batch's other items exist together or not at all.
pub(crate) async fn run_resources_race_contract(store: Arc<dyn ControlPlaneStore>, rounds: usize) {
    for round in 0..rounds {
        let names = [
            format!("race{round}a"),
            format!("race{round}b"),
            format!("race{round}c"),
        ];
        let batch = {
            let store = Arc::clone(&store);
            let streams = names.iter().map(|name| stream(name)).collect::<Vec<_>>();
            let caches = vec![cache(&names[0])];
            tokio::spawn(async move { store.create_resources(streams, caches).await })
        };
        let single = {
            let store = Arc::clone(&store);
            let contested = stream(&names[1]);
            tokio::spawn(async move { store.create_stream(contested).await })
        };
        let batch = batch.await.expect("batch task");
        let single = single.await.expect("single task");

        let a = exists(store.as_ref(), &names[0]).await;
        let c = exists(store.as_ref(), &names[2]).await;
        let cache_a = store.get_cache(&cache_key(&names[0])).await.is_ok();
        match (&batch, &single) {
            (Ok(()), Err(StoreError::Conflict(_))) => {
                assert!(
                    a && c && cache_a,
                    "round {round}: the batch won but is partial"
                );
            }
            (Err(StoreError::Conflict(_)), Ok(_)) => {
                assert!(
                    !a && !c && !cache_a,
                    "round {round}: the batch lost but left items behind"
                );
            }
            other => panic!("round {round}: exactly one create must win, got {other:?}"),
        }
    }
}

async fn creates_every_item_with_a_change_each(store: &dyn ControlPlaneStore) {
    let streams_before = store.stream_snapshot().await.unwrap().next_seq;
    let caches_before = store.cache_snapshot().await.unwrap().next_seq;
    store
        .create_resources(
            vec![stream("chat"), stream("presence")],
            vec![cache("cursors"), cache("board")],
        )
        .await
        .expect("batch");
    for name in ["chat", "presence"] {
        assert_eq!(
            store.get_stream(&stream_key(name)).await.unwrap(),
            stream(name)
        );
    }
    for name in ["cursors", "board"] {
        assert_eq!(
            store.get_cache(&cache_key(name)).await.unwrap(),
            cache(name)
        );
    }
    let streams = store.stream_changes(streams_before).await.unwrap();
    let caches = store.cache_changes(caches_before).await.unwrap();
    let ours = |tenant: &str| tenant == TENANT;
    assert_eq!(
        streams
            .items
            .iter()
            .filter(|c| ours(&c.key.tenant_id))
            .count(),
        2,
        "one stream change per item"
    );
    assert_eq!(
        caches
            .items
            .iter()
            .filter(|c| ours(&c.key.tenant_id))
            .count(),
        2,
        "one cache change per item"
    );
}

async fn an_existing_item_fails_the_whole_batch(store: &dyn ControlPlaneStore) {
    store.create_cache(cache("taken")).await.expect("cache");
    let err = store
        .create_resources(
            vec![stream("left-out-1")],
            vec![cache("left-out-2"), cache("taken")],
        )
        .await
        .expect_err("conflict");
    assert!(matches!(err, StoreError::Conflict(_)), "{err:?}");
    assert!(!exists(store, "left-out-1").await);
    assert!(store.get_cache(&cache_key("left-out-2")).await.is_err());

    let err = store
        .create_resources(vec![stream("left-out-3"), stream("chat")], vec![])
        .await
        .expect_err("conflict");
    assert!(matches!(err, StoreError::Conflict(_)), "{err:?}");
    assert!(!exists(store, "left-out-3").await);
}

async fn a_name_given_twice_fails_the_whole_batch(store: &dyn ControlPlaneStore) {
    let err = store
        .create_resources(
            vec![stream("twice"), stream("once"), stream("twice")],
            vec![],
        )
        .await
        .expect_err("conflict");
    assert!(matches!(err, StoreError::Conflict(_)), "{err:?}");
    assert!(!exists(store, "twice").await);
    assert!(!exists(store, "once").await);
}

async fn a_missing_namespace_fails_the_whole_batch(store: &dyn ControlPlaneStore) {
    let mut elsewhere = cache("orphan");
    elsewhere.namespace = "missing".to_string();
    let err = store
        .create_resources(vec![stream("homed")], vec![elsewhere])
        .await
        .expect_err("not found");
    assert!(matches!(err, StoreError::NotFound(_)), "{err:?}");
    assert!(!exists(store, "homed").await);
}

async fn exists(store: &dyn ControlPlaneStore, name: &str) -> bool {
    store.get_stream(&stream_key(name)).await.is_ok()
}

fn stream(name: &str) -> Stream {
    Stream {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: name.to_string(),
        kind: StreamKind::Stream,
        shards: 2,
        replication_factor: 1,
        retention: RetentionPolicy {
            max_age_seconds: Some(60),
            max_size_bytes: None,
        },
        consistency: ConsistencyLevel::Leader,
        delivery: DeliveryGuarantee::AtLeastOnce,
        durable: true,
        region: None,
        routing: Default::default(),
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

fn stream_key(name: &str) -> StreamKey {
    StreamKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: name.to_string(),
    }
}

fn cache_key(name: &str) -> CacheKey {
    CacheKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        cache: name.to_string(),
    }
}
