//! MQTT mounts every backend must satisfy: a prefix belongs to one stream of
//! a tenant, and no two prefixes of a tenant overlap, however the writes race.
use std::sync::Arc;

use crate::model::mount;
use crate::model::{
    ConsistencyLevel, DeliveryGuarantee, Namespace, RetentionPolicy, Stream, StreamKey, StreamKind,
    StreamPatchRequest, Tenant,
};
use crate::store::{ControlPlaneStore, StoreError};

const TENANT: &str = "mount-t";
const OTHER_TENANT: &str = "mount-other";
const NAMESPACES: [&str; 2] = ["floor", "yard"];

pub(crate) async fn run_mount_contract(store: Arc<dyn ControlPlaneStore>) {
    for tenant in [TENANT, OTHER_TENANT] {
        store
            .create_tenant(Tenant {
                tenant_id: tenant.to_string(),
                display_name: tenant.to_string(),
            })
            .await
            .expect("create tenant");
        for namespace in NAMESPACES {
            store
                .create_namespace(Namespace {
                    tenant_id: tenant.to_string(),
                    namespace: namespace.to_string(),
                    display_name: namespace.to_string(),
                })
                .await
                .expect("create namespace");
        }
    }
    a_mount_is_stored_and_travels_with_the_stream(store.as_ref()).await;
    overlapping_mounts_of_a_tenant_are_refused(store.as_ref()).await;
    a_retried_create_is_not_taken_for_an_overlap(store.as_ref()).await;
    only_a_durable_stream_can_be_mounted(store.as_ref()).await;
    deleting_a_stream_frees_its_mount(store.as_ref()).await;
    a_patch_sets_changes_and_clears_a_mount(store.as_ref()).await;
    a_batch_is_refused_whole_for_an_overlap(store.as_ref()).await;
}

/// Overlapping mount writes at once, whether two creates, a create and a
/// patch, or two patches: exactly one wins.
pub(crate) async fn run_mount_race_contract(store: Arc<dyn ControlPlaneStore>, rounds: usize) {
    for round in 0..rounds {
        let tenant = format!("race-t{round}");
        store
            .create_tenant(Tenant {
                tenant_id: tenant.clone(),
                display_name: tenant.clone(),
            })
            .await
            .expect("create tenant");
        for namespace in NAMESPACES {
            store
                .create_namespace(Namespace {
                    tenant_id: tenant.clone(),
                    namespace: namespace.to_string(),
                    display_name: namespace.to_string(),
                })
                .await
                .expect("create namespace");
        }
        let spawn = |namespace: &'static str, name: &str, prefix: &str| {
            let store = Arc::clone(&store);
            let mut wanted = stream(name, Some(prefix));
            wanted.tenant_id = tenant.clone();
            wanted.namespace = namespace.to_string();
            tokio::spawn(async move { store.create_stream(wanted).await })
        };
        // One prefix inside the other, in different namespaces.
        let a = spawn(NAMESPACES[0], "wide", "line/");
        let b = spawn(NAMESPACES[1], "narrow", "line/3/");
        let (a, b) = (a.await.expect("task"), b.await.expect("task"));
        match (&a, &b) {
            (Ok(_), Err(err)) | (Err(err), Ok(_)) => {
                assert!(mount::refusal(err).is_some(), "round {round}: {err:?}");
            }
            other => panic!("round {round}: exactly one create must win, got {other:?}"),
        }
        let mut stored = 0;
        for (name, namespace) in [("wide", NAMESPACES[0]), ("narrow", NAMESPACES[1])] {
            let key = StreamKey {
                tenant_id: tenant.clone(),
                namespace: namespace.to_string(),
                stream: name.to_string(),
            };
            stored += usize::from(store.get_stream(&key).await.is_ok());
        }
        assert_eq!(stored, 1, "round {round}: one stream is stored");

        // A create against a patch in another namespace.
        let plain = unmounted(&store, &tenant, NAMESPACES[0], "plain").await;
        let patching = tokio::spawn({
            let store = Arc::clone(&store);
            let plain = plain.clone();
            async move { store.patch_stream(&plain, mount_patch("belt/")).await }
        });
        let mut other = stream("other", Some("belt/2/"));
        other.tenant_id = tenant.clone();
        other.namespace = NAMESPACES[1].to_string();
        let creating = tokio::spawn({
            let store = Arc::clone(&store);
            async move { store.create_stream(other).await }
        });
        let (patched, created) = (patching.await.expect("task"), creating.await.expect("task"));
        exactly_one_wins(round, "create against patch", &patched, &created);

        // Two patches of unmounted streams in different namespaces.
        let first = unmounted(&store, &tenant, NAMESPACES[0], "first").await;
        let second = unmounted(&store, &tenant, NAMESPACES[1], "second").await;
        let patch = |target: StreamKey, prefix: &'static str| {
            let store = Arc::clone(&store);
            tokio::spawn(async move { store.patch_stream(&target, mount_patch(prefix)).await })
        };
        let a = patch(first, "gate/");
        let b = patch(second, "gate/1/");
        let (a, b) = (a.await.expect("task"), b.await.expect("task"));
        exactly_one_wins(round, "two patches", &a, &b);
    }
}

fn exactly_one_wins<T: std::fmt::Debug>(
    round: usize,
    what: &str,
    a: &Result<T, StoreError>,
    b: &Result<T, StoreError>,
) {
    match (a, b) {
        (Ok(_), Err(err)) | (Err(err), Ok(_)) => {
            assert!(
                mount::refusal(err).is_some(),
                "round {round}, {what}: {err:?}"
            );
        }
        other => panic!("round {round}, {what}: exactly one must win, got {other:?}"),
    }
}

async fn unmounted(
    store: &Arc<dyn ControlPlaneStore>,
    tenant: &str,
    namespace: &str,
    name: &str,
) -> StreamKey {
    let mut plain = stream(name, None);
    plain.tenant_id = tenant.to_string();
    plain.namespace = namespace.to_string();
    store.create_stream(plain).await.expect("create unmounted");
    StreamKey {
        tenant_id: tenant.to_string(),
        namespace: namespace.to_string(),
        stream: name.to_string(),
    }
}

fn mount_patch(prefix: &str) -> StreamPatchRequest {
    StreamPatchRequest {
        retention: None,
        consistency: None,
        delivery: None,
        durable: None,
        mount: Some(Some(prefix.to_string())),
    }
}

async fn a_mount_is_stored_and_travels_with_the_stream(store: &dyn ControlPlaneStore) {
    let before = store.stream_snapshot().await.unwrap().next_seq;
    let created = store
        .create_stream(stream("plant", Some("plant/")))
        .await
        .expect("create");
    assert_eq!(created.mount.as_deref(), Some("plant/"));
    let read = store
        .get_stream(&key(NAMESPACES[0], "plant"))
        .await
        .unwrap();
    assert_eq!(read.mount.as_deref(), Some("plant/"));
    let changes = store.stream_changes(before).await.unwrap();
    let change = changes
        .items
        .iter()
        .find(|c| c.key.stream == "plant" && c.key.tenant_id == TENANT)
        .expect("a change");
    assert_eq!(
        change.stream.as_ref().and_then(|s| s.mount.as_deref()),
        Some("plant/")
    );
    let plain = store
        .create_stream(stream("plain", None))
        .await
        .expect("create");
    assert_eq!(plain.mount, None);
}

async fn overlapping_mounts_of_a_tenant_are_refused(store: &dyn ControlPlaneStore) {
    // "plant/" is mounted on floor/plant.
    for (name, prefix) in [
        ("same", "plant/"),
        ("longer", "plant/line1/"),
        ("shorter", "plan"),
        ("bare", "plant"),
    ] {
        for namespace in NAMESPACES {
            let mut wanted = stream(name, Some(prefix));
            wanted.namespace = namespace.to_string();
            let err = store
                .create_stream(wanted)
                .await
                .expect_err("an overlap is refused");
            let message = mount::refusal(&err).unwrap_or_else(|| panic!("{prefix}: {err:?}"));
            assert!(
                message.contains("floor/plant"),
                "names the other: {message}"
            );
            assert!(
                store.get_stream(&key(namespace, name)).await.is_err(),
                "{prefix}: nothing stored"
            );
        }
    }
    // Neighbours that share no prefix, and another tenant's mounts, are free.
    store
        .create_stream(stream("plants", Some("plants/")))
        .await
        .expect("a sibling prefix");
    let mut elsewhere = stream("plant", Some("plant/"));
    elsewhere.tenant_id = OTHER_TENANT.to_string();
    store
        .create_stream(elsewhere)
        .await
        .expect("another tenant may use the same prefix");
}

async fn a_retried_create_is_not_taken_for_an_overlap(store: &dyn ControlPlaneStore) {
    let err = store
        .create_stream(stream("plant", Some("plant/")))
        .await
        .expect_err("it exists");
    assert!(matches!(err, StoreError::Conflict(_)), "{err:?}");
    assert!(mount::refusal(&err).is_none(), "{err:?}");
}

async fn only_a_durable_stream_can_be_mounted(store: &dyn ControlPlaneStore) {
    let mut wanted = stream("volatile", Some("volatile/"));
    wanted.durable = false;
    let err = store.create_stream(wanted).await.expect_err("refused");
    assert!(mount::refusal(&err).is_some(), "{err:?}");
    assert!(
        store
            .get_stream(&key(NAMESPACES[0], "volatile"))
            .await
            .is_err()
    );
}

async fn deleting_a_stream_frees_its_mount(store: &dyn ControlPlaneStore) {
    store
        .create_stream(stream("temp", Some("temp/")))
        .await
        .expect("create");
    store
        .delete_stream(&key(NAMESPACES[0], "temp"))
        .await
        .expect("delete");
    let mut again = stream("temp2", Some("temp/sub/"));
    again.namespace = NAMESPACES[1].to_string();
    store
        .create_stream(again)
        .await
        .expect("the prefix is free");
}

async fn a_patch_sets_changes_and_clears_a_mount(store: &dyn ControlPlaneStore) {
    let patch = |mount: Option<Option<&str>>, durable: Option<bool>| StreamPatchRequest {
        retention: None,
        consistency: None,
        delivery: None,
        durable,
        mount: mount.map(|inner| inner.map(str::to_string)),
    };
    let target = key(NAMESPACES[0], "plain");

    // An overlap leaves the stream as it was.
    let err = store
        .patch_stream(&target, patch(Some(Some("plant/x/")), None))
        .await
        .expect_err("overlap");
    assert!(mount::refusal(&err).is_some(), "{err:?}");
    assert_eq!(store.get_stream(&target).await.unwrap().mount, None);

    let set = store
        .patch_stream(&target, patch(Some(Some("dock/")), None))
        .await
        .expect("set");
    assert_eq!(set.mount.as_deref(), Some("dock/"));

    // A stream's own mount is not in its way when it changes.
    let moved = store
        .patch_stream(&target, patch(Some(Some("dock/east/")), None))
        .await
        .expect("change");
    assert_eq!(moved.mount.as_deref(), Some("dock/east/"));

    // Not left alone by an unrelated patch.
    let kept = store
        .patch_stream(&target, patch(None, Some(true)))
        .await
        .expect("unrelated");
    assert_eq!(kept.mount.as_deref(), Some("dock/east/"));

    // A mounted stream stays durable.
    let err = store
        .patch_stream(&target, patch(None, Some(false)))
        .await
        .expect_err("not while mounted");
    assert!(mount::refusal(&err).is_some(), "{err:?}");
    assert!(store.get_stream(&target).await.unwrap().durable);

    // Clearing the mount and the durability together is fine.
    let cleared = store
        .patch_stream(&target, patch(Some(None), Some(false)))
        .await
        .expect("clear");
    assert_eq!(cleared.mount, None);
    assert!(!cleared.durable);
    assert_eq!(store.get_stream(&target).await.unwrap().mount, None);

    // Its old prefix is free for another stream.
    let mut taker = stream("taker", Some("dock/east/"));
    taker.namespace = NAMESPACES[1].to_string();
    store
        .create_stream(taker)
        .await
        .expect("freed by the clear");
}

async fn a_batch_is_refused_whole_for_an_overlap(store: &dyn ControlPlaneStore) {
    let against_stored = store
        .create_resources(
            vec![
                stream("b-ok", Some("batch/ok/")),
                stream("b-bad", Some("plants/x/")),
            ],
            vec![],
        )
        .await
        .expect_err("overlaps a stored mount");
    assert!(
        mount::refusal(&against_stored).is_some(),
        "{against_stored:?}"
    );
    assert!(store.get_stream(&key(NAMESPACES[0], "b-ok")).await.is_err());

    let within = store
        .create_resources(
            vec![
                stream("w1", Some("within/")),
                stream("w2", Some("within/x/")),
            ],
            vec![],
        )
        .await
        .expect_err("overlaps itself");
    assert!(mount::refusal(&within).is_some(), "{within:?}");
    assert!(store.get_stream(&key(NAMESPACES[0], "w1")).await.is_err());

    store
        .create_resources(
            vec![stream("g1", Some("good/a/")), stream("g2", Some("good/b/"))],
            vec![],
        )
        .await
        .expect("disjoint mounts");
}

fn key(namespace: &str, name: &str) -> StreamKey {
    StreamKey {
        tenant_id: TENANT.to_string(),
        namespace: namespace.to_string(),
        stream: name.to_string(),
    }
}

fn stream(name: &str, prefix: Option<&str>) -> Stream {
    Stream {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACES[0].to_string(),
        stream: name.to_string(),
        kind: StreamKind::Stream,
        shards: 2,
        replication_factor: 1,
        retention: RetentionPolicy {
            max_age_seconds: None,
            max_size_bytes: None,
        },
        consistency: ConsistencyLevel::Leader,
        delivery: DeliveryGuarantee::AtLeastOnce,
        durable: true,
        region: None,
        mount: prefix.map(str::to_string),
        routing: Default::default(),
        incarnation: 0,
    }
}
