use felix_storage::EphemeralCache;

use super::*;
use crate::broker::metadata::StreamMetadata;

fn key(tenant: &str, namespace: &str, stream: &str) -> StreamKey {
    StreamKey::new(tenant, namespace, stream)
}

fn resolved(mounts: &Mounts, tenant: &str, topic: &str) -> Option<(String, String)> {
    mounts
        .resolve(tenant, topic)
        .map(|m| (m.namespace.to_string(), m.stream.to_string()))
}

fn pair(namespace: &str, stream: &str) -> Option<(String, String)> {
    Some((namespace.to_string(), stream.to_string()))
}

#[test]
fn a_topic_under_a_mount_resolves_to_its_stream() {
    let mounts = Mounts::default();
    mounts.set(&key("t", "ns", "plant"), Some("plant/"));
    assert_eq!(
        resolved(&mounts, "t", "plant/line1/temp"),
        pair("ns", "plant")
    );
    assert_eq!(resolved(&mounts, "t", "plant/"), pair("ns", "plant"));
}

#[test]
fn the_prefix_boundary_is_the_bytes() {
    let mounts = Mounts::default();
    mounts.set(&key("t", "ns", "plant"), Some("plant/"));
    // A topic shorter than the prefix, or one that only shares its start.
    assert_eq!(resolved(&mounts, "t", "plant"), None);
    assert_eq!(resolved(&mounts, "t", "plants/x"), None);
    assert_eq!(resolved(&mounts, "t", "Plant/x"), None);
    // The prefix has to be the start, not just somewhere in the topic.
    assert_eq!(resolved(&mounts, "t", "other/plant/x"), None);
    // A prefix without a trailing slash is a plain byte prefix.
    mounts.set(&key("t", "ns", "bare"), Some("bare"));
    assert_eq!(resolved(&mounts, "t", "barely/x"), pair("ns", "bare"));
}

#[test]
fn a_topic_under_no_mount_resolves_to_nothing() {
    let mounts = Mounts::default();
    assert_eq!(resolved(&mounts, "t", "plant/x"), None);
    mounts.set(&key("t", "ns", "plant"), Some("plant/"));
    assert_eq!(resolved(&mounts, "t", "yard/x"), None);
    assert_eq!(resolved(&mounts, "t", ""), None);
}

#[test]
fn mounts_belong_to_their_tenant() {
    let mounts = Mounts::default();
    mounts.set(&key("a", "ns", "plant"), Some("plant/"));
    mounts.set(&key("b", "other", "mill"), Some("plant/"));
    assert_eq!(resolved(&mounts, "a", "plant/x"), pair("ns", "plant"));
    assert_eq!(resolved(&mounts, "b", "plant/x"), pair("other", "mill"));
    assert_eq!(resolved(&mounts, "c", "plant/x"), None);
}

#[test]
fn the_longest_prefix_wins_if_two_ever_match() {
    let mounts = Mounts::default();
    mounts.set(&key("t", "ns", "wide"), Some("plant/"));
    mounts.set(&key("t", "ns", "narrow"), Some("plant/line1/"));
    assert_eq!(
        resolved(&mounts, "t", "plant/line1/x"),
        pair("ns", "narrow")
    );
    assert_eq!(resolved(&mounts, "t", "plant/line2/x"), pair("ns", "wide"));
}

#[test]
fn a_changed_mount_replaces_the_old_one_and_a_cleared_one_goes() {
    let mounts = Mounts::default();
    let plant = key("t", "ns", "plant");
    mounts.set(&plant, Some("plant/"));
    mounts.set(&plant, Some("works/"));
    assert_eq!(resolved(&mounts, "t", "plant/x"), None);
    assert_eq!(resolved(&mounts, "t", "works/x"), pair("ns", "plant"));
    mounts.set(&plant, None);
    assert_eq!(resolved(&mounts, "t", "works/x"), None);
    assert!(
        mounts.tenants.load().is_empty(),
        "an empty tenant is dropped"
    );
    // Clearing what was never set changes nothing.
    mounts.set(&plant, None);
}

#[test]
fn an_unchanged_mount_does_not_replace_the_table() {
    let mounts = Mounts::default();
    let plant = key("t", "ns", "plant");
    mounts.set(&plant, Some("plant/"));
    let before = mounts.tenants.load_full();
    mounts.set(&plant, Some("plant/"));
    assert!(Arc::ptr_eq(&before, &mounts.tenants.load_full()));
}

#[tokio::test]
async fn the_registry_keeps_the_table_in_step() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t").await.unwrap();
    broker.register_namespace("t", "ns").await.unwrap();
    let mounted = |prefix: Option<&str>| StreamMetadata {
        mount: prefix.map(str::to_string),
        ..StreamMetadata::default()
    };

    broker
        .register_stream("t", "ns", "plant", mounted(Some("plant/")))
        .await
        .unwrap();
    let found = broker.resolve_mount("t", "plant/x").expect("mounted");
    assert_eq!((&*found.namespace, &*found.stream), ("ns", "plant"));

    // A refresh with another mount moves it; with none, clears it.
    broker
        .register_stream("t", "ns", "plant", mounted(Some("works/")))
        .await
        .unwrap();
    assert!(broker.resolve_mount("t", "plant/x").is_none());
    assert!(broker.resolve_mount("t", "works/x").is_some());
    broker
        .register_stream("t", "ns", "plant", mounted(None))
        .await
        .unwrap();
    assert!(broker.resolve_mount("t", "works/x").is_none());

    // Removing the stream frees its prefix.
    broker
        .register_stream("t", "ns", "plant", mounted(Some("plant/")))
        .await
        .unwrap();
    assert!(broker.remove_stream("t", "ns", "plant").await.unwrap());
    assert!(broker.resolve_mount("t", "plant/x").is_none());
}
