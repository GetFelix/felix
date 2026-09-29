//! A stream created after the fleet finalizes `jump_hash_routing` maps keys to
//! shards with jump consistent hashing, on every broker and in the client. A
//! stream created before keeps modulo.
//!
//! Run with `cargo test -p felix-cluster --test routing jump_hash::`.
use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_controlplane_service::store::ControlPlaneStore;
use felix_wire::AckMode;
use felix_wire::routing::{ShardRouting, shard_for_routing};
use serial_test::serial;

const LEGACY: &str = "legacy";
const FRESH: &str = "fresh";
const SHARDS: u32 = 8;

fn keys() -> Vec<String> {
    (0..48).map(|i| format!("customer-{i}")).collect()
}

/// Create `FRESH` through the API, as an operator would, after finalizing.
async fn create_fresh(cluster: &Cluster) -> serde_json::Value {
    let response = reqwest::Client::new()
        .post(format!(
            "{}/v1/tenants/{}/namespaces/{}/streams",
            cluster.control_plane_url(),
            cluster.tenant_id,
            cluster.namespace
        ))
        .bearer_auth(&cluster.admin_token)
        .json(&serde_json::json!({
            "stream": FRESH,
            "kind": "Stream",
            "shards": SHARDS,
            "replication_factor": 1,
            "retention": { "max_age_seconds": null, "max_size_bytes": null },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": true,
        }))
        .send()
        .await
        .expect("create stream");
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    response.json().await.expect("stream body")
}

/// Read every shard of `stream` from its owner until each holds the payloads
/// `expected` puts there. A record on the wrong shard leaves its shard short,
/// so this fails rather than passing on a partial read.
async fn assert_shards_hold(
    cluster: &Cluster,
    stream: &str,
    expected: &HashMap<u32, BTreeSet<String>>,
) {
    let owners = cluster.shard_owners_for(stream).await.expect("owners");
    for shard in 0..SHARDS {
        let want = expected.get(&shard).cloned().unwrap_or_default();
        let owner = owners.get(&shard).expect("every shard placed");
        let (_client, mut subscription) = cluster
            .replay_shard(owner, stream, shard)
            .await
            .expect("replay shard");
        let mut seen = BTreeSet::new();
        let deadline =
            tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(20));
        while !want.is_subset(&seen) {
            match tokio::time::timeout_at(deadline, subscription.next_event()).await {
                Ok(Ok(Some(event))) => {
                    let payload = String::from_utf8_lossy(&event.payload).to_string();
                    // The harness probes streams at startup; only ours count.
                    if payload.starts_with("rec/") {
                        seen.insert(payload);
                    }
                }
                other => panic!(
                    "{stream} shard {shard}: read ended ({:?}) holding {seen:?}, want {want:?}",
                    other.map(|r| r.map(|e| e.is_some()))
                ),
            }
        }
        assert_eq!(
            seen, want,
            "{stream} shard {shard} holds records of other shards"
        );
    }
}

/// **Every broker, and the client, route a jump-hash stream alike.** Each key
/// is published through a different broker in turn, so a broker that resolved
/// keys differently would put some on a shard the others would not. The
/// legacy stream, created before finalizing, keeps modulo throughout.
#[serial]
#[tokio::test]
async fn a_jump_hash_stream_routes_alike_everywhere_and_a_legacy_one_is_unaffected() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new(LEGACY, SHARDS)],
        ..Default::default()
    })
    .await
    .expect("start cluster");

    cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store
        .finalize_fleet_feature(felix_common::fleet::JUMP_HASH_ROUTING.name())
        .await
        .expect("finalize jump_hash_routing");
    let created = create_fresh(&cluster).await;
    assert_eq!(created["routing"], "jump_hash");
    cluster.place_shards().await;

    // Every broker describes each stream by its own mapping.
    for node_id in cluster.node_ids() {
        for (stream, routing) in [
            (LEGACY, ShardRouting::Modulo),
            (FRESH, ShardRouting::JumpHash),
        ] {
            felix_cluster::wait::until(
                Duration::from_secs(30),
                "the broker to learn the stream's placement",
                || async {
                    let Ok(client) = cluster.client_on(&node_id).await else {
                        return false;
                    };
                    client
                        .stream_routing(&cluster.tenant_id, &cluster.namespace, stream)
                        .await
                        .is_ok_and(|answer| answer == (SHARDS, routing))
                },
            )
            .await
            .unwrap_or_else(|err| panic!("{node_id} on {stream}: {err}"));
        }
    }

    let nodes = cluster.node_ids();
    let mut expected: HashMap<&str, HashMap<u32, BTreeSet<String>>> = HashMap::new();
    for (stream, routing) in [
        (LEGACY, ShardRouting::Modulo),
        (FRESH, ShardRouting::JumpHash),
    ] {
        for (i, key) in keys().iter().enumerate() {
            let payload = format!("rec/{stream}/{key}");
            cluster
                .publish_keyed_via_settled(
                    &nodes[i % nodes.len()],
                    stream,
                    key.as_bytes(),
                    payload.clone().into_bytes(),
                    Duration::from_secs(30),
                )
                .await
                .expect("publish");
            let shard = shard_for_routing(routing, SHARDS, Some(key.as_bytes()));
            expected
                .entry(stream)
                .or_default()
                .entry(shard)
                .or_default()
                .insert(payload);
        }
    }
    let differ = keys()
        .iter()
        .filter(|key| {
            let key = Some(key.as_bytes());
            shard_for_routing(ShardRouting::Modulo, SHARDS, key)
                != shard_for_routing(ShardRouting::JumpHash, SHARDS, key)
        })
        .count();
    assert!(differ > 0, "the keys must tell the two mappings apart");
    assert_shards_hold(&cluster, LEGACY, &expected[LEGACY]).await;
    assert_shards_hold(&cluster, FRESH, &expected[FRESH]).await;

    // The client keys its owner cache by the shard it computes. Computed with
    // the wrong mapping, keys of different shards share an entry and keep
    // being forwarded after every owner has been seen.
    let client = felix_cluster::client::connect_cluster(
        &cluster.broker_addrs(),
        &cluster.tenant_id,
        &cluster.client_token,
    )
    .await
    .expect("connect a cluster client");
    for stream in [LEGACY, FRESH] {
        let publish = async |key: &str, pass: &str| {
            client
                .publish_keyed(
                    &cluster.tenant_id,
                    &cluster.namespace,
                    stream,
                    format!("{pass}/{key}").into_bytes(),
                    bytes::Bytes::from(key.to_string().into_bytes()),
                    AckMode::PerMessage,
                )
                .await
        };
        for key in keys() {
            publish(&key, "warm").await.expect("warm");
        }
        let after_warm = felix_client::publishes_forwarded();
        for key in keys() {
            publish(&key, "hot").await.expect("hot");
        }
        assert_eq!(
            felix_client::publishes_forwarded(),
            after_warm,
            "{stream}: keyed publishes were still forwarded after every shard owner was learned"
        );
    }

    cluster.shutdown().await;
}
