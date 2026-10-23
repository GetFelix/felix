//! Batched publishing: a batch is split by shard and each piece goes to that
//! shard's owner, against stub brokers that record the batches they receive.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use bytes::Bytes;
use felix_wire::binary::PublishOwner;
use felix_wire::{AckMode, ErrorCode, Message};

use super::connections::{default_config, policy};
use super::stub_broker::{STREAM_SHARDS, StubBroker};
use crate::cluster::ClusterClient;
use crate::test_support::build_server_config;

fn ok(request_id: u64) -> Message {
    Message::PublishOk {
        request_id,
        offset: None,
    }
}

fn refuse(request_id: u64, code: ErrorCode) -> Message {
    Message::PublishError {
        request_id,
        message: "refused".to_string(),
        code: Some(code),
        retry: None,
        detail: None,
    }
}

fn shard_of(key: &Bytes) -> u32 {
    felix_wire::routing::shard_for(STREAM_SHARDS, Some(key.as_ref()))
}

/// A key that resolves to `shard`; `n` picks among several.
fn key_on(shard: u32, n: usize) -> Bytes {
    (0..10_000)
        .map(|i| Bytes::from(format!("key-{i}")))
        .filter(|key| shard_of(key) == shard)
        .nth(n)
        .expect("a key on the shard")
}

fn record(key: &Bytes, tag: &str) -> (Bytes, Vec<u8>) {
    (key.clone(), tag.as_bytes().to_vec())
}

fn payloads(batch: &(Option<Bytes>, Vec<Bytes>)) -> Vec<String> {
    batch
        .1
        .iter()
        .map(|p| String::from_utf8(p.to_vec()).unwrap())
        .collect()
}

async fn own(cluster: &ClusterClient, shard: u32, owner: &StubBroker, node: &str) {
    cluster
        .remember_owner(
            ("t1".into(), "default".into(), "orders".into(), shard),
            PublishOwner {
                node_id: node.to_string(),
                addr: Some(owner.addr.to_string()),
                generation: 1,
            },
            None,
        )
        .await;
}

/// An entry broker `a` and a second broker `b`, with the given scripts.
async fn pair(
    a_script: impl Fn(u64) -> Message + Send + Sync + 'static,
    b_script: impl Fn(u64) -> Message + Send + Sync + 'static,
) -> Result<(ClusterClient, StubBroker, StubBroker)> {
    let (server_config, cert) = build_server_config()?;
    let a = StubBroker::start_with(server_config.clone(), a_script)?;
    let b = StubBroker::start_with(server_config, b_script)?;
    let cluster =
        ClusterClient::connect_with_policy(&[a.addr], "localhost", default_config(cert)?, policy())
            .await?;
    Ok((cluster, a, b))
}

/// **Each shard's records go to that shard's owner, in the order given, as
/// one batch.** Shard 1 is owned by `b`, shard 2 is not owned by anyone known
/// and goes through the entry broker.
#[tokio::test]
#[serial_test::serial]
async fn records_are_split_by_owner_and_keep_their_order() -> Result<()> {
    let (cluster, a, b) = pair(ok, ok).await?;
    own(&cluster, 1, &b, "b").await;
    let (k1, k1b, k2) = (key_on(1, 0), key_on(1, 1), key_on(2, 0));
    let records = vec![
        record(&k1, "r0"),
        record(&k2, "r1"),
        record(&k1b, "r2"),
        record(&k2, "r3"),
        record(&k1, "r4"),
    ];

    let report = cluster
        .publish_records("t1", "default", "orders", records, AckMode::PerBatch)
        .await;

    assert!(report.is_ok(), "{report:?}");
    let on_b = b.batches();
    assert_eq!(on_b.len(), 1, "one frame for shard 1");
    assert_eq!(payloads(&on_b[0]), ["r0", "r2", "r4"]);
    let on_a = a.batches();
    assert_eq!(on_a.len(), 1, "one frame for shard 2");
    assert_eq!(payloads(&on_a[0]), ["r1", "r3"]);
    let pieces: Vec<(u32, Vec<usize>)> = report
        .shards
        .iter()
        .map(|s| (s.shard, s.records.clone()))
        .collect();
    assert_eq!(pieces, [(1, vec![0, 2, 4]), (2, vec![1, 3])]);
    Ok(())
}

/// **A shard with no known owner goes through the entry broker**, which
/// forwards it; nothing is lost for want of an owner.
#[tokio::test]
#[serial_test::serial]
async fn an_unknown_owner_falls_back_to_the_entry_broker() -> Result<()> {
    let (cluster, a, b) = pair(ok, ok).await?;
    let records = vec![record(&key_on(0, 0), "x"), record(&key_on(3, 0), "y")];
    let report = cluster
        .publish_records("t1", "default", "orders", records, AckMode::PerBatch)
        .await;
    assert!(report.is_ok(), "{report:?}");
    assert_eq!(a.batches().len(), 2);
    assert_eq!(b.batches().len(), 0);
    Ok(())
}

/// **An owner that says it applied nothing is forgotten, and the piece goes
/// once through the entry broker with its order intact.** Ownership moved
/// after the owner was cached.
#[tokio::test]
#[serial_test::serial]
async fn a_moved_owner_sends_the_piece_through_the_entry_broker() -> Result<()> {
    let (cluster, a, b) = pair(ok, |id| refuse(id, ErrorCode::NotLeader)).await?;
    own(&cluster, 1, &b, "b").await;
    let records = vec![
        record(&key_on(1, 0), "r0"),
        record(&key_on(1, 1), "r1"),
        record(&key_on(1, 0), "r2"),
    ];

    let report = cluster
        .publish_records("t1", "default", "orders", records, AckMode::PerBatch)
        .await;

    assert!(report.is_ok(), "{report:?}");
    assert_eq!(b.batches().len(), 1, "the stale owner was tried once");
    let on_a = a.batches();
    assert_eq!(on_a.len(), 1);
    assert_eq!(payloads(&on_a[0]), ["r0", "r1", "r2"]);

    // The stale owner is forgotten: the next batch does not try it again.
    cluster
        .publish_batch_keyed(
            "t1",
            "default",
            "orders",
            key_on(1, 0),
            vec![b"again".to_vec()],
            AckMode::PerBatch,
        )
        .await?;
    assert_eq!(b.batches().len(), 1);
    assert_eq!(a.batches().len(), 2);
    Ok(())
}

/// **A failed piece is reported with the records it held, and does not stop
/// the others.** The outcome of shard 1's piece is unknown, so it is not
/// sent again.
#[tokio::test]
#[serial_test::serial]
async fn a_failed_piece_is_reported_per_record() -> Result<()> {
    let (cluster, a, b) = pair(ok, |id| refuse(id, ErrorCode::Internal)).await?;
    own(&cluster, 1, &b, "b").await;
    let records = vec![
        record(&key_on(1, 0), "r0"),
        record(&key_on(2, 0), "r1"),
        record(&key_on(1, 0), "r2"),
    ];

    let report = cluster
        .publish_records("t1", "default", "orders", records, AckMode::PerBatch)
        .await;

    assert!(!report.is_ok());
    assert_eq!(report.failed_records(), [0, 2]);
    assert_eq!(report.shards.len(), 2);
    assert!(report.shards[1].outcome.is_ok(), "shard 2 was accepted");
    assert_eq!(a.batches().len(), 1);
    assert_eq!(b.batches().len(), 1, "an ambiguous piece is not re-sent");
    let err = report.into_result().unwrap_err();
    assert!(format!("{err:#}").contains("2 records"), "{err:#}");
    Ok(())
}

/// **At least once re-sends a piece whose outcome was unknown**, with the
/// shard's order intact, and reports success.
#[tokio::test]
#[serial_test::serial]
async fn at_least_once_sends_an_ambiguous_piece_again() -> Result<()> {
    let answered = Arc::new(AtomicUsize::new(0));
    let script = {
        let answered = Arc::clone(&answered);
        move |id| match answered.fetch_add(1, Ordering::SeqCst) {
            0 => refuse(id, ErrorCode::Internal),
            _ => ok(id),
        }
    };
    let (cluster, a, _b) = pair(script, ok).await?;
    let records = vec![record(&key_on(2, 0), "r0"), record(&key_on(2, 1), "r1")];

    let report = cluster
        .publish_records_at_least_once("t1", "default", "orders", records, AckMode::PerBatch)
        .await;

    assert!(report.is_ok(), "{report:?}");
    let seen = a.batches();
    assert_eq!(seen.len(), 2, "sent twice");
    assert_eq!(payloads(&seen[1]), ["r0", "r1"]);
    Ok(())
}

/// **At least once still stops at a fatal refusal.**
#[tokio::test]
#[serial_test::serial]
async fn at_least_once_does_not_retry_a_fatal_refusal() -> Result<()> {
    let (cluster, a, _b) = pair(|id| refuse(id, ErrorCode::Forbidden), ok).await?;
    let report = cluster
        .publish_records_at_least_once(
            "t1",
            "default",
            "orders",
            vec![record(&key_on(0, 0), "r0")],
            AckMode::PerBatch,
        )
        .await;
    assert_eq!(report.failed_records(), [0]);
    assert_eq!(a.batches().len(), 1);
    Ok(())
}

/// **An unkeyed batch is one frame on shard 0's owner.**
#[tokio::test]
#[serial_test::serial]
async fn an_unkeyed_batch_goes_to_shard_zero_in_order() -> Result<()> {
    let (cluster, a, b) = pair(ok, ok).await?;
    own(&cluster, 0, &b, "b").await;
    cluster
        .publish_batch(
            "t1",
            "default",
            "orders",
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
            AckMode::PerBatch,
        )
        .await?;
    assert_eq!(a.batches().len(), 0);
    let on_b = b.batches();
    assert_eq!(on_b.len(), 1);
    assert_eq!(on_b[0].0, None);
    assert_eq!(payloads(&on_b[0]), ["a", "b", "c"]);
    Ok(())
}
