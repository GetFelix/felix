//! Record headers through a [`Client`] against stub brokers: what the client
//! offers, what it sends, and what it refuses to send.

use anyhow::Result;
use bytes::Bytes;
use felix_wire::{AckMode, Message, RecordHeaders};

use super::connections::{default_config, policy};
use super::stub_broker::{STREAM_SHARDS, StubBroker};
use crate::RecordHeadersError;
use crate::cluster::Client;
use crate::test_support::build_server_config;

fn ok(request_id: u64) -> Message {
    Message::PublishOk {
        request_id,
        offset: Some(5),
    }
}

fn headers(key: Option<&'static [u8]>, name: &str, value: &'static [u8]) -> RecordHeaders {
    RecordHeaders {
        key: key.map(Bytes::from_static),
        headers: vec![(name.to_string(), Bytes::from_static(value))],
    }
}

async fn connect(
    broker: &StubBroker,
    cert: rustls::pki_types::CertificateDer<'static>,
) -> Result<Client> {
    Client::connect_with_policy(&[broker.addr], "localhost", default_config(cert)?, policy()).await
}

/// **The extension is offered unless the config turns it off**, and with it
/// off the `Auth` is the one a client sent before headers existed.
#[tokio::test]
#[serial_test::serial]
async fn record_headers_are_offered_unless_turned_off() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let on = StubBroker::start_with(server_config.clone(), ok)?;
    let off = StubBroker::start_with(server_config, ok)?;
    let client = connect(&on, cert.clone()).await?;
    let mut config = default_config(cert)?;
    config.record_headers = false;
    let plain = Client::connect_with_policy(&[off.addr], "localhost", config, policy()).await?;

    let offered = on.offers();
    assert!(!offered.is_empty(), "no stream authenticated");
    for (flags, ext) in offered {
        assert!(felix_wire::supports(
            flags.expect("flags"),
            felix_wire::FLAG_EXTENDED
        ));
        assert_eq!(ext, Some(felix_wire::EXT_RECORD_HEADERS));
    }
    let before = Some(
        felix_wire::KNOWN_FLAGS
            & !felix_wire::FLAG_EVENT_BATCH_PUBLISHER
            & !felix_wire::FLAG_EVENT_BATCH_TIMESTAMPS,
    );
    let offered = off.offers();
    assert!(!offered.is_empty(), "no stream authenticated");
    for offer in offered {
        assert_eq!(offer, (before, None));
    }
    drop((client, plain));
    Ok(())
}

/// **Each record's block reaches the broker as published**, and a record with
/// neither a key nor headers has none.
#[tokio::test]
#[serial_test::serial]
async fn headers_reach_a_broker_that_answered_the_extension() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let broker = StubBroker::start_with(server_config, ok)?;
    broker.honour_record_headers();
    let cluster = connect(&broker, cert).await?;

    let first = headers(Some(b"order-7"), "content-type", b"text/plain");
    let offset = cluster
        .publish_with_headers(
            "t1",
            "default",
            "orders",
            first.clone(),
            b"one".to_vec(),
            AckMode::PerBatch,
        )
        .await?;
    assert_eq!(offset, Some(5));
    let third = headers(None, "trace-id", b"abc");
    cluster
        .publish_batch_with_headers(
            "t1",
            "default",
            "orders",
            vec![
                (first.clone(), b"a".to_vec()),
                (RecordHeaders::default(), b"b".to_vec()),
                (third.clone(), b"c".to_vec()),
            ],
            AckMode::PerBatch,
        )
        .await?;

    let blocks = broker.blocks();
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0], [Some(first.encode()?)]);
    assert_eq!(
        blocks[1],
        [Some(first.encode()?), None, Some(third.encode()?)]
    );
    assert_eq!(broker.batches()[1].1, [&b"a"[..], b"b", b"c"]);
    Ok(())
}

/// **Nothing is sent to a broker that did not answer the extension.** Sending
/// the frame would be refused, and sending the payload alone would drop the
/// headers.
#[tokio::test]
#[serial_test::serial]
async fn a_broker_that_did_not_answer_is_sent_nothing() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let broker = StubBroker::start_with(server_config, ok)?;
    let cluster = connect(&broker, cert).await?;

    let err = cluster
        .publish_with_headers(
            "t1",
            "default",
            "orders",
            headers(None, "trace-id", b"abc"),
            b"x".to_vec(),
            AckMode::PerBatch,
        )
        .await
        .expect_err("the broker never answered the extension");
    assert_eq!(
        err.downcast_ref::<RecordHeadersError>(),
        Some(&RecordHeadersError::Unsupported),
        "{err:#}"
    );
    assert_eq!(broker.publishes(), 0);
    assert!(broker.batches().is_empty());
    // Not a broken connection: no reconnect, and a plain publish still works.
    cluster
        .publish("t1", "default", "orders", b"y".to_vec(), AckMode::PerBatch)
        .await?;
    assert_eq!(broker.accepted_connections(), 1);
    Ok(())
}

/// **A name the broker would refuse is refused here**, before anything is sent.
#[tokio::test]
#[serial_test::serial]
async fn reserved_and_empty_names_are_refused_locally() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let broker = StubBroker::start_with(server_config, ok)?;
    broker.honour_record_headers();
    let cluster = connect(&broker, cert).await?;

    for (name, expected) in [
        (
            "felix.offset",
            RecordHeadersError::ReservedName {
                name: "felix.offset".into(),
            },
        ),
        ("", RecordHeadersError::EmptyName),
    ] {
        let err = cluster
            .publish_with_headers(
                "t1",
                "default",
                "orders",
                headers(None, name, b"v"),
                b"x".to_vec(),
                AckMode::PerBatch,
            )
            .await
            .expect_err("a refused name");
        assert_eq!(err.downcast_ref::<RecordHeadersError>(), Some(&expected));
    }
    // Case matters to the broker, so a different case is not reserved.
    cluster
        .publish_with_headers(
            "t1",
            "default",
            "orders",
            headers(None, "Felix.offset", b"v"),
            b"x".to_vec(),
            AckMode::PerBatch,
        )
        .await?;
    assert_eq!(broker.publishes(), 1);
    Ok(())
}

/// **With nothing to carry the frame is the plain one**, so it works against
/// any broker and costs nothing.
#[tokio::test]
#[serial_test::serial]
async fn records_without_headers_are_sent_plain() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    // Does not answer the extension: a plain frame must still be accepted.
    let broker = StubBroker::start_with(server_config, ok)?;
    let cluster = connect(&broker, cert).await?;

    cluster
        .publish_batch_with_headers(
            "t1",
            "default",
            "orders",
            vec![
                (RecordHeaders::default(), b"a".to_vec()),
                (RecordHeaders::default(), b"b".to_vec()),
            ],
            AckMode::PerBatch,
        )
        .await?;

    // `blocks` is empty for a frame without the extension.
    assert_eq!(broker.blocks(), [Vec::<Option<Bytes>>::new()]);
    assert_eq!(broker.batches()[0].1, [&b"a"[..], b"b"]);
    Ok(())
}

/// **The record's key does not route**: a record goes to shard 0's owner
/// whatever key it carries.
#[tokio::test]
#[serial_test::serial]
async fn a_record_key_does_not_choose_the_shard() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let a = StubBroker::start_with(server_config.clone(), ok)?;
    let b = StubBroker::start_with(server_config, ok)?;
    a.honour_record_headers();
    b.honour_record_headers();
    let owners = (0..STREAM_SHARDS)
        .map(|shard| {
            let (node, broker) = if shard == 0 { ("a", &a) } else { ("b", &b) };
            felix_wire::ShardOwner {
                shard,
                node_id: Some(node.to_string()),
                addr: Some(broker.addr.to_string()),
                generation: 1,
                unavailable: None,
            }
        })
        .collect();
    a.set_cache_owners(owners);
    let cluster = connect(&a, cert).await?;

    for key in [&b"k0"[..], b"k1", b"k2", b"k3", b"k4"] {
        let key = Bytes::copy_from_slice(key);
        cluster
            .publish_with_headers(
                "t1",
                "default",
                "orders",
                RecordHeaders {
                    key: Some(key),
                    headers: Vec::new(),
                },
                b"x".to_vec(),
                AckMode::PerBatch,
            )
            .await?;
    }
    assert_eq!(a.blocks().len(), 5);
    assert!(b.blocks().is_empty());
    Ok(())
}
