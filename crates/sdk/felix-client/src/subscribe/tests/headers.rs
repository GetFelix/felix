//! Record headers on delivered events: each event gets its own record's key
//! and headers, and a frame without the extension gives none.

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::binary::EventBatchMeta;
use felix_wire::{Message, RecordHeaders};

use crate::Connection;
use crate::frame_io::{read_message, write_message};
use crate::test_support::{
    build_client_config_with_overrides, build_server_config, set_client_env,
};

const SUBSCRIPTION_ID: u64 = 1;

fn headers(key: &'static [u8], name: &str, value: &'static [u8]) -> RecordHeaders {
    RecordHeaders {
        key: Some(Bytes::from_static(key)),
        headers: vec![(name.to_string(), Bytes::from_static(value))],
    }
}

fn payloads() -> Vec<Bytes> {
    ["a", "b", "c"]
        .map(|p| Bytes::from(p.as_bytes().to_vec()))
        .to_vec()
}

/// The second record has neither a key nor headers.
fn blocks() -> (Vec<Option<RecordHeaders>>, Vec<Option<Bytes>>) {
    let first = headers(b"k1", "content-type", b"text/plain");
    let third = headers(b"k3", "trace-id", b"abc");
    let encoded = vec![
        Some(first.encode().expect("encode")),
        None,
        Some(third.encode().expect("encode")),
    ];
    (vec![Some(first), None, Some(third)], encoded)
}

/// Serve `frame` on the event stream of the first subscription, then end the
/// stream, and read back every event the client yields.
async fn events_from(frame: Bytes) -> Result<Vec<crate::Event>> {
    let _env = set_client_env();
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        while let Ok(connection) = server.accept().await {
            let frame = frame.clone();
            tokio::spawn(async move {
                let mut scratch = BytesMut::with_capacity(64 * 1024);
                let (mut send, mut recv) = connection.accept_bi().await?;
                let _ = read_message(&mut recv, &mut scratch).await?;
                write_message(&mut send, Message::Ok).await?;
                while let Some(message) = read_message(&mut recv, &mut scratch).await? {
                    if !matches!(message, Message::Subscribe { .. }) {
                        continue;
                    }
                    write_message(
                        &mut send,
                        Message::Subscribed {
                            subscription_id: SUBSCRIPTION_ID,
                            start_offset: None,
                            live_offset: None,
                            queue_capacity: None,
                        },
                    )
                    .await?;
                    let mut uni = connection.open_uni().await?;
                    write_message(
                        &mut uni,
                        Message::EventStreamHello {
                            subscription_id: SUBSCRIPTION_ID,
                        },
                    )
                    .await?;
                    uni.write_all(&frame).await?;
                    uni.finish()?;
                    // Hold the connection until the client has read the frame.
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
                anyhow::Ok(())
            });
        }
    });
    let client = Connection::connect_with_transport(
        addr,
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
        TransportConfig::default(),
    )
    .await?;
    let mut subscription = client.subscribe("t1", "default", "updates").await?;
    let mut events = Vec::new();
    while events.len() < 3 {
        let Some(event) = subscription.next_event().await? else {
            break;
        };
        events.push(event);
    }
    server_task.abort();
    Ok(events)
}

fn assert_aligned(events: &[crate::Event], expected: &[Option<RecordHeaders>]) {
    assert_eq!(events.len(), expected.len());
    for (event, expected) in events.iter().zip(expected) {
        assert_eq!(&event.headers, expected, "payload {:?}", event.payload);
    }
}

/// **Each event carries its own record's headers**, in the order the batch
/// held them, and a record without a block has none.
#[tokio::test]
#[serial_test::serial]
async fn a_batch_with_headers_gives_each_event_its_own() -> Result<()> {
    let (expected, encoded) = blocks();
    let frame = felix_wire::binary::encode_event_batch_bytes_with_meta(
        SUBSCRIPTION_ID,
        &payloads(),
        EventBatchMeta {
            base_offset: Some(10),
            record_headers: Some(&encoded),
            ..EventBatchMeta::default()
        },
    )?;
    let events = events_from(frame).await?;
    assert_aligned(&events, &expected);
    assert_eq!(
        events.iter().map(|event| event.offset).collect::<Vec<_>>(),
        [Some(10), Some(11), Some(12)]
    );
    Ok(())
}

/// The shared frame, one encoding for every subscriber, carries them too.
#[tokio::test]
#[serial_test::serial]
async fn a_shared_batch_with_headers_gives_each_event_its_own() -> Result<()> {
    let (expected, encoded) = blocks();
    let frame = felix_wire::binary::encode_shared_event_batch_bytes_with_meta(
        &payloads(),
        EventBatchMeta {
            record_headers: Some(&encoded),
            ..EventBatchMeta::default()
        },
    )?;
    let events = events_from(frame).await?;
    assert_aligned(&events, &expected);
    Ok(())
}

/// **Without the extension no event has headers**: a broker or a config that
/// did not negotiate it sends the plain frame.
#[tokio::test]
#[serial_test::serial]
async fn a_frame_without_the_extension_gives_no_headers() -> Result<()> {
    for frame in [
        felix_wire::binary::encode_event_batch_bytes(SUBSCRIPTION_ID, &payloads())?,
        felix_wire::binary::encode_shared_event_batch_bytes(&payloads())?,
    ] {
        let events = events_from(frame).await?;
        assert_aligned(&events, &[None, None, None]);
    }
    Ok(())
}
