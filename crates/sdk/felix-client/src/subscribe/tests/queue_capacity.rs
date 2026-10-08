//! Asking the broker for a queue capacity, and only one that will answer.

use anyhow::Result;
use bytes::BytesMut;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::Message;
use tokio::sync::mpsc;

use crate::Client;
use crate::frame_io::{read_message, write_message};
use crate::test_support::{
    build_client_config_with_overrides, build_server_config, set_client_env,
};

/// A broker that advertises `features` (none means one that predates
/// negotiation), reports the `queue_capacity` each subscribe carried, and
/// grants half of it.
async fn subscribe_against(
    features: Option<u32>,
    asked: Option<u32>,
) -> Result<(Option<u32>, Option<u32>)> {
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
    let server_task = tokio::spawn(async move {
        while let Ok(connection) = server.accept().await {
            let seen_tx = seen_tx.clone();
            tokio::spawn(async move {
                let mut scratch = BytesMut::with_capacity(64 * 1024);
                let (mut send, mut recv) = connection.accept_bi().await?;
                let _ = read_message(&mut recv, &mut scratch).await?;
                let answer = match features {
                    Some(features) => Message::AuthOk {
                        server_flags: felix_wire::ORIGINAL_V1_FLAGS,
                        server_features: Some(features),
                        listener_ports: None,
                        publish_window: None,
                    },
                    None => Message::Ok,
                };
                write_message(&mut send, answer).await?;
                while let Some(message) = read_message(&mut recv, &mut scratch).await? {
                    let Message::Subscribe { queue_capacity, .. } = message else {
                        continue;
                    };
                    let _ = seen_tx.send(queue_capacity);
                    write_message(
                        &mut send,
                        Message::Subscribed {
                            subscription_id: 1,
                            start_offset: None,
                            live_offset: None,
                            queue_capacity: queue_capacity.map(|asked| asked / 2),
                        },
                    )
                    .await?;
                    let mut uni = connection.open_uni().await?;
                    write_message(&mut uni, Message::EventStreamHello { subscription_id: 1 })
                        .await?;
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                }
                anyhow::Ok(())
            });
        }
    });
    let mut config = build_client_config_with_overrides(cert, 1)?;
    config.broker_sub_queue_capacity = asked;
    let client =
        Client::connect_with_transport(addr, "localhost", config, TransportConfig::default())
            .await?;
    let subscription = client.subscribe("t1", "default", "updates").await?;
    let sent = seen_rx.recv().await.expect("the broker saw the subscribe");
    server_task.abort();
    Ok((sent, subscription.queue_capacity()))
}

#[tokio::test]
#[serial_test::serial]
async fn a_broker_that_advertises_the_bit_is_asked_and_its_grant_reported() -> Result<()> {
    let _env = set_client_env();
    let (sent, granted) =
        subscribe_against(Some(felix_wire::FEATURE_SUBSCRIBE_QUEUE), Some(4096)).await?;
    assert_eq!(sent, Some(4096));
    assert_eq!(granted, Some(2048));
    Ok(())
}

/// An older broker would ignore the field and give the stream's default, so
/// it is not sent, and the subscribe is the frame it always was.
#[tokio::test]
#[serial_test::serial]
async fn a_broker_without_the_bit_is_not_asked() -> Result<()> {
    let _env = set_client_env();
    for features in [None, Some(felix_wire::FEATURE_SUBSCRIPTION_LAGGED)] {
        let (sent, granted) = subscribe_against(features, Some(4096)).await?;
        assert_eq!(sent, None);
        assert_eq!(granted, None);
    }
    Ok(())
}
