//! A durable-stream subscription that falls behind ends with
//! `SubscriptionLagged`, whichever queue dropped the records.

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::Message;
use tokio::time::{Duration, timeout};

use crate::frame_io::{read_message, write_message};
use crate::test_support::{
    build_client_config_with_overrides, build_server_config, set_client_env,
};
use crate::{Client, ClientConfig, SubscriptionLagged};

/// What the stub broker writes on the event stream after its hello.
enum Write {
    /// One binary batch of one event at this offset.
    Event(u64),
    Message(Message),
}

/// Serve one subscription that writes `writes`, then wait to be stopped.
async fn stub_broker(
    writes: Vec<Write>,
    configure: impl FnOnce(&mut ClientConfig),
) -> Result<(Client, tokio::task::JoinHandle<Result<()>>)> {
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let writes = std::sync::Arc::new(tokio::sync::Mutex::new(Some(writes)));
    // The client opens several connections; whichever carries the subscribe
    // gets the event stream.
    let server_task = tokio::spawn(async move {
        let mut handlers = Vec::new();
        while let Ok(connection) = server.accept().await {
            let writes = std::sync::Arc::clone(&writes);
            handlers.push(tokio::spawn(serve(connection, writes)));
        }
        Ok(())
    });
    let mut config = build_client_config_with_overrides(cert, 1)?;
    configure(&mut config);
    let client =
        Client::connect_with_transport(addr, "localhost", config, TransportConfig::default())
            .await?;
    Ok((client, server_task))
}

async fn serve(
    connection: felix_transport::QuicConnection,
    writes: std::sync::Arc<tokio::sync::Mutex<Option<Vec<Write>>>>,
) -> Result<()> {
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let (mut send, mut recv) = connection.accept_bi().await?;
    let _ = read_message(&mut recv, &mut scratch).await?;
    write_message(&mut send, Message::Ok).await?;
    while let Some(message) = read_message(&mut recv, &mut scratch).await? {
        let Message::Subscribe { .. } = message else {
            continue;
        };
        write_message(
            &mut send,
            Message::Subscribed {
                subscription_id: 1,
                start_offset: None,
                live_offset: None,
                queue_capacity: None,
            },
        )
        .await?;
        let mut uni = connection.open_uni().await?;
        write_message(&mut uni, Message::EventStreamHello { subscription_id: 1 }).await?;
        for write in writes.lock().await.take().unwrap_or_default() {
            match write {
                Write::Event(offset) => {
                    let batch = felix_wire::binary::encode_event_batch_bytes_with_offset(
                        1,
                        &[Bytes::from(format!("event-{offset}"))],
                        offset,
                    )?;
                    uni.write_all(&batch).await?;
                }
                Write::Message(message) => write_message(&mut uni, message).await?,
            }
        }
        // Held open: the end has to come from the lag, not a finished stream.
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
    Ok(())
}

fn lagged(err: &anyhow::Error) -> Option<u64> {
    err.downcast_ref::<SubscriptionLagged>()
        .map(|lagged| lagged.resume_from)
}

/// The broker's `subscription_lagged` ends the subscription after the events
/// sent before it, with where to resume, while the stream is still open.
#[tokio::test]
#[serial_test::serial]
async fn the_brokers_lag_notice_ends_the_subscription() -> Result<()> {
    let _env = set_client_env();
    let (client, server) = stub_broker(
        vec![
            Write::Event(7),
            Write::Message(Message::SubscriptionLagged {
                subscription_id: 1,
                resume_from: 8,
            }),
        ],
        |_| {},
    )
    .await?;
    let mut subscription = client.subscribe("t1", "default", "updates").await?;

    let event = timeout(Duration::from_secs(5), subscription.next_event())
        .await??
        .expect("the event before the drop");
    assert_eq!(event.offset, Some(7));
    let Err(end) = timeout(Duration::from_secs(5), subscription.next_event()).await? else {
        panic!("a lag ends the subscription with an error");
    };
    assert_eq!(lagged(&end), Some(8), "{end:#}");
    server.abort();
    Ok(())
}

/// A drop in the client's own queue is reported the same way, with the
/// first offset it dropped, though no further event will ever arrive.
#[tokio::test]
#[serial_test::serial]
async fn the_clients_own_drop_ends_the_subscription() -> Result<()> {
    let _env = set_client_env();
    let (client, server) = stub_broker(
        vec![Write::Event(7), Write::Event(8), Write::Event(9)],
        |config| config.client_sub_queue_capacity = 1,
    )
    .await?;
    let mut subscription = client.subscribe("t1", "default", "updates").await?;
    // Nothing reads until all three have arrived, so the queue of one
    // holds the first and drops the rest.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let event = timeout(Duration::from_secs(5), subscription.next_event())
        .await??
        .expect("the event that fit");
    assert_eq!(event.offset, Some(7));
    let Err(end) = timeout(Duration::from_secs(5), subscription.next_event()).await? else {
        panic!("a drop ends the subscription with an error");
    };
    assert_eq!(lagged(&end), Some(8), "{end:#}");
    server.abort();
    Ok(())
}
