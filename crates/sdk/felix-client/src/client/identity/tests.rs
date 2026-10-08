use std::sync::{Arc, Mutex};

use anyhow::Result;
use bytes::BytesMut;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::Message;

use crate::Client;
use crate::frame_io::{read_message_with_limit, write_message};
use crate::test_support::{build_client_config, build_server_config};

const MAX_FRAME: usize = 1024 * 1024;

/// A broker that refuses the token `"refused"`, accepts any other, and
/// records which token each stream authenticated with.
struct RecordingBroker {
    addr: std::net::SocketAddr,
    connections: Arc<Mutex<usize>>,
    tokens: Arc<Mutex<Vec<(String, String)>>>,
    task: tokio::task::JoinHandle<()>,
}

impl RecordingBroker {
    fn start() -> Result<(Self, rustls::pki_types::CertificateDer<'static>)> {
        let (server_config, cert) = build_server_config()?;
        let server = QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?;
        let addr = server.local_addr()?;
        let connections = Arc::new(Mutex::new(0));
        let tokens = Arc::new(Mutex::new(Vec::new()));
        let task = tokio::spawn({
            let connections = Arc::clone(&connections);
            let tokens = Arc::clone(&tokens);
            async move {
                while let Ok(connection) = server.accept().await {
                    *connections.lock().unwrap() += 1;
                    let tokens = Arc::clone(&tokens);
                    tokio::spawn(async move {
                        while let Ok((send, recv)) = connection.accept_bi().await {
                            tokio::spawn(answer(send, recv, Arc::clone(&tokens)));
                        }
                    });
                }
            }
        });
        Ok((
            Self {
                addr,
                connections,
                tokens,
                task,
            },
            cert,
        ))
    }

    fn connections(&self) -> usize {
        *self.connections.lock().unwrap()
    }

    fn streams_for(&self, token: &str) -> usize {
        self.tokens
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, presented)| presented == token)
            .count()
    }
}

impl Drop for RecordingBroker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn answer(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    tokens: Arc<Mutex<Vec<(String, String)>>>,
) -> Result<()> {
    let mut scratch = BytesMut::new();
    while let Some(message) = read_message_with_limit(&mut recv, &mut scratch, MAX_FRAME).await? {
        let Message::Auth {
            tenant_id, token, ..
        } = message
        else {
            write_message(&mut send, Message::Ok).await?;
            continue;
        };
        if token == "refused" {
            write_message(
                &mut send,
                Message::Error {
                    message: "auth failed".to_string(),
                    code: None,
                    retry: None,
                    detail: None,
                },
            )
            .await?;
            break;
        }
        tokens.lock().unwrap().push((tenant_id, token));
        write_message(
            &mut send,
            Message::AuthOk {
                server_flags: felix_wire::KNOWN_FLAGS,
                server_features: Some(0),
                server_features_hi: None,
                listener_ports: None,
                publish_window: None,
            },
        )
        .await?;
    }
    send.finish()?;
    Ok(())
}

#[tokio::test]
async fn identities_share_the_parent_connections() -> Result<()> {
    let (broker, cert) = RecordingBroker::start()?;
    let parent = Client::connect(broker.addr, "localhost", build_client_config(cert)?).await?;
    let held = parent.connection_count();

    let alice = parent.with_identity_token("t1", "alice").await?;
    let bob = parent.with_identity_token("t2", "bob").await?;

    // The broker counts a connection when its accept loop gets to it, which
    // can trail the client's handshake.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(broker.connections(), held, "an identity dialled");
    assert_eq!(alice.connection_count(), held);
    assert_eq!(bob.connection_count(), held);
    // One publish and one cache stream each, under its own token.
    assert_eq!(broker.streams_for("alice"), 2);
    assert_eq!(broker.streams_for("bob"), 2);
    assert!(
        broker
            .tokens
            .lock()
            .unwrap()
            .iter()
            .any(|(tenant, token)| tenant == "t2" && token == "bob"),
        "bob's streams did not name bob's tenant"
    );

    // Streams opened later go out under the identity that opened them.
    let before = broker.streams_for("alice");
    let _stream = alice.open_event_stream().await?;
    assert_eq!(broker.streams_for("alice"), before + 1);
    assert_eq!(broker.streams_for("bob"), 2);
    Ok(())
}

#[tokio::test]
async fn a_refused_identity_leaves_the_others_working() -> Result<()> {
    let (broker, cert) = RecordingBroker::start()?;
    let parent = Client::connect(broker.addr, "localhost", build_client_config(cert)?).await?;
    let alice = parent.with_identity_token("t1", "alice").await?;
    let held = parent.connection_count();

    let refused = parent.with_identity_token("t1", "refused").await;
    assert!(refused.is_err(), "a refused token built a client");

    assert_eq!(
        parent.connection_count(),
        held,
        "a refusal cost a connection"
    );
    assert!(parent.is_usable());
    assert!(alice.is_usable());
    alice.open_event_stream().await?;
    parent.open_event_stream().await?;
    Ok(())
}

#[tokio::test]
async fn dropping_an_identity_leaves_the_connections() -> Result<()> {
    let (broker, cert) = RecordingBroker::start()?;
    let parent = Client::connect(broker.addr, "localhost", build_client_config(cert)?).await?;
    let held = parent.connection_count();
    drop(parent.with_identity_token("t1", "alice").await?);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(parent.connection_count(), held);
    assert!(parent.is_usable());
    parent.open_event_stream().await?;
    Ok(())
}
