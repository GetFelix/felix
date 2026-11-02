//! The accept loop: its caps and rate, and that it runs on its own threads.

use std::sync::Mutex;

use felix_mqtt_wire::{Connect, Limits, Packet, Properties, ReasonCode, Utf8, Version};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::*;
use crate::serving::auth::demo::demo_auth_for_tenant;

fn config(max_connections_per_ip: usize) -> MqttListenerConfig {
    MqttListenerConfig {
        listen: "127.0.0.1:0".parse().expect("addr"),
        tls: false,
        worker_threads: 1,
        max_connections: 64,
        max_connections_per_ip,
        connect_rate: 0,
        settings: felix_mqtt::Settings::default(),
        ws: None,
    }
}

/// Accepts every token and records the thread each check ran on.
#[derive(Default)]
struct Recorder {
    threads: Mutex<Vec<String>>,
}

#[async_trait]
impl Authenticator for Recorder {
    async fn authenticate(
        &self,
        tenant_id: &str,
        _token: &str,
        _peer_certs: &[Vec<u8>],
    ) -> Result<Principal, AuthError> {
        let name = std::thread::current()
            .name()
            .unwrap_or_default()
            .to_string();
        self.threads.lock().expect("lock").push(name);
        Ok(Principal::new(tenant_id))
    }
}

fn connect_packet(tenant: &str, token: &str) -> Vec<u8> {
    let connect = Connect {
        version: Version::V311,
        clean_start: true,
        keep_alive: 30,
        client_id: Utf8::new("dev-1").expect("id"),
        will: None,
        username: Some(Utf8::new(tenant.to_string()).expect("tenant")),
        password: Some(bytes::Bytes::copy_from_slice(token.as_bytes())),
        properties: Properties::default(),
    };
    Packet::Connect(connect)
        .to_bytes(Version::V311)
        .expect("encode")
        .to_vec()
}

async fn read_connack(socket: &mut TcpStream) -> ReasonCode {
    let mut buf = bytes::BytesMut::new();
    loop {
        if let Some(Packet::ConnAck(ack)) =
            Packet::decode(&mut buf, Some(Version::V311), &Limits::default()).expect("decode")
        {
            return ack.reason;
        }
        let mut chunk = [0u8; 64];
        let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut chunk))
            .await
            .expect("answered")
            .expect("read");
        assert!(n > 0, "closed before CONNACK");
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// True when the server closed `socket` within a second.
async fn closed_by_server(socket: &mut TcpStream) -> bool {
    let mut byte = [0u8; 1];
    matches!(
        tokio::time::timeout(Duration::from_secs(1), socket.read(&mut byte)).await,
        Ok(Ok(0)) | Ok(Err(_))
    )
}

struct Running {
    addr: std::net::SocketAddr,
    ws_addr: Option<std::net::SocketAddr>,
    shutdown: CancellationToken,
    connections: TaskTracker,
}

fn start(config: &MqttListenerConfig, auth: Arc<dyn Authenticator>) -> Running {
    start_with_tls(config, None, auth)
}

fn start_with_tls(
    config: &MqttListenerConfig,
    tls: Option<Arc<rustls::ServerConfig>>,
    auth: Arc<dyn Authenticator>,
) -> Running {
    let listener =
        MqttListener::bind(config, tls, auth, Arc::new(felix_mqtt::NoMounts)).expect("bind");
    let addr = listener.local_addr().expect("addr");
    let ws_addr = listener.ws_local_addr().expect("ws addr");
    let shutdown = CancellationToken::new();
    let connections = TaskTracker::new();
    drop(listener.spawn(shutdown.clone(), connections.clone(), None));
    Running {
        addr,
        ws_addr,
        shutdown,
        connections,
    }
}

impl Running {
    async fn stop(self) {
        self.shutdown.cancel();
        self.connections.close();
        tokio::time::timeout(Duration::from_secs(5), self.connections.wait())
            .await
            .expect("connections drained");
    }
}

#[tokio::test]
async fn connections_are_served_on_the_listeners_own_threads() {
    let recorder = Arc::new(Recorder::default());
    let running = start(&config(0), recorder.clone());
    let mut socket = TcpStream::connect(running.addr).await.expect("connect");
    socket
        .write_all(&connect_packet("acme", "anything"))
        .await
        .expect("write");
    assert_eq!(read_connack(&mut socket).await, ReasonCode::Success);
    let threads = recorder.threads.lock().expect("lock").clone();
    assert!(
        !threads.is_empty() && threads.iter().all(|name| name == "felix-mqtt"),
        "authentication ran on {threads:?}"
    );
    drop(socket);
    running.stop().await;
}

#[tokio::test]
async fn a_connection_past_the_per_address_cap_is_closed_on_arrival() {
    let running = start(&config(2), Arc::new(Recorder::default()));
    let mut first = TcpStream::connect(running.addr).await.expect("first");
    let mut second = TcpStream::connect(running.addr).await.expect("second");
    let mut third = TcpStream::connect(running.addr).await.expect("third");
    assert!(closed_by_server(&mut third).await, "the third is refused");
    assert!(!closed_by_server(&mut first).await, "the first is served");
    assert!(!closed_by_server(&mut second).await, "the second is served");
    drop((first, second, third));
    running.stop().await;
}

#[tokio::test]
async fn a_demo_token_connects_and_a_wrong_one_does_not() {
    let demo = demo_auth_for_tenant("t1").expect("demo auth");
    let running = start(
        &config(0),
        Arc::new(BrokerMqttAuth::new(Arc::clone(&demo.auth))),
    );

    let mut good = TcpStream::connect(running.addr).await.expect("connect");
    good.write_all(&connect_packet("t1", &demo.token))
        .await
        .expect("write");
    assert_eq!(read_connack(&mut good).await, ReasonCode::Success);

    let mut bad = TcpStream::connect(running.addr).await.expect("connect");
    bad.write_all(&connect_packet("t1", "not-a-token"))
        .await
        .expect("write");
    assert_eq!(
        read_connack(&mut bad).await,
        ReasonCode::BadUserNameOrPassword
    );
    drop((good, bad));
    running.stop().await;
}

// Paused so the timer, not the machine, sets the pace.
#[tokio::test(start_paused = true)]
async fn the_connect_rate_holds_connections_back() {
    let mut gate = RateGate::new(10);
    let start = tokio::time::Instant::now();
    for _ in 0..6 {
        gate.admit().await;
    }
    // The first is free (the burst), the other five arrive 100 ms apart.
    assert_eq!(start.elapsed(), Duration::from_millis(500));

    let mut unlimited = RateGate::new(0);
    let start = tokio::time::Instant::now();
    for _ in 0..1000 {
        unlimited.admit().await;
    }
    assert_eq!(start.elapsed(), Duration::ZERO);
}

// A failed bind must come back as an error. Dropping the listener's runtime
// by hand inside the broker's own would panic instead.
#[tokio::test]
async fn binding_a_taken_port_is_an_error_not_a_panic() {
    let taken = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let mut taken_config = config(0);
    taken_config.listen = taken.local_addr().expect("addr");
    let err = MqttListener::bind(
        &taken_config,
        None,
        Arc::new(Recorder::default()),
        Arc::new(felix_mqtt::NoMounts),
    )
    .err()
    .expect("the port is taken");
    assert!(
        format!("{err:#}").contains("bind the MQTT listener"),
        "{err:#}"
    );
}

#[tokio::test]
async fn credentials_the_broker_refuses_are_rejected_not_unavailable() {
    let demo = demo_auth_for_tenant("t1").expect("demo auth");
    let auth = BrokerMqttAuth::new(Arc::new((*demo.auth).clone().with_subject_binding(true)));
    let rejected =
        |result: Result<Principal, AuthError>| matches!(result, Err(AuthError::Rejected(_)));

    // An empty tenant id, and a token for a tenant that is not the user's.
    assert!(rejected(auth.authenticate("", &demo.token, &[]).await));
    assert!(rejected(auth.authenticate("t1", "not-a-token", &[]).await));

    // A certificate issued to someone else, under subject binding.
    let mut params =
        rcgen::CertificateParams::new(vec!["someone-else.felix.test".to_string()]).expect("params");
    params.is_ca = rcgen::IsCa::NoCa;
    let key = rcgen::KeyPair::generate().expect("key");
    let cert = params.self_signed(&key).expect("cert").der().to_vec();
    assert!(rejected(
        auth.authenticate("t1", &demo.token, &[cert]).await
    ));

    let principal = auth
        .authenticate("t1", &demo.token, &[])
        .await
        .expect("no certificate, nothing to bind to");
    assert_eq!(principal.tenant_id(), "t1");
}

mod websocket {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::HeaderValue;
    use tokio_tungstenite::tungstenite::{Message, handshake::client::Request};
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

    use super::*;

    type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

    fn ws_config(max_connections: usize, origins: ws::AllowedOrigins) -> MqttListenerConfig {
        let mut config = config(0);
        config.max_connections = max_connections;
        config.ws = Some(crate::config::MqttWsConfig {
            listen: "127.0.0.1:0".parse().expect("addr"),
            settings: ws::Settings {
                allowed_origins: origins,
                ..ws::Settings::default()
            },
        });
        config
    }

    fn request(addr: std::net::SocketAddr, origin: Option<&str>) -> Request {
        let mut request = format!("ws://{addr}/mqtt")
            .into_client_request()
            .expect("request");
        request
            .headers_mut()
            .insert("Sec-WebSocket-Protocol", HeaderValue::from_static("mqtt"));
        if let Some(origin) = origin {
            request
                .headers_mut()
                .insert("Origin", HeaderValue::from_str(origin).expect("origin"));
        }
        request
    }

    async fn connect(addr: std::net::SocketAddr) -> Client {
        let (client, response) = connect_async(request(addr, None)).await.expect("upgrade");
        assert_eq!(
            response
                .headers()
                .get("Sec-WebSocket-Protocol")
                .expect("subprotocol"),
            "mqtt"
        );
        client
    }

    /// The packets in the next message the server sends. It writes the replies
    /// to one batch of packets as one message.
    async fn next_packets<S>(client: &mut WebSocketStream<S>) -> Vec<Packet>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let message = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("answered")
            .expect("open")
            .expect("message");
        let Message::Binary(bytes) = message else {
            panic!("expected a binary message, got {message:?}");
        };
        let mut buf = bytes::BytesMut::from(&bytes[..]);
        let mut packets = Vec::new();
        while let Some(packet) =
            Packet::decode(&mut buf, Some(Version::V311), &Limits::default()).expect("decode")
        {
            packets.push(packet);
        }
        assert!(buf.is_empty(), "the message ends on a packet boundary");
        packets
    }

    async fn next_packet<S>(client: &mut WebSocketStream<S>) -> Packet
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let mut packets = next_packets(client).await;
        assert_eq!(packets.len(), 1, "{packets:?}");
        packets.remove(0)
    }

    fn ack(packet: Packet) -> ReasonCode {
        match packet {
            Packet::ConnAck(ack) => ack.reason,
            other => panic!("expected CONNACK, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_client_connects_over_websockets_on_the_listeners_own_threads() {
        let recorder = Arc::new(Recorder::default());
        let running = start(&ws_config(64, ws::AllowedOrigins::None), recorder.clone());
        let mut client = connect(running.ws_addr.expect("websocket on")).await;
        client
            .send(Message::binary(connect_packet("acme", "anything")))
            .await
            .expect("send");
        assert_eq!(ack(next_packet(&mut client).await), ReasonCode::Success);
        client
            .send(Message::binary(vec![0xc0, 0x00]))
            .await
            .expect("send");
        assert!(matches!(next_packet(&mut client).await, Packet::PingResp));
        let threads = recorder.threads.lock().expect("lock").clone();
        assert!(
            !threads.is_empty() && threads.iter().all(|name| name == "felix-mqtt"),
            "authentication ran on {threads:?}"
        );
        client.close(None).await.expect("close");
        running.stop().await;
    }

    #[tokio::test]
    async fn a_packet_may_span_messages_and_messages_may_hold_several_packets() {
        let running = start(
            &ws_config(64, ws::AllowedOrigins::None),
            Arc::new(Recorder::default()),
        );
        let mut client = connect(running.ws_addr.expect("websocket on")).await;
        let connect = connect_packet("acme", "anything");
        let (head, tail) = connect.split_at(5);
        client
            .send(Message::binary(head.to_vec()))
            .await
            .expect("send");
        client
            .send(Message::binary(tail.to_vec()))
            .await
            .expect("send");
        assert_eq!(ack(next_packet(&mut client).await), ReasonCode::Success);

        // Two PINGREQs in one message are two PINGRESPs.
        client
            .send(Message::binary(vec![0xc0, 0x00, 0xc0, 0x00]))
            .await
            .expect("send");
        let replies = next_packets(&mut client).await;
        assert!(matches!(replies[..], [Packet::PingResp, Packet::PingResp]));
        running.stop().await;
    }

    #[tokio::test]
    async fn the_origin_list_decides_which_browsers_connect() {
        let origins = ws::AllowedOrigins::Only(vec!["https://app.example.com".into()]);
        let running = start(&ws_config(64, origins), Arc::new(Recorder::default()));
        let addr = running.ws_addr.expect("websocket on");

        let refused = connect_async(request(addr, Some("https://evil.example.com"))).await;
        let Err(tokio_tungstenite::tungstenite::Error::Http(response)) = refused else {
            panic!("the upgrade should be refused with a status: {refused:?}");
        };
        assert_eq!(response.status().as_u16(), 403);

        let (mut client, _) = connect_async(request(addr, Some("https://app.example.com")))
            .await
            .expect("allowed origin");
        client
            .send(Message::binary(connect_packet("acme", "anything")))
            .await
            .expect("send");
        assert_eq!(ack(next_packet(&mut client).await), ReasonCode::Success);
        running.stop().await;
    }

    #[tokio::test]
    async fn mqtt_sent_to_the_websocket_port_is_closed() {
        let running = start(
            &ws_config(64, ws::AllowedOrigins::None),
            Arc::new(Recorder::default()),
        );
        let mut socket = TcpStream::connect(running.ws_addr.expect("websocket on"))
            .await
            .expect("connect");
        socket
            .write_all(&connect_packet("acme", "anything"))
            .await
            .expect("write");
        let mut reply = String::new();
        tokio::time::timeout(Duration::from_secs(1), socket.read_to_string(&mut reply))
            .await
            .expect("closed")
            .expect("read");
        assert!(reply.starts_with("HTTP/1.1 400 Bad Request\r\n"), "{reply}");
        running.stop().await;
    }

    // The WebSocket socket draws on the same slots as the plain one, so a
    // browser fleet cannot take more than the configured connections.
    #[tokio::test]
    async fn connection_caps_count_both_sockets_together() {
        let running = start(
            &ws_config(1, ws::AllowedOrigins::None),
            Arc::new(Recorder::default()),
        );
        let mut plain = TcpStream::connect(running.addr).await.expect("connect");
        plain
            .write_all(&connect_packet("acme", "anything"))
            .await
            .expect("write");
        assert_eq!(read_connack(&mut plain).await, ReasonCode::Success);

        let mut socket = TcpStream::connect(running.ws_addr.expect("websocket on"))
            .await
            .expect("connect");
        assert!(closed_by_server(&mut socket).await, "no slot left");

        drop(plain);
        // The slot comes back once the first connection is gone.
        let mut admitted = false;
        for _ in 0..50 {
            if let Ok((_, _)) = connect_async(request(running.ws_addr.expect("ws"), None)).await {
                admitted = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(admitted, "the slot was never released");
        running.stop().await;
    }

    // `wss`: the TLS handshake comes first and the upgrade runs inside it.
    #[tokio::test]
    async fn a_client_connects_over_secure_websockets() {
        let key = rcgen::KeyPair::generate().expect("key");
        let cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
            .expect("params")
            .self_signed(&key)
            .expect("cert");
        let der = cert.der().clone();
        let server = crate::serving::kafka::tls_config(
            der.clone(),
            rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        )
        .expect("server config");
        let mut config = ws_config(64, ws::AllowedOrigins::None);
        config.tls = true;
        let running = start_with_tls(&config, Some(server), Arc::new(Recorder::default()));

        let mut roots = rustls::RootCertStore::empty();
        roots.add(der).expect("root");
        let client_config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_protocol_versions(rustls::ALL_VERSIONS)
        .expect("versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
        let addr = running.ws_addr.expect("websocket on");
        let tcp = TcpStream::connect(addr).await.expect("connect");
        let tls = tokio_rustls::TlsConnector::from(Arc::new(client_config))
            .connect("localhost".try_into().expect("name"), tcp)
            .await
            .expect("tls");
        let (mut client, _) = tokio_tungstenite::client_async(request(addr, None), tls)
            .await
            .expect("upgrade");
        client
            .send(Message::binary(connect_packet("acme", "anything")))
            .await
            .expect("send");
        assert_eq!(ack(next_packet(&mut client).await), ReasonCode::Success);
        running.stop().await;
    }
}
