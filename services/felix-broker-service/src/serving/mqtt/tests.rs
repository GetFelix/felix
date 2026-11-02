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
    shutdown: CancellationToken,
    connections: TaskTracker,
}

fn start(config: &MqttListenerConfig, auth: Arc<dyn Authenticator>) -> Running {
    let listener = MqttListener::bind(config, None, auth).expect("bind");
    let addr = listener.local_addr().expect("addr");
    let shutdown = CancellationToken::new();
    let connections = TaskTracker::new();
    drop(listener.spawn(shutdown.clone(), connections.clone(), None));
    Running {
        addr,
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
    let err = MqttListener::bind(&taken_config, None, Arc::new(Recorder::default()))
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
