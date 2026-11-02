use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use felix_mqtt_wire::{
    Connect, Disconnect, Limits, Packet, Properties, Publish, QoS, ReasonCode, Subscribe,
    SubscribeFilter, Unsubscribe, Utf8, Version, Will,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::MqttService;
use crate::auth::{AuthError, Authenticator, Principal};
use crate::settings::Settings;

/// Accepts the token `good` for tenant `acme`, says `down` is unavailable, and
/// rejects everything else.
struct FakeAuth {
    expires_at: Option<i64>,
    revoke: Arc<Notify>,
}

#[async_trait]
impl Authenticator for FakeAuth {
    async fn authenticate(
        &self,
        tenant_id: &str,
        token: &str,
        _peer_certs: &[Vec<u8>],
    ) -> Result<Principal, AuthError> {
        match (tenant_id, token) {
            ("acme", "good") => {
                let revoke = Arc::clone(&self.revoke);
                let mut principal = Principal::new("acme").with_revocation(move || {
                    let revoke = Arc::clone(&revoke);
                    Box::pin(async move { revoke.notified().await })
                });
                if let Some(at) = self.expires_at {
                    principal = principal.with_expiry(at);
                }
                Ok(principal)
            }
            (_, "down") => Err(AuthError::Unavailable),
            _ => Err(AuthError::Rejected("no".into())),
        }
    }
}

struct Client {
    io: DuplexStream,
    buf: BytesMut,
    version: Option<Version>,
    ended: Arc<AtomicBool>,
}

impl Client {
    async fn send(&mut self, packet: Packet, version: Version) {
        let mut out = BytesMut::new();
        packet.encode(version, &mut out).unwrap();
        self.io.write_all(&out).await.unwrap();
    }

    async fn raw(&mut self, bytes: &[u8]) {
        let _ = self.io.write_all(bytes).await;
    }

    /// The next packet, or `None` once the server has closed the connection.
    async fn recv(&mut self) -> Option<Packet> {
        loop {
            if let Some(packet) = Packet::decode(
                &mut self.buf,
                self.version.or(Some(Version::V311)),
                &Limits::default(),
            )
            .unwrap()
            {
                return Some(packet);
            }
            let mut chunk = [0u8; 4096];
            match self.io.read(&mut chunk).await {
                Ok(0) | Err(_) => return None,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
            }
        }
    }

    async fn closed(&mut self) -> bool {
        self.recv().await.is_none()
    }

    /// Closed without waiting on any timer, which time paused would hide.
    async fn closed_at_once(&mut self) -> bool {
        let start = tokio::time::Instant::now();
        let closed = self.closed().await;
        closed && start.elapsed() < Duration::from_secs(1)
    }
}

struct Harness {
    client: Client,
    shutdown: CancellationToken,
    revoke: Arc<Notify>,
}

fn start(settings: Settings, expires_at: Option<i64>) -> Harness {
    let (client_io, server_io) = duplex(64 * 1024);
    let revoke = Arc::new(Notify::new());
    let service = MqttService::new(
        Arc::new(FakeAuth {
            expires_at,
            revoke: Arc::clone(&revoke),
        }),
        settings,
    );
    let shutdown = CancellationToken::new();
    let ended = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&ended);
    let token = shutdown.clone();
    tokio::spawn(async move {
        service.serve_connection(server_io, Vec::new(), token).await;
        flag.store(true, Ordering::SeqCst);
    });
    Harness {
        client: Client {
            io: client_io,
            buf: BytesMut::new(),
            version: None,
            ended,
        },
        shutdown,
        revoke,
    }
}

fn connect(version: Version) -> Connect {
    Connect {
        version,
        clean_start: true,
        keep_alive: 30,
        client_id: Utf8::new("dev-1").unwrap(),
        will: None,
        username: Some(Utf8::new("acme").unwrap()),
        password: Some(Bytes::from_static(b"good")),
        properties: Properties::default(),
    }
}

fn connack(packet: Option<Packet>) -> felix_mqtt_wire::ConnAck {
    match packet {
        Some(Packet::ConnAck(ack)) => ack,
        other => panic!("expected CONNACK, got {other:?}"),
    }
}

async fn connected(version: Version) -> Harness {
    let mut harness = start(Settings::default(), None);
    harness.client.version = Some(version);
    harness
        .client
        .send(Packet::Connect(connect(version)), version)
        .await;
    let ack = connack(harness.client.recv().await);
    assert_eq!(ack.reason, ReasonCode::Success);
    harness
}

fn publish(qos: QoS, packet_id: Option<u16>) -> Publish {
    Publish {
        dup: false,
        qos,
        retain: false,
        topic: Utf8::new("a/b").unwrap(),
        packet_id,
        properties: Properties::default(),
        payload: Bytes::from_static(b"x"),
    }
}

#[tokio::test(start_paused = true)]
async fn valid_token_connects_and_pings() {
    for version in [Version::V311, Version::V5] {
        let mut h = connected(version).await;
        h.client.send(Packet::PingReq, version).await;
        assert_eq!(h.client.recv().await, Some(Packet::PingResp));
        h.client
            .send(
                Packet::Disconnect(Disconnect::new(ReasonCode::Success)),
                version,
            )
            .await;
        assert!(h.client.closed().await);
    }
}

#[tokio::test(start_paused = true)]
async fn auth_is_required_by_default() {
    let mut h = start(Settings::default(), None);
    let mut c = connect(Version::V5);
    c.username = None;
    c.password = None;
    h.client.version = Some(Version::V5);
    h.client.send(Packet::Connect(c), Version::V5).await;
    let ack = connack(h.client.recv().await);
    assert_eq!(ack.reason, ReasonCode::NotAuthorized);
    assert!(h.client.closed().await);
}

#[tokio::test(start_paused = true)]
async fn bad_token_and_unreachable_control_plane_are_told_apart() {
    for (token, want) in [
        (&b"wrong"[..], ReasonCode::BadUserNameOrPassword),
        (&b"down"[..], ReasonCode::ServerUnavailable),
    ] {
        let mut h = start(Settings::default(), None);
        let mut c = connect(Version::V311);
        c.password = Some(Bytes::copy_from_slice(token));
        h.client.send(Packet::Connect(c), Version::V311).await;
        assert_eq!(connack(h.client.recv().await).reason, want);
        assert!(h.client.closed().await);
    }
}

#[tokio::test(start_paused = true)]
async fn anonymous_tenant_admits_only_connections_without_credentials() {
    let settings = Settings {
        anonymous_tenant: Some("dev".into()),
        ..Settings::default()
    };
    let mut h = start(settings.clone(), None);
    let mut c = connect(Version::V311);
    c.username = None;
    c.password = None;
    h.client.send(Packet::Connect(c), Version::V311).await;
    assert_eq!(connack(h.client.recv().await).reason, ReasonCode::Success);

    // A wrong token is not quietly downgraded to anonymous.
    let mut h = start(settings, None);
    let mut c = connect(Version::V311);
    c.password = Some(Bytes::from_static(b"wrong"));
    h.client.send(Packet::Connect(c), Version::V311).await;
    assert_eq!(
        connack(h.client.recv().await).reason,
        ReasonCode::BadUserNameOrPassword
    );
}

#[tokio::test(start_paused = true)]
async fn silent_connection_is_closed_at_the_auth_timeout() {
    let mut h = start(Settings::default(), None);
    tokio::time::sleep(Duration::from_secs(11)).await;
    assert!(h.client.closed().await);
}

#[tokio::test(start_paused = true)]
async fn oversized_packet_before_connect_closes_the_connection() {
    let mut h = start(Settings::default(), None);
    // A CONNECT whose remaining length is 70,000 bytes, announced and never sent.
    h.client.raw(&[0x10, 0xF0, 0xA2, 0x04]).await;
    assert!(h.client.closed_at_once().await);
}

#[tokio::test(start_paused = true)]
async fn first_packet_must_be_connect() {
    let mut h = start(Settings::default(), None);
    h.client.send(Packet::PingReq, Version::V311).await;
    assert!(h.client.closed_at_once().await);
}

#[tokio::test(start_paused = true)]
async fn unsupported_protocol_level_is_answered_then_closed() {
    let mut h = start(Settings::default(), None);
    h.client
        .raw(b"\x10\x0C\x00\x04MQTT\x03\x02\x00\x1e\x00\x00")
        .await;
    assert_eq!(
        connack(h.client.recv().await).reason,
        ReasonCode::UnsupportedProtocolVersion
    );
    assert!(h.client.closed().await);
}

#[tokio::test(start_paused = true)]
async fn a_will_is_refused_while_there_is_no_publish_path() {
    let mut h = start(Settings::default(), None);
    let mut c = connect(Version::V311);
    c.will = Some(Will {
        topic: Utf8::new("last/words").unwrap(),
        payload: Bytes::from_static(b"bye"),
        qos: QoS::AtMostOnce,
        retain: false,
        properties: Properties::default(),
    });
    h.client.send(Packet::Connect(c), Version::V311).await;
    assert_eq!(
        connack(h.client.recv().await).reason,
        ReasonCode::NotAuthorized
    );
    assert!(h.client.closed().await);
}

#[tokio::test(start_paused = true)]
async fn empty_client_id_is_assigned_under_5_and_clean_sessions_only_under_3_1_1() {
    let mut h = start(Settings::default(), None);
    h.client.version = Some(Version::V5);
    let mut c = connect(Version::V5);
    c.client_id = Utf8::new("").unwrap();
    h.client.send(Packet::Connect(c), Version::V5).await;
    let ack = connack(h.client.recv().await);
    assert_eq!(ack.reason, ReasonCode::Success);
    assert!(ack.properties.assigned_client_identifier.is_some());

    let mut h = start(Settings::default(), None);
    let mut c = connect(Version::V311);
    c.client_id = Utf8::new("").unwrap();
    c.clean_start = false;
    h.client.send(Packet::Connect(c), Version::V311).await;
    assert_eq!(
        connack(h.client.recv().await).reason,
        ReasonCode::ClientIdentifierNotValid
    );
}

#[tokio::test(start_paused = true)]
async fn keepalive_is_capped_and_a_missing_one_is_replaced() {
    let settings = Settings {
        keepalive_max_secs: 60,
        ..Settings::default()
    };
    for (asked, want) in [(0u16, Some(60u16)), (3600, Some(60)), (30, None)] {
        let mut h = start(settings.clone(), None);
        h.client.version = Some(Version::V5);
        let mut c = connect(Version::V5);
        c.keep_alive = asked;
        h.client.send(Packet::Connect(c), Version::V5).await;
        let ack = connack(h.client.recv().await);
        assert_eq!(ack.properties.server_keep_alive, want, "asked for {asked}");
    }
}

#[tokio::test(start_paused = true)]
async fn silence_past_one_and_a_half_keepalives_closes_the_connection() {
    let mut h = connected(Version::V311).await;
    // Keepalive 30 s: still open at 44 s, closed by 46 s.
    tokio::time::sleep(Duration::from_secs(44)).await;
    h.client.send(Packet::PingReq, Version::V311).await;
    assert_eq!(h.client.recv().await, Some(Packet::PingResp));
    tokio::time::sleep(Duration::from_secs(44)).await;
    assert!(!h.client.ended.load(Ordering::SeqCst));
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(h.client.closed().await);
}

#[tokio::test(start_paused = true)]
async fn publish_is_refused_per_message_under_5_and_by_closing_under_3_1_1() {
    let mut h = connected(Version::V5).await;
    h.client
        .send(
            Packet::Publish(publish(QoS::AtLeastOnce, Some(7))),
            Version::V5,
        )
        .await;
    match h.client.recv().await {
        Some(Packet::PubAck(ack)) => {
            assert_eq!(ack.packet_id, 7);
            assert_eq!(ack.reason, ReasonCode::NotAuthorized);
        }
        other => panic!("expected PUBACK, got {other:?}"),
    }
    h.client
        .send(
            Packet::Publish(publish(QoS::ExactlyOnce, Some(8))),
            Version::V5,
        )
        .await;
    assert!(
        matches!(h.client.recv().await, Some(Packet::PubRec(ack)) if ack.reason == ReasonCode::NotAuthorized)
    );
    h.client
        .send(Packet::Publish(publish(QoS::AtMostOnce, None)), Version::V5)
        .await;
    assert!(matches!(
        h.client.recv().await,
        Some(Packet::Disconnect(d)) if d.reason == ReasonCode::NotAuthorized
    ));
    assert!(h.client.closed().await);

    let mut h = connected(Version::V311).await;
    h.client
        .send(
            Packet::Publish(publish(QoS::AtLeastOnce, Some(1))),
            Version::V311,
        )
        .await;
    assert!(h.client.closed().await);
}

#[tokio::test(start_paused = true)]
async fn subscribe_is_refused_and_unsubscribe_finds_nothing() {
    for (version, want) in [
        (Version::V5, ReasonCode::NotAuthorized),
        (Version::V311, ReasonCode::UnspecifiedError),
    ] {
        let mut h = connected(version).await;
        let filters = vec![
            SubscribeFilter::new(Utf8::new("a/#").unwrap(), QoS::AtLeastOnce),
            SubscribeFilter::new(Utf8::new("b/+").unwrap(), QoS::AtMostOnce),
        ];
        h.client
            .send(
                Packet::Subscribe(Subscribe {
                    packet_id: 3,
                    filters,
                    properties: Properties::default(),
                }),
                version,
            )
            .await;
        match h.client.recv().await {
            Some(Packet::SubAck(ack)) => {
                assert_eq!(ack.packet_id, 3);
                assert_eq!(ack.reasons, vec![want, want]);
            }
            other => panic!("expected SUBACK, got {other:?}"),
        }
        h.client
            .send(
                Packet::Unsubscribe(Unsubscribe {
                    packet_id: 4,
                    filters: vec![Utf8::new("a/#").unwrap()],
                    properties: Properties::default(),
                }),
                version,
            )
            .await;
        match h.client.recv().await {
            Some(Packet::UnsubAck(ack)) => assert_eq!(ack.packet_id, 4),
            other => panic!("expected UNSUBACK, got {other:?}"),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_second_connect_is_a_protocol_error() {
    let mut h = connected(Version::V5).await;
    h.client
        .send(Packet::Connect(connect(Version::V5)), Version::V5)
        .await;
    assert!(matches!(
        h.client.recv().await,
        Some(Packet::Disconnect(d)) if d.reason == ReasonCode::ProtocolError
    ));
    assert!(h.client.closed().await);
}

#[tokio::test(start_paused = true)]
async fn connection_ends_when_the_token_expires() {
    let soon = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 20;
    let mut h = start(Settings::default(), Some(soon));
    h.client.version = Some(Version::V5);
    let mut c = connect(Version::V5);
    c.keep_alive = 600;
    h.client.send(Packet::Connect(c), Version::V5).await;
    assert_eq!(connack(h.client.recv().await).reason, ReasonCode::Success);
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert!(!h.client.ended.load(Ordering::SeqCst));
    assert!(matches!(
        h.client.recv().await,
        Some(Packet::Disconnect(d)) if d.reason == ReasonCode::NotAuthorized
    ));
}

#[tokio::test(start_paused = true)]
async fn connection_ends_when_the_credential_is_revoked() {
    let mut h = connected(Version::V5).await;
    tokio::task::yield_now().await;
    h.revoke.notify_waiters();
    assert!(matches!(
        h.client.recv().await,
        Some(Packet::Disconnect(d)) if d.reason == ReasonCode::NotAuthorized
    ));
}

#[tokio::test(start_paused = true)]
async fn shutdown_tells_a_5_client_and_closes() {
    let mut h = connected(Version::V5).await;
    h.shutdown.cancel();
    assert!(matches!(
        h.client.recv().await,
        Some(Packet::Disconnect(d)) if d.reason == ReasonCode::ServerShuttingDown
    ));
    assert!(h.client.closed().await);
}
