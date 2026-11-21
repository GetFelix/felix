//! Peer mTLS follows a rotation of its files: new handshakes on both sides
//! use the new certificate, connections already up keep working until their
//! certificate ends, and a rotated certificate that fails the startup checks
//! is refused with the old one kept.
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::super::{PeerSecurity, PeerTls, client_tls, fingerprint};
use crate::auth::cert_binding::PeerCertificates;

const CLUSTER: &str = "rot-cluster";

struct Ca {
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
    pem: String,
}

fn ca() -> Ca {
    let key = rcgen::KeyPair::generate().expect("ca key");
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let pem = params.self_signed(&key).expect("ca cert").pem();
    Ca {
        issuer: rcgen::Issuer::new(params, key),
        pem,
    }
}

/// Write a certificate naming member `id` into `dir`'s cert and key files,
/// replacing what is there. Returns its DER.
fn issue(ca: &Ca, dir: &Path, id: u64) -> CertificateDer<'static> {
    issue_with(ca, dir, id, |_| {})
}

/// [`issue`], with `edit` applied to the certificate first. It covers
/// `localhost` and ends in 2099 unless `edit` says otherwise.
fn issue_with(
    ca: &Ca,
    dir: &Path,
    id: u64,
    edit: impl FnOnce(&mut rcgen::CertificateParams),
) -> CertificateDer<'static> {
    let key = rcgen::KeyPair::generate().expect("key");
    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).expect("params");
    params.subject_alt_names.push(rcgen::SanType::URI(
        format!("felix:principal:raft-member:{CLUSTER}:{id}")
            .try_into()
            .expect("uri"),
    ));
    params.not_after = rcgen::date_time_ymd(2099, 1, 1);
    edit(&mut params);
    let cert = params.signed_by(&key, &ca.issuer).expect("cert");
    std::fs::write(dir.join("peer.pem"), cert.pem()).expect("write cert");
    std::fs::write(dir.join("peer.key"), key.serialize_pem()).expect("write key");
    cert.der().clone()
}

fn member_security(ca: &Ca, dir: &Path) -> PeerSecurity {
    std::fs::write(dir.join("ca.pem"), &ca.pem).expect("write ca");
    if !dir.join("revoked.txt").exists() {
        std::fs::write(dir.join("revoked.txt"), "").expect("revoked file");
    }
    let path = |name: &str| dir.join(name).display().to_string();
    PeerSecurity {
        cluster_id: CLUSTER.to_string(),
        token: None,
        previous_token: None,
        next_token: None,
        tls: Some(
            PeerTls::new(path("peer.pem"), path("peer.key"), path("ca.pem"))
                .with_revoked_file(Some(path("revoked.txt"))),
        ),
    }
}

/// Serve, through the member's own peer listener, a route answering the
/// fingerprint of the client certificate the connection was opened with.
async fn serve(security: &PeerSecurity) -> u16 {
    let router = axum::Router::new().route(
        "/who",
        axum::routing::get(
            |axum::Extension(certs): axum::Extension<PeerCertificates>| async move {
                fingerprint(&certs.0[0])
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let security = security.clone();
    drop(tokio::spawn(async move {
        security
            .serve(listener, router, tokio_util::sync::CancellationToken::new())
            .await
    }));
    port
}

type Stream = tokio_rustls::client::TlsStream<tokio::net::TcpStream>;

/// Open a connection as `security`'s member; the server's leaf fingerprint.
async fn connect(security: &PeerSecurity, port: u16) -> std::io::Result<(Stream, String)> {
    let live = security
        .tls
        .as_ref()
        .expect("tls")
        .live(CLUSTER)
        .expect("live");
    let config = client_tls(live, CLUSTER).expect("client tls");
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
    let stream = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(ServerName::try_from("localhost").expect("name"), tcp)
        .await?;
    let server = fingerprint(&stream.get_ref().1.peer_certificates().expect("server cert")[0]);
    Ok((stream, server))
}

/// One request on `stream`; the client fingerprint the server saw.
async fn who(stream: &mut Stream) -> String {
    stream
        .write_all(b"GET /who HTTP/1.1\r\nhost: localhost\r\n\r\n")
        .await
        .expect("write");
    let mut buf = vec![0; 4096];
    let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("answer in time")
        .expect("read");
    let text = String::from_utf8_lossy(&buf[..n]).into_owned();
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
    text.rsplit("\r\n\r\n")
        .next()
        .unwrap_or_default()
        .to_string()
}

#[tokio::test]
async fn a_rotation_reaches_new_handshakes_on_both_sides_and_old_connections_keep_working() {
    let ca = ca();
    let (dir1, dir2) = (
        tempfile::tempdir().expect("dir"),
        tempfile::tempdir().expect("dir"),
    );
    let a1 = fingerprint(&issue(&ca, dir1.path(), 1));
    let a2 = fingerprint(&issue(&ca, dir2.path(), 2));
    let (one, two) = (
        member_security(&ca, dir1.path()),
        member_security(&ca, dir2.path()),
    );
    let port = serve(&one).await;

    let (mut old, server) = connect(&two, port).await.expect("connect");
    assert_eq!(
        (server.as_str(), who(&mut old).await.as_str()),
        (a1.as_str(), a2.as_str())
    );

    // Rotate both members' files, and reload as the timer would.
    let b1 = fingerprint(&issue(&ca, dir1.path(), 1));
    let b2 = fingerprint(&issue(&ca, dir2.path(), 2));
    for security in [&one, &two] {
        security
            .tls
            .as_ref()
            .expect("tls")
            .live(CLUSTER)
            .expect("live")
            .reload();
    }

    let (mut new, server) = connect(&two, port).await.expect("connect after rotation");
    assert_eq!(server, b1, "the listener presents the rotated certificate");
    assert_eq!(
        who(&mut new).await,
        b2,
        "the client presents the rotated certificate"
    );
    assert_eq!(
        who(&mut old).await,
        a2,
        "the connection opened before keeps working"
    );
}

#[tokio::test]
async fn a_rotated_certificate_that_fails_the_startup_checks_is_refused_and_the_old_one_kept() {
    let ca = ca();
    let (dir1, dir2) = (
        tempfile::tempdir().expect("dir"),
        tempfile::tempdir().expect("dir"),
    );
    let a1 = fingerprint(&issue(&ca, dir1.path(), 1));
    issue(&ca, dir2.path(), 2);
    let (one, two) = (
        member_security(&ca, dir1.path()),
        member_security(&ca, dir2.path()),
    );
    one.check_own_identity(1).expect("member 1 starts");
    let port = serve(&one).await;
    let live = one.tls.as_ref().expect("tls").live(CLUSTER).expect("live");

    // Member 1's files now hold member 3's certificate.
    issue(&ca, dir1.path(), 3);
    live.reload();
    assert_eq!(
        connect(&two, port).await.expect("connect").1,
        a1,
        "another member's certificate"
    );

    // A certificate for member 1 that is already revoked.
    let revoked = fingerprint(&issue(&ca, dir1.path(), 1));
    std::fs::write(dir1.path().join("revoked.txt"), format!("{revoked}\n")).expect("revoke");
    live.reload();
    assert_eq!(
        connect(&two, port).await.expect("connect").1,
        a1,
        "a revoked certificate"
    );

    // A key that does not match the certificate.
    std::fs::write(
        dir1.path().join("peer.key"),
        rcgen::KeyPair::generate().expect("key").serialize_pem(),
    )
    .expect("write key");
    live.reload();
    assert_eq!(
        connect(&two, port).await.expect("connect").1,
        a1,
        "a mismatched key"
    );
    one.check_own_identity(1).expect("still member 1");
}

#[tokio::test]
async fn a_revocation_file_change_reaches_the_next_handshake() {
    let ca = ca();
    let (dir1, dir2) = (
        tempfile::tempdir().expect("dir"),
        tempfile::tempdir().expect("dir"),
    );
    let a1 = fingerprint(&issue(&ca, dir1.path(), 1));
    issue(&ca, dir2.path(), 2);
    for dir in [&dir1, &dir2] {
        std::fs::write(dir.path().join("revoked.txt"), "# none yet\n").expect("revoked file");
    }
    let (one, two) = (
        member_security(&ca, dir1.path()),
        member_security(&ca, dir2.path()),
    );
    let port = serve(&one).await;
    connect(&two, port).await.expect("member 1 is trusted");

    std::fs::write(
        dir2.path().join("revoked.txt"),
        format!("{a1} # member 1, wiped\n"),
    )
    .expect("revoke");
    two.tls
        .as_ref()
        .expect("tls")
        .live(CLUSTER)
        .expect("live")
        .reload();
    assert!(
        connect(&two, port).await.is_err(),
        "member 2 refuses member 1's revoked certificate"
    );
}

#[tokio::test]
async fn a_connection_closes_when_its_client_certificate_ends() {
    let ca = ca();
    let (dir1, dir2) = (
        tempfile::tempdir().expect("dir"),
        tempfile::tempdir().expect("dir"),
    );
    issue(&ca, dir1.path(), 1);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock");
    issue_with(&ca, dir2.path(), 2, |params| {
        params.not_after = rcgen::date_time_ymd(1970, 1, 1) + now + Duration::from_secs(4);
    });
    let (one, two) = (
        member_security(&ca, dir1.path()),
        member_security(&ca, dir2.path()),
    );
    let port = serve(&one).await;
    let (mut stream, _) = connect(&two, port).await.expect("connect");
    who(&mut stream).await;
    let mut buf = [0; 64];
    let read = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut buf))
        .await
        .expect("the listener closes the connection by the certificate's end");
    assert!(matches!(read, Ok(0) | Err(_)), "{read:?}");
}

/// Member 1 serves under `a1`, then each bad file is written in turn and
/// reloaded; member 2 must still see `a1`.
async fn assert_still_serving(
    live: &super::super::live::LivePeerTls,
    two: &PeerSecurity,
    port: u16,
    a1: &str,
    why: &str,
) {
    live.reload();
    assert_eq!(connect(two, port).await.expect("connect").1, a1, "{why}");
}

#[tokio::test]
async fn a_rotated_certificate_that_would_not_verify_is_refused_and_the_old_one_kept() {
    let ca = ca();
    let (dir1, dir2) = (
        tempfile::tempdir().expect("dir"),
        tempfile::tempdir().expect("dir"),
    );
    let a1 = fingerprint(&issue(&ca, dir1.path(), 1));
    issue(&ca, dir2.path(), 2);
    let (one, two) = (
        member_security(&ca, dir1.path()),
        member_security(&ca, dir2.path()),
    );
    let port = serve(&one).await;
    let live = one.tls.as_ref().expect("tls").live(CLUSTER).expect("live");

    issue_with(&ca, dir1.path(), 1, |params| {
        params.not_after = rcgen::date_time_ymd(2020, 1, 1);
    });
    assert_still_serving(&live, &two, port, &a1, "an expired certificate").await;

    issue(&self::ca(), dir1.path(), 1);
    assert_still_serving(&live, &two, port, &a1, "a certificate from another CA").await;

    issue_with(&ca, dir1.path(), 1, |params| {
        params.subject_alt_names[0] =
            rcgen::SanType::DnsName("elsewhere.test".try_into().expect("dns"));
    });
    assert_still_serving(&live, &two, port, &a1, "a certificate for another host").await;
}

#[tokio::test]
async fn a_ca_rotation_rolls_over_without_a_gap_and_a_bundle_dropping_the_live_root_is_refused() {
    let (old_ca, new_ca) = (ca(), ca());
    let (dir1, dir2) = (
        tempfile::tempdir().expect("dir"),
        tempfile::tempdir().expect("dir"),
    );
    let a1 = fingerprint(&issue(&old_ca, dir1.path(), 1));
    issue(&old_ca, dir2.path(), 2);
    let (one, two) = (
        member_security(&old_ca, dir1.path()),
        member_security(&old_ca, dir2.path()),
    );
    let port = serve(&one).await;
    let reload = |security: &PeerSecurity| {
        security
            .tls
            .as_ref()
            .expect("tls")
            .live(CLUSTER)
            .expect("live")
            .reload();
    };

    // Dropping the root this member's own certificate chains to is refused.
    let live = one.tls.as_ref().expect("tls").live(CLUSTER).expect("live");
    std::fs::write(dir1.path().join("ca.pem"), &new_ca.pem).expect("write ca");
    assert_still_serving(&live, &two, port, &a1, "a bundle without the live root").await;

    // Both roots everywhere, then new leaves, then the new root alone.
    let both = format!("{}{}", old_ca.pem, new_ca.pem);
    for dir in [&dir1, &dir2] {
        std::fs::write(dir.path().join("ca.pem"), &both).expect("write ca");
    }
    reload(&one);
    reload(&two);
    let b1 = fingerprint(&issue(&new_ca, dir1.path(), 1));
    reload(&one);
    assert_eq!(
        connect(&two, port)
            .await
            .expect("old-CA client to new-CA server")
            .1,
        b1
    );
    issue(&new_ca, dir2.path(), 2);
    reload(&two);
    for dir in [&dir1, &dir2] {
        std::fs::write(dir.path().join("ca.pem"), &new_ca.pem).expect("write ca");
    }
    reload(&one);
    reload(&two);
    let (mut stream, server) = connect(&two, port).await.expect("new CA only");
    assert_eq!(server, b1);
    who(&mut stream).await;
}

#[tokio::test]
async fn a_revocation_file_read_back_blank_keeps_the_list() {
    let ca = ca();
    let (dir1, dir2) = (
        tempfile::tempdir().expect("dir"),
        tempfile::tempdir().expect("dir"),
    );
    let a1 = fingerprint(&issue(&ca, dir1.path(), 1));
    issue(&ca, dir2.path(), 2);
    std::fs::write(dir2.path().join("revoked.txt"), format!("{a1}\n")).expect("revoke");
    let (one, two) = (
        member_security(&ca, dir1.path()),
        member_security(&ca, dir2.path()),
    );
    let port = serve(&one).await;
    let live = two.tls.as_ref().expect("tls").live(CLUSTER).expect("live");
    assert!(connect(&two, port).await.is_err(), "revoked at startup");

    // Caught between truncate and write.
    std::fs::write(dir2.path().join("revoked.txt"), "").expect("truncate");
    live.reload();
    assert!(connect(&two, port).await.is_err(), "still revoked");

    // A comment clears it on purpose.
    std::fs::write(dir2.path().join("revoked.txt"), "# none\n").expect("clear");
    live.reload();
    connect(&two, port).await.expect("cleared");
}
