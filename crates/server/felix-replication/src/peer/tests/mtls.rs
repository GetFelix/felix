//! Peer authentication: who may connect to the internal listener, and as whom.

use std::path::{Path, PathBuf};

use super::*;
use crate::peer::config::PeerTlsConfig;
use crate::peer::tls::PeerTls;

/// A CA and the certificates it issued, on disk the way a deployment
/// mounts them. Generated per test so nothing in the repo looks like a
/// real credential.
struct Pki {
    dir: tempfile::TempDir,
    ca: rcgen::Issuer<'static, rcgen::KeyPair>,
    ca_path: PathBuf,
}

impl Pki {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let ca_key = rcgen::KeyPair::generate().expect("ca key");
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let cert = params.self_signed(&ca_key).expect("ca cert");
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&ca_path, cert.pem()).expect("write ca");
        Self {
            ca: rcgen::Issuer::new(params, ca_key),
            dir,
            ca_path,
        }
    }

    /// Issue a certificate for `name`, valid now, under `label`.
    fn issue(&self, label: &str, name: &str) -> PeerTlsConfig {
        self.issue_with(label, name, |_| {})
    }

    fn issue_with(
        &self,
        label: &str,
        name: &str,
        adjust: impl FnOnce(&mut rcgen::CertificateParams),
    ) -> PeerTlsConfig {
        let key = rcgen::KeyPair::generate().expect("key");
        let mut params = rcgen::CertificateParams::new(vec![name.to_string()]).expect("params");
        adjust(&mut params);
        let cert = params.signed_by(&key, &self.ca).expect("sign");
        let cert_path = self.dir.path().join(format!("{label}.pem"));
        let key_path = self.dir.path().join(format!("{label}.key.pem"));
        std::fs::write(&cert_path, cert.pem()).expect("write cert");
        std::fs::write(&key_path, key.serialize_pem()).expect("write key");
        PeerTlsConfig {
            cert_path: cert_path.display().to_string(),
            key_path: key_path.display().to_string(),
            ca_path: self.ca_path.display().to_string(),
        }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }
}

/// A TLS refusal is seen by the dialler as a handshake that did not
/// complete, or as a connection the listener closed before answering
/// `Hello`. Either way no request was served.
fn refused_at_handshake(err: &PeerError) -> bool {
    matches!(
        err,
        PeerError::Unavailable { .. }
            | PeerError::Handshake { .. }
            | PeerError::Disconnected { .. }
    )
}

fn tls(paths: &PeerTlsConfig) -> Arc<PeerTls> {
    Arc::new(PeerTls::load(paths).expect("load peer tls"))
}

async fn listener(node_id: &str, tls: Option<Arc<PeerTls>>) -> Listener {
    let handler: Arc<dyn PeerRequestHandler> = Arc::new(CountingHandler::default());
    let mut last = None;
    for _ in 0..100 {
        match PeerServer::bind_with_tls(
            node_id.to_string(),
            &config(),
            handler.clone(),
            tls.clone(),
        ) {
            Ok(server) => {
                let addr = server.local_addr().expect("addr");
                let shutdown = CancellationToken::new();
                let task = tokio::spawn(server.serve(shutdown.clone()));
                return Listener {
                    addr,
                    shutdown,
                    task,
                };
            }
            Err(err) => {
                last = Some(err);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    panic!("bind never succeeded: {:#}", last.expect("an error"));
}

fn pool_as(node_id: &str, tls: Option<Arc<PeerTls>>) -> Arc<PeerPool> {
    PeerPool::new_with_tls(node_id.to_string(), config(), CancellationToken::new(), tls)
        .expect("pool")
}

/// The whole thing working: both ends hold certificates from the CA
/// issued to their node ids, and a forward goes through.
#[tokio::test]
async fn peers_with_certificates_from_the_ca_forward_to_each_other() {
    let pki = Pki::new();
    let server = listener(PEER, Some(tls(&pki.issue("b", PEER)))).await;
    let pool = pool_as("broker-a", Some(tls(&pki.issue("a", "broker-a"))));

    let answer = pool
        .request(PEER, server.addr, forward())
        .await
        .expect("an authenticated peer is served");
    assert!(matches!(answer, InternalMessage::ForwardPublishOk(_)));

    pool.shutdown().await;
    server.stop().await;
}

/// **No certificate, no connection.** A dialler in the unauthenticated
/// mode -- what a broker that predates mTLS, or anything that is not a
/// broker, presents -- is refused in the handshake, before any frame.
#[tokio::test]
async fn a_peer_without_a_certificate_is_refused() {
    let pki = Pki::new();
    let server = listener(PEER, Some(tls(&pki.issue("b", PEER)))).await;
    let pool = pool_as("broker-a", None);

    let err = pool
        .request(PEER, server.addr, forward())
        .await
        .expect_err("a connection with no client certificate was accepted");
    assert!(
        refused_at_handshake(&err),
        "refused in the handshake, not answered: {err:?}"
    );

    pool.shutdown().await;
    server.stop().await;
}

/// A certificate from a CA the listener does not trust is not a
/// credential, however well-formed.
#[tokio::test]
async fn a_certificate_from_another_ca_is_refused() {
    let pki = Pki::new();
    let other = Pki::new();
    let server = listener(PEER, Some(tls(&pki.issue("b", PEER)))).await;
    // Trusts the right CA, so the *server* verifies; presents a certificate
    // the server's CA never issued.
    let mut paths = other.issue("a", "broker-a");
    paths.ca_path = pki.ca_path.display().to_string();
    let pool = pool_as("broker-a", Some(tls(&paths)));

    let err = pool
        .request(PEER, server.addr, forward())
        .await
        .expect_err("a certificate from an untrusted CA was accepted");
    assert!(refused_at_handshake(&err), "{err:?}");

    pool.shutdown().await;
    server.stop().await;
}

/// An expired certificate is refused like an untrusted one.
#[tokio::test]
async fn an_expired_certificate_is_refused() {
    let pki = Pki::new();
    let server = listener(PEER, Some(tls(&pki.issue("b", PEER)))).await;
    let expired = pki.issue_with("a", "broker-a", |params| {
        params.not_before = rcgen::date_time_ymd(2020, 1, 1);
        params.not_after = rcgen::date_time_ymd(2020, 1, 2);
    });
    let pool = pool_as("broker-a", Some(tls(&expired)));

    let err = pool
        .request(PEER, server.addr, forward())
        .await
        .expect_err("an expired certificate was accepted");
    assert!(refused_at_handshake(&err), "{err:?}");

    pool.shutdown().await;
    server.stop().await;
}

/// **The certificate's name is the identity.** A peer holding a valid
/// certificate for one node id cannot claim another in `Hello`: the
/// handshake proves it is *a* broker, the name check proves *which*.
#[tokio::test]
async fn a_peer_cannot_claim_a_node_id_its_certificate_does_not_carry() {
    let pki = Pki::new();
    let server = listener(PEER, Some(tls(&pki.issue("b", PEER)))).await;
    // A real certificate from the right CA, issued to broker-c, presented
    // by a pool that says it is broker-a.
    let pool = pool_as("broker-a", Some(tls(&pki.issue("c", "broker-c"))));

    let err = pool
        .request(PEER, server.addr, forward())
        .await
        .expect_err("a peer was served under a name its certificate does not carry");
    assert!(refused_at_handshake(&err), "{err:?}");

    pool.shutdown().await;
    server.stop().await;
}

/// The other direction: a dialler verifies the listener's certificate
/// against the node id it meant to reach, so a catalog entry that points
/// at the wrong broker fails in the handshake.
#[tokio::test]
async fn a_listener_is_verified_against_the_node_id_being_dialled() {
    let pki = Pki::new();
    // The listener is broker-b, with broker-b's certificate.
    let server = listener(PEER, Some(tls(&pki.issue("b", PEER)))).await;
    let pool = pool_as("broker-a", Some(tls(&pki.issue("a", "broker-a"))));

    // Dialled as broker-c at broker-b's address.
    let err = pool
        .request("broker-c", server.addr, forward())
        .await
        .expect_err("the wrong broker was accepted as the one dialled");
    assert!(refused_at_handshake(&err), "{err:?}");

    // And correctly as broker-b it works, on the same pool.
    pool.request(PEER, server.addr, forward())
        .await
        .expect("the right broker is served");

    pool.shutdown().await;
    server.stop().await;
}

/// **Rotation is a file swap.** Replacing the certificate on disk and
/// reloading changes what the *next* handshake presents; a connection
/// already up keeps working.
#[tokio::test]
async fn a_rotated_certificate_is_used_by_new_connections_without_dropping_old_ones() {
    let pki = Pki::new();
    let server_tls = tls(&pki.issue("b", PEER));
    let server = listener(PEER, Some(Arc::clone(&server_tls))).await;

    // The dialler's identity starts out issued to broker-c, so the
    // listener refuses it as broker-a ...
    let paths = pki.issue("a", "broker-c");
    let dialler_tls = tls(&paths);
    let pool = pool_as("broker-a", Some(Arc::clone(&dialler_tls)));
    pool.request(PEER, server.addr, forward())
        .await
        .expect_err("the pre-rotation certificate was accepted for the wrong name");

    // ... until the files are replaced with a certificate for broker-a
    // and reloaded. Same paths, new material: what a cert-manager renewal
    // looks like from the broker's side.
    let rotated = pki.issue("a", "broker-a");
    assert_eq!(rotated.cert_path, paths.cert_path);
    assert!(
        dialler_tls.reload().expect("reload"),
        "the new certificate was not noticed"
    );
    assert!(
        !dialler_tls.reload().expect("reload"),
        "an unchanged certificate was reported as rotated"
    );

    // The pool's endpoint is unchanged; only the resolver's answer moved.
    // The refused attempt above put the peer into reconnect backoff, so
    // give the pool a few tries.
    let mut last = None;
    for _ in 0..50 {
        match pool.request(PEER, server.addr, forward()).await {
            Ok(_) => {
                last = None;
                break;
            }
            Err(err) => {
                last = Some(err);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    assert!(
        last.is_none(),
        "the rotated certificate was not presented on a new handshake: {last:?}"
    );

    pool.shutdown().await;
    server.stop().await;
    let _ = pki.path();
}

/// Missing or mismatched material is refused at load, before anything
/// binds -- and the key never appears in the error.
#[test]
fn unreadable_material_fails_at_load_without_leaking_the_key() {
    let pki = Pki::new();
    let mut paths = pki.issue("a", "broker-a");
    let key_pem = std::fs::read_to_string(&paths.key_path).expect("key");

    paths.key_path = pki.path().join("missing.key.pem").display().to_string();
    let err = PeerTls::load(&paths).expect_err("a missing key loaded");
    assert!(
        format!("{err:#}").contains("FELIX_INTERNAL_TLS_KEY"),
        "{err:#}"
    );

    // A key that does not belong to the certificate.
    let other = pki.issue("x", "broker-x");
    paths.key_path = other.key_path;
    let err = PeerTls::load(&paths).expect_err("a mismatched key loaded");
    let rendered = format!("{err:#}");
    assert!(rendered.contains("does not match"), "{rendered}");
    assert!(
        !rendered.contains(key_pem.trim()),
        "the key reached an error message"
    );
}
