//! A member's certificate is its identity: a request that speaks for another
//! member, or comes with a revoked certificate, is refused, and no peer can
//! propose an import of the whole store.
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use rustls::pki_types::CertificateDer;
use tower::ServiceExt;

use super::{NullApp, TOKEN};
use crate::auth::cert_binding::PeerCertificates;
use crate::raft::{
    AppStateMachine, CLUSTER_ID_HEADER, MEMBER_URI_PREFIX, PEER_PREFIX, PeerSecurity, PeerTls,
    RaftHandle, RaftSettings,
};

/// A CA and, for each id, a certificate naming that member of `test-cluster`.
struct Pki {
    _dir: tempfile::TempDir,
    tls: PeerTls,
    members: Vec<CertificateDer<'static>>,
}

fn pki(ids: &[u64]) -> Pki {
    let dir = tempfile::tempdir().expect("tempdir");
    let ca_key = rcgen::KeyPair::generate().expect("ca key");
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca cert");
    let ca = rcgen::Issuer::new(ca_params, ca_key);
    let mut members = Vec::new();
    let mut own = None;
    for id in ids {
        let key = rcgen::KeyPair::generate().expect("key");
        let mut params =
            rcgen::CertificateParams::new(vec!["localhost".to_string()]).expect("params");
        params.subject_alt_names.push(rcgen::SanType::URI(
            format!("{MEMBER_URI_PREFIX}test-cluster:{id}")
                .try_into()
                .expect("uri"),
        ));
        let cert = params.signed_by(&key, &ca).expect("cert");
        if own.is_none() {
            own = Some((cert.pem(), key.serialize_pem()));
        }
        members.push(cert.der().clone());
    }
    let (cert, key) = own.expect("one member");
    let path = |name: &str| dir.path().join(name).to_string_lossy().into_owned();
    std::fs::write(path("ca.pem"), ca_cert.pem()).expect("write ca");
    std::fs::write(path("peer.pem"), cert).expect("write cert");
    std::fs::write(path("peer.key"), key).expect("write key");
    let tls = PeerTls::new(path("peer.pem"), path("peer.key"), path("ca.pem"));
    Pki {
        _dir: dir,
        tls,
        members,
    }
}

async fn member(tls: PeerTls, app: Arc<dyn AppStateMachine>) -> (tempfile::TempDir, RaftHandle) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut settings = RaftSettings::new(
        1,
        dir.path().into(),
        PeerSecurity {
            cluster_id: "test-cluster".to_string(),
            token: None,
            previous_token: None,
            next_token: None,
            tls: Some(tls),
        },
    );
    settings.write_timeout = Duration::from_millis(200);
    let handle = RaftHandle::start(settings, app).await.expect("start");
    (dir, handle)
}

/// Send `path` as the member whose certificate is `cert`.
async fn send(
    router: &axum::Router,
    cert: Option<&CertificateDer<'static>>,
    path: &str,
    body: Option<serde_json::Value>,
) -> StatusCode {
    let request = match &body {
        Some(_) => Request::post(format!("{PEER_PREFIX}/{path}"))
            .header("content-type", "application/json"),
        None => Request::get(format!("{PEER_PREFIX}/{path}")),
    }
    .header(CLUSTER_ID_HEADER, "test-cluster");
    let mut request = request
        .body(body.map_or_else(Body::empty, |body| Body::from(body.to_string())))
        .expect("request");
    if let Some(cert) = cert {
        request
            .extensions_mut()
            .insert(PeerCertificates(Arc::from(vec![cert.clone()])));
    }
    router
        .clone()
        .oneshot(request)
        .await
        .expect("response")
        .status()
}

fn vote_for(candidate: u64) -> serde_json::Value {
    serde_json::json!({
        "vote": {"term": 7, "voted_for": candidate, "committed": false},
        "last_log_id": null,
    })
}

#[tokio::test]
async fn a_member_presenting_another_members_id_is_refused() {
    let pki = pki(&[1, 2, 3]);
    let (_dir, handle) = member(pki.tls.clone(), Arc::new(NullApp)).await;
    let router = handle.rpc_router();
    let two = &pki.members[1];

    // Member 2 asking for a vote for member 3, or posing as it in standing.
    assert_eq!(
        send(&router, Some(two), "vote", Some(vote_for(3))).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        send(&router, Some(two), "standing?from=3", None).await,
        StatusCode::FORBIDDEN
    );
    let forming = serde_json::json!({"member": 3, "members": [1, 2, 3]});
    assert_eq!(
        send(&router, Some(two), "forming", Some(forming)).await,
        StatusCode::FORBIDDEN
    );
    // Replicating, or installing a snapshot, as leader member 3 or 1.
    let append = serde_json::json!({
        "vote": {"term": 7, "voted_for": 3, "committed": true},
        "prev_log_id": null,
        "entries": [],
        "leader_commit": null,
    });
    assert_eq!(
        send(&router, Some(two), "append-entries", Some(append)).await,
        StatusCode::FORBIDDEN
    );
    let pieces = super::snapshot::whole(7, 5, b"state");
    let mut request = Request::post(format!("{PEER_PREFIX}/install-snapshot"))
        .header(CLUSTER_ID_HEADER, "test-cluster")
        .body(Body::from_stream(futures_util::stream::iter(
            pieces.into_iter().map(Ok::<_, std::io::Error>),
        )))
        .expect("request");
    request
        .extensions_mut()
        .insert(PeerCertificates(Arc::from(vec![two.clone()])));
    let installed = router.clone().oneshot(request).await.expect("response");
    assert_eq!(installed.status(), StatusCode::FORBIDDEN);
    // Speaking for itself is served.
    assert_eq!(
        send(&router, Some(two), "standing?from=2", None).await,
        StatusCode::OK
    );
    assert_ne!(
        send(&router, Some(two), "vote", Some(vote_for(2))).await,
        StatusCode::FORBIDDEN
    );
    // No certificate, or one that names no member, identifies nobody.
    assert_eq!(
        send(&router, None, "standing", None).await,
        StatusCode::FORBIDDEN
    );
    let stranger = pki_without_member_uri();
    assert_eq!(
        send(&router, Some(&stranger), "standing", None).await,
        StatusCode::FORBIDDEN
    );
    handle.shutdown().await.expect("shutdown");
}

fn pki_without_member_uri() -> CertificateDer<'static> {
    let key = rcgen::KeyPair::generate().expect("key");
    rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .expect("params")
        .self_signed(&key)
        .expect("cert")
        .der()
        .clone()
}

#[tokio::test]
async fn a_revoked_members_certificate_is_refused() {
    let pki = pki(&[1, 2]);
    let tls = pki
        .tls
        .clone()
        .with_revoked([crate::raft::peer::fingerprint(&pki.members[1])].into());
    let (_dir, handle) = member(tls, Arc::new(NullApp)).await;
    let router = handle.rpc_router();
    assert_eq!(
        send(&router, Some(&pki.members[1]), "standing?from=2", None).await,
        StatusCode::FORBIDDEN
    );
    handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_member_must_not_start_under_another_members_certificate() {
    let pki = pki(&[2]);
    let security = PeerSecurity {
        cluster_id: "test-cluster".to_string(),
        token: None,
        previous_token: None,
        next_token: None,
        tls: Some(pki.tls.clone()),
    };
    assert!(security.check_own_identity(2).is_ok());
    let err = security.check_own_identity(1).expect_err("member 1");
    assert!(
        err.to_string().contains("issued to raft member 2"),
        "{err:#}"
    );
}

/// The metadata state machine's answer, on an otherwise empty app.
struct Metadata;

#[async_trait::async_trait]
impl AppStateMachine for Metadata {
    async fn apply(&self, _command: &[u8]) -> Vec<u8> {
        Vec::new()
    }
    async fn snapshot(&self) -> Vec<u8> {
        Vec::new()
    }
    async fn restore(&self, _snapshot: &[u8]) {}
    fn peer_may_propose(&self, command: &[u8], caller: Option<crate::raft::NodeId>) -> bool {
        crate::store::raft::command::peer_may_propose(command, caller)
    }
}

#[tokio::test]
async fn a_peer_cannot_propose_an_import() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut settings = RaftSettings::new(
        1,
        dir.path().into(),
        PeerSecurity {
            cluster_id: "test-cluster".to_string(),
            token: Some(TOKEN.to_string()),
            previous_token: None,
            next_token: None,
            tls: None,
        },
    );
    settings.write_timeout = Duration::from_millis(200);
    let handle = RaftHandle::start(settings, Arc::new(Metadata))
        .await
        .expect("start");
    let router = handle.rpc_router();
    let propose = async |body: &'static str| {
        let request = Request::post(format!("{PEER_PREFIX}/propose"))
            .header(CLUSTER_ID_HEADER, "test-cluster")
            .header("authorization", format!("Bearer {TOKEN}"))
            .body(Body::from(body))
            .expect("request");
        router
            .clone()
            .oneshot(request)
            .await
            .expect("response")
            .status()
    };
    assert_eq!(
        propose(r#"{"v":1,"op":"import_state","overwrite":true}"#).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(propose("not json").await, StatusCode::FORBIDDEN);
    // Any other command goes on to the log (no leader here, so 503).
    assert_eq!(
        propose(r#"{"v":1,"op":"create_tenant"}"#).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_peer_cannot_propose_audit_records_for_another_member() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut settings = RaftSettings::new(
        1,
        dir.path().into(),
        PeerSecurity {
            cluster_id: "test-cluster".to_string(),
            token: Some(TOKEN.to_string()),
            previous_token: None,
            next_token: None,
            tls: None,
        },
    );
    settings.write_timeout = Duration::from_millis(200);
    let handle = RaftHandle::start(settings, Arc::new(Metadata))
        .await
        .expect("start");
    let router = handle.rpc_router();
    // The member a certificate named, as `require_member` leaves it.
    let propose = async |caller: Option<u64>, member: &str| {
        let body = format!(
            r#"{{"v":1,"op":"record_audit","entries":[{{"at_ms":1,"member":"{member}","listener":"api","method":"POST","route":"/v1/x","path":"/v1/x","credential":"none","status":204}}]}}"#
        );
        let mut request = Request::post(format!("{PEER_PREFIX}/propose"))
            .header(CLUSTER_ID_HEADER, "test-cluster")
            .header("authorization", format!("Bearer {TOKEN}"))
            .body(Body::from(body))
            .expect("request");
        if let Some(caller) = caller {
            request
                .extensions_mut()
                .insert(crate::raft::peer::PeerMember(caller));
        }
        router
            .clone()
            .oneshot(request)
            .await
            .expect("response")
            .status()
    };
    assert_eq!(propose(Some(2), "3").await, StatusCode::FORBIDDEN);
    // Its own records go on to the log (no leader here, so 503).
    assert_eq!(propose(Some(2), "2").await, StatusCode::SERVICE_UNAVAILABLE);
    // No certificate names the caller: the name is the sender's word.
    assert_eq!(propose(None, "3").await, StatusCode::SERVICE_UNAVAILABLE);
    handle.shutdown().await.expect("shutdown");
}
