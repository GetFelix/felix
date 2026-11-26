//! Who may speak Raft to this member, and how this member proves it is one.
//!
//! The Raft routes are served on their own listener, and every request must
//! name the cluster. With peer mTLS the certificate is the member's identity:
//! it must carry the URI SAN `felix:principal:raft-member:<cluster>:<id>`, and
//! a vote, append, snapshot or forming record is refused unless the member it
//! speaks for is that id ([`require_member`], [`PeerMember::refusal`]). So one
//! member's key does not let it speak as another, and one member's
//! certificate can be revoked on its own (`FELIX_RAFT_TLS_REVOKED`).
//!
//! The shared peer token (`FELIX_RAFT_PEER_TOKEN`) is the fallback for a group
//! without peer TLS. It says only that the caller is some member, never which.
//! No peer request can replace the whole store: an import comes from the
//! operator's bootstrap route only, and `propose` refuses one.
//!
//! A Raft RPC also names the member it is meant for, and a member answers
//! only its own ([`require_target`]).
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Carries the sender's cluster id on every peer request.
pub const CLUSTER_ID_HEADER: &str = "x-felix-raft-cluster-id";

/// Carries the id of the member a Raft RPC is meant for.
pub const TARGET_HEADER: &str = "x-felix-raft-target";

/// How this member authenticates Raft traffic, both directions.
#[derive(Clone)]
pub struct PeerSecurity {
    /// Names the group. A member only talks to peers that name the same one,
    /// and refuses to start on a data dir recorded under another.
    pub cluster_id: String,
    /// The shared peer secret. `None` only when the operator explicitly opted
    /// out (`FELIX_RAFT_INSECURE_PEERS`); config validation enforces that.
    pub token: Option<String>,
    /// The token being retired (`FELIX_RAFT_PEER_TOKEN_PREVIOUS`). Accepted on
    /// inbound requests, never sent.
    pub previous_token: Option<String>,
    /// The token being rolled in (`FELIX_RAFT_PEER_TOKEN_NEXT`). Accepted on
    /// inbound requests, never sent.
    pub next_token: Option<String>,
    /// mTLS for the peer listener and the peer client.
    pub tls: Option<PeerTls>,
}

impl std::fmt::Debug for PeerSecurity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerSecurity")
            .field("cluster_id", &self.cluster_id)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field(
                "previous_token",
                &self.previous_token.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "next_token",
                &self.next_token.as_ref().map(|_| "<redacted>"),
            )
            .field("tls", &self.tls)
            .finish()
    }
}

/// PEM files for peer mTLS. Every member presents `cert_path` as both server
/// and client, and trusts only certificates signed by `ca_path`. The files
/// are re-read on a timer once first used ([`live`]).
#[derive(Debug, Clone)]
pub struct PeerTls {
    cert_path: String,
    key_path: String,
    ca_path: String,
    /// SHA-256 fingerprints (lowercase hex) of member certificates that are
    /// refused even though they chain to the CA: a member that was removed,
    /// or an earlier incarnation of one whose machine was wiped.
    revoked: std::collections::BTreeSet<String>,
    /// A file of more such fingerprints (`FELIX_RAFT_TLS_REVOKED_FILE`),
    /// re-read with the certificates.
    revoked_file: Option<String>,
    /// Loaded on first use, then shared by every clone, so the listener and
    /// all peer clients see one rotation.
    live: Arc<std::sync::Mutex<Option<Arc<live::LivePeerTls>>>>,
}

impl PeerTls {
    /// Peer mTLS from these files, with nothing revoked.
    pub fn new(cert_path: String, key_path: String, ca_path: String) -> Self {
        Self {
            cert_path,
            key_path,
            ca_path,
            revoked: std::collections::BTreeSet::new(),
            revoked_file: None,
            live: Arc::default(),
        }
    }

    /// Where this member's certificate is read from.
    pub fn cert_path(&self) -> &str {
        &self.cert_path
    }

    /// Where this member's key is read from.
    pub fn key_path(&self) -> &str {
        &self.key_path
    }

    /// Where the cluster CA is read from.
    pub fn ca_path(&self) -> &str {
        &self.ca_path
    }

    /// Refuse these certificate fingerprints (SHA-256, lowercase hex).
    pub fn with_revoked(self, revoked: std::collections::BTreeSet<String>) -> Self {
        Self {
            revoked,
            live: Arc::default(),
            ..self
        }
    }

    /// Also refuse the fingerprints listed in `path`, re-read on rotation.
    pub fn with_revoked_file(self, path: Option<String>) -> Self {
        Self {
            revoked_file: path,
            live: Arc::default(),
            ..self
        }
    }

    /// The live material, read on the first call.
    fn live(&self, cluster_id: &str) -> Result<Arc<live::LivePeerTls>> {
        let mut slot = self
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(live) = &*slot {
            return Ok(Arc::clone(live));
        }
        let live = live::LivePeerTls::load(self, cluster_id)?;
        *slot = Some(Arc::clone(&live));
        Ok(live)
    }
}

/// The URI SAN prefix naming a Raft member, after which come the cluster id,
/// a colon, and the member id.
pub const MEMBER_URI_PREFIX: &str = "felix:principal:raft-member:";

/// The member a peer request comes from, as its certificate says. Only set
/// under peer mTLS; without it nothing says which member is calling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PeerMember(pub(super) super::NodeId);

impl PeerMember {
    /// The refusal for a request that speaks for a member other than the
    /// caller, or `None` when it may go on.
    pub(super) fn refusal(caller: Option<&Self>, claimed: super::NodeId) -> Option<Response> {
        match caller {
            Some(Self(id)) if *id != claimed => {
                metrics::counter!("felix_meta_raft_peer_rejected_total", "reason" => "member")
                    .increment(1);
                tracing::warn!(
                    caller = id,
                    claimed,
                    "raft peer request speaking for another member refused"
                );
                Some(
                    (
                        StatusCode::FORBIDDEN,
                        format!("raft member {id} may not speak for member {claimed}"),
                    )
                        .into_response(),
                )
            }
            _ => None,
        }
    }
}

/// The member ids `leaf` is issued to in `cluster_id`, from its URI SANs.
fn members_named(leaf: &[u8], cluster_id: &str) -> Result<Vec<super::NodeId>, String> {
    let prefix = format!("{MEMBER_URI_PREFIX}{cluster_id}:");
    let names =
        felix_common::tls::uri_sans_after(leaf, &prefix).map_err(|err| err.describe("peer"))?;
    Ok(names.iter().filter_map(|id| id.parse().ok()).collect())
}

/// The one member `leaf` is issued to. A certificate naming none, or more
/// than one, identifies nobody.
fn member_of(leaf: &[u8], cluster_id: &str) -> Result<super::NodeId, String> {
    match members_named(leaf, cluster_id)?[..] {
        [id] => Ok(id),
        [] => Err(format!(
            "the peer certificate names no member of {cluster_id} (no {MEMBER_URI_PREFIX}{cluster_id}:<id> URI SAN)"
        )),
        _ => Err("the peer certificate names more than one member".to_string()),
    }
}

/// The SHA-256 fingerprint of a DER certificate, as `FELIX_RAFT_TLS_REVOKED`
/// lists it.
pub(super) fn fingerprint(der: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(der))
}

/// The client side of peer mTLS: this member's certificate, and a verifier
/// that trusts only the cluster CA and refuses a revoked member's server.
/// Both follow rotation.
fn client_tls(live: Arc<live::LivePeerTls>, cluster_id: &str) -> Result<rustls::ClientConfig> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let identity = Arc::clone(&live) as Arc<dyn rustls::client::ResolvesClientCert>;
    let verifier = MemberServerVerifier {
        live,
        cluster_id: cluster_id.to_string(),
    };
    // Only the cluster CA: a peer certificate from a public CA must not be
    // enough to receive the log.
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context("raft peer TLS versions")?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_client_cert_resolver(identity);
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

/// Verifies a peer's server certificate as the CA does, then refuses one
/// that is revoked or names no member of this cluster, so a wiped member's
/// old key cannot answer for it. Which member it names is not checked
/// against the target yet (#2912).
struct MemberServerVerifier {
    live: Arc<live::LivePeerTls>,
    cluster_id: String,
}

impl std::fmt::Debug for MemberServerVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemberServerVerifier")
            .field("cluster_id", &self.cluster_id)
            .finish()
    }
}

impl rustls::client::danger::ServerCertVerifier for MemberServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let verified = self.live.trust().server_verifier().verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )?;
        if self.live.is_revoked(end_entity) {
            metrics::counter!("felix_meta_raft_peer_rejected_total", "reason" => "revoked_server")
                .increment(1);
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::Revoked,
            ));
        }
        member_of(end_entity, &self.cluster_id)
            .map_err(|why| rustls::Error::General(format!("raft peer server: {why}")))?;
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.live
            .trust()
            .server_verifier()
            .verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.live
            .trust()
            .server_verifier()
            .verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.live
            .trust()
            .server_verifier()
            .supported_verify_schemes()
    }
}

impl PeerSecurity {
    /// URL scheme for reaching a peer.
    pub fn scheme(&self) -> &'static str {
        if self.tls.is_some() { "https" } else { "http" }
    }

    /// An HTTP client that authenticates as a peer on every request.
    pub fn client(&self, timeout: Duration) -> Result<reqwest::Client> {
        let mut builder = reqwest::Client::builder()
            .timeout(timeout)
            .default_headers(self.peer_headers()?);
        if let Some(tls) = &self.tls {
            let live = tls.live(&self.cluster_id)?;
            builder = builder.tls_backend_preconfigured(client_tls(live, &self.cluster_id)?);
        }
        builder.build().context("build raft peer client")
    }

    /// The headers every outbound peer request carries. Only the current
    /// token is sent; the rotation tokens are for checking inbound requests.
    fn peer_headers(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(
            CLUSTER_ID_HEADER,
            HeaderValue::from_str(&self.cluster_id).context("cluster id is not a valid header")?,
        );
        if let Some(token) = &self.token {
            let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
                .context("peer token is not a valid header")?;
            value.set_sensitive(true);
            headers.insert(header::AUTHORIZATION, value);
        }
        Ok(headers)
    }

    /// Which configured token `offered` is, as the metric label, or `None`.
    ///
    /// Every slot is compared, and on SHA-256 digests, so the time taken
    /// says neither which slot matched nor how long any token is.
    pub(super) fn match_token(&self, offered: &str) -> Option<&'static str> {
        use sha2::{Digest, Sha256};
        let offered = Sha256::digest(offered.as_bytes());
        let mut matched = None;
        for (label, token) in [
            ("current", &self.token),
            ("next", &self.next_token),
            ("previous", &self.previous_token),
        ] {
            // An empty slot is unset, so an empty bearer can never match it.
            let hit = token.as_ref().is_some_and(|token| {
                let expected = Sha256::digest(token.as_bytes());
                !token.is_empty() & crate::api::bootstrap::constant_time_eq(&offered, &expected)
            });
            if hit && matched.is_none() {
                matched = Some(label);
            }
        }
        matched
    }

    /// Refuse to serve under a certificate that does not name this member, or
    /// is revoked: the other members would refuse every request it sends.
    /// The same checks hold for every rotated certificate.
    pub(super) fn check_own_identity(&self, id: super::NodeId) -> Result<()> {
        let Some(tls) = &self.tls else {
            return Ok(());
        };
        let live = tls
            .live(&self.cluster_id)
            .map_err(|err| anyhow::anyhow!("{err:#}"))?;
        anyhow::ensure!(
            live.member == id,
            "FELIX_RAFT_TLS_CERT is issued to raft member {}, but this is member {id}",
            live.member
        );
        Ok(())
    }

    /// Server-side TLS for the peer listener, when mTLS is configured, and
    /// when each connection must close because the client's certificate
    /// ends.
    pub(super) fn server_tls(
        &self,
    ) -> Result<Option<(Arc<rustls::ServerConfig>, crate::server::tls::CloseAt)>> {
        let Some(tls) = &self.tls else {
            return Ok(None);
        };
        let live = tls.live(&self.cluster_id)?;
        let config = crate::server::tls::mtls_server_config(
            Arc::clone(&live) as Arc<dyn rustls::server::ResolvesServerCert>,
            Arc::clone(&live) as Arc<dyn rustls::server::danger::ClientCertVerifier>,
        )
        .context("raft peer TLS")?;
        let close_at: crate::server::tls::CloseAt = Arc::new(move |chain| live.close_at(chain));
        Ok(Some((config, close_at)))
    }

    /// Serve `router` on `listener` until `shutdown`: over mTLS when it is
    /// configured, closing each connection when its client certificate
    /// ends.
    pub(super) async fn serve(
        &self,
        listener: tokio::net::TcpListener,
        router: axum::Router,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        match self.server_tls()? {
            Some((tls, close_at)) => {
                crate::server::tls::serve_tls(
                    "raft",
                    listener,
                    router,
                    tls,
                    Some(close_at),
                    shutdown,
                )
                .await;
            }
            None => {
                axum::serve(listener, router.into_make_service())
                    .with_graceful_shutdown(async move { shutdown.cancelled().await })
                    .await
                    .context("serve raft peer listener")?;
            }
        }
        Ok(())
    }

    /// Whether `der` is a revoked member certificate, as of now.
    fn is_revoked(&self, der: &[u8]) -> bool {
        self.tls
            .as_ref()
            .and_then(|tls| tls.live(&self.cluster_id).ok())
            .is_some_and(|live| live.is_revoked(der))
    }
}

/// Refuse any request that does not name this cluster and carry the token.
///
/// The current token, `FELIX_RAFT_PEER_TOKEN_NEXT` and
/// `FELIX_RAFT_PEER_TOKEN_PREVIOUS` are all accepted, so a rotation rolls
/// across the members with no step at which two of them cannot talk.
/// `felix_meta_raft_peer_token_accepted_total` says which one each request
/// used, which is how an operator knows the old token is no longer in use.
///
/// The cluster id is checked first and answered with 403 rather than 401:
/// a peer from another group holds a valid token for *its* group, and the
/// operator reading the log needs to know the members are cross-wired rather
/// than that a secret is wrong.
pub(super) async fn require_peer(
    State(security): State<std::sync::Arc<PeerSecurity>>,
    request: Request,
    next: Next,
) -> Response {
    let headers = request.headers();
    let cluster = headers
        .get(CLUSTER_ID_HEADER)
        .and_then(|value| value.to_str().ok());
    if cluster != Some(security.cluster_id.as_str()) {
        metrics::counter!("felix_meta_raft_peer_rejected_total", "reason" => "cluster_id")
            .increment(1);
        tracing::warn!(
            offered = cluster.unwrap_or("<none>"),
            expected = %security.cluster_id,
            "raft peer request for another cluster refused"
        );
        return (StatusCode::FORBIDDEN, "wrong raft cluster id").into_response();
    }
    if security.token.is_some() {
        let offered = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .unwrap_or("");
        let Some(slot) = security.match_token(offered) else {
            metrics::counter!("felix_meta_raft_peer_rejected_total", "reason" => "token")
                .increment(1);
            tracing::warn!("raft peer request with a missing or wrong peer token refused");
            return (StatusCode::UNAUTHORIZED, "raft peer token required").into_response();
        };
        metrics::counter!("felix_meta_raft_peer_token_accepted_total", "token" => slot)
            .increment(1);
    }
    next.run(request).await
}

/// Under peer mTLS, name the calling member from its certificate, and refuse
/// a revoked certificate or one that names no member of this cluster.
///
/// The handshake already checked the chain. This is the binding between a
/// certificate and a member id, which the CA alone does not give: any
/// certificate it signed would otherwise speak for any member.
pub(super) async fn require_member(
    State(security): State<std::sync::Arc<PeerSecurity>>,
    mut request: Request,
    next: Next,
) -> Response {
    if security.tls.is_none() {
        return next.run(request).await;
    }
    let leaf = request
        .extensions()
        .get::<crate::auth::cert_binding::PeerCertificates>()
        .and_then(|certs| certs.0.first().cloned());
    let Some(leaf) = leaf else {
        return refuse_member("no_certificate", "the peer presented no certificate");
    };
    if security.is_revoked(&leaf) {
        return refuse_member("revoked", "the peer certificate is revoked");
    }
    match member_of(&leaf, &security.cluster_id) {
        Ok(id) => {
            request.extensions_mut().insert(PeerMember(id));
            next.run(request).await
        }
        Err(why) => refuse_member("identity", &why),
    }
}

fn refuse_member(reason: &'static str, why: &str) -> Response {
    metrics::counter!("felix_meta_raft_peer_rejected_total", "reason" => reason).increment(1);
    tracing::warn!(why, "raft peer request refused");
    (StatusCode::FORBIDDEN, why.to_string()).into_response()
}

/// Refuse a request meant for another member, as a member that is not
/// there would: every sender reads the 503 as unreachable.
///
/// An address can reach the wrong member: two configured addresses that
/// resolve to one machine, or a slot whose member has since changed. Its
/// vote or its append must not count as the member the sender asked.
///
/// A request without the header is served. A sender on a build without the
/// header means the configured id, which is the only id a member has had.
pub(super) async fn require_target(
    State(id): State<super::NodeId>,
    request: Request,
    next: Next,
) -> Response {
    if let Some(value) = request.headers().get(TARGET_HEADER) {
        let target = value
            .to_str()
            .ok()
            .and_then(|value| value.parse::<super::NodeId>().ok());
        if target != Some(id) {
            metrics::counter!("felix_meta_raft_peer_rejected_total", "reason" => "target")
                .increment(1);
            tracing::warn!(
                target = value.to_str().unwrap_or("<not text>"),
                member = id,
                "raft peer request meant for another member refused"
            );
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("this is raft member {id}, not the member the request is for"),
            )
                .into_response();
        }
    }
    next.run(request).await
}

mod live;
pub(crate) use live::parse_fingerprints;

#[cfg(test)]
mod tests;
