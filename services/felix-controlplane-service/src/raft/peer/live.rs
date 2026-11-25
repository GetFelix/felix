//! Peer mTLS material that follows rotation: this member's certificate and
//! key, the cluster CA, and the revocation list, each re-read every
//! [`felix_common::tls::RELOAD_INTERVAL`].
//!
//! A rotated certificate must pass the startup checks: it verifies against
//! the cluster CA as both client and server, covers every name the current
//! one does, names the same member of the same cluster, and is not revoked.
//! A rotated CA bundle must verify this member's own certificate or the one
//! waiting on disk, and in the second case that one is taken with it. A
//! revocation file that lists nothing clears the list only with an explicit
//! marker line. Whatever fails is refused with an error log and the current
//! material stays, so a bad file never takes a member off the group.
//!
//! The CA, the server verifier built from it and the certificate this member
//! presents sit in one [`PeerTrust`], checked against each other in full
//! before it is swapped in. A handshake reads one `PeerTrust` or the next,
//! never a CA from one with a certificate from another, and a refused
//! rotation stores nothing.
//!
//! A reload changes only the next handshake; connections already up close
//! when the certificate they were opened under ends (see
//! `server::tls::serve_tls` and [`LivePeerTls::close_at`]).
use std::collections::BTreeSet;
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use felix_common::tls::{
    ClientTrust, IdentityFiles, LoadedIdentity, Presenter, TlsFileError, load_pem_certs,
    verify_chain,
};
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::sign::CertifiedKey;

use super::{PeerTls, fingerprint, member_of};

/// The live peer mTLS state one member shares across its listener and every
/// peer client it builds. It is the listener's certificate resolver and
/// client verifier and the dialler's certificate resolver.
pub(crate) struct LivePeerTls {
    files: IdentityFiles,
    ca_path: String,
    cluster_id: String,
    trust: ArcSwap<PeerTrust>,
    revoked: ArcSwap<BTreeSet<String>>,
    revoked_env: BTreeSet<String>,
    revoked_file: Option<String>,
    /// The member the startup certificate names; every rotated one must
    /// name it too.
    pub(super) member: super::super::NodeId,
    /// Every `PeerTrust` a reload stored, in order.
    #[cfg(test)]
    pub(super) commits: std::sync::Mutex<Vec<Arc<PeerTrust>>>,
}

/// What one handshake trusts and presents.
pub(super) struct PeerTrust {
    ca: Arc<CaTrust>,
    identity: Arc<LoadedIdentity>,
}

/// One read of the CA bundle, with the verifiers built from it.
struct CaTrust {
    client: ClientTrust,
    server: Arc<rustls::client::WebPkiServerVerifier>,
}

impl PeerTrust {
    /// The roots peer chains are checked against.
    pub(super) fn roots(&self) -> &Arc<rustls::RootCertStore> {
        self.ca.client.roots()
    }

    /// The CA bundle as read.
    #[cfg(test)]
    pub(super) fn bundle(&self) -> &[CertificateDer<'static>] {
        self.ca.client.bundle()
    }

    /// The verifier for a peer's server certificate.
    pub(super) fn server_verifier(&self) -> &Arc<rustls::client::WebPkiServerVerifier> {
        &self.ca.server
    }

    /// The chain this member presents, leaf first.
    pub(super) fn chain(&self) -> &[CertificateDer<'static>] {
        self.identity.chain()
    }
}

impl std::fmt::Debug for LivePeerTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LivePeerTls")
            .field("files", &self.files)
            .field("ca_path", &self.ca_path)
            .field("member", &self.member)
            .finish_non_exhaustive()
    }
}

impl LivePeerTls {
    /// Read every file, and start re-reading them on the current runtime.
    /// The reload task stops once the last holder is dropped.
    pub(super) fn load(tls: &PeerTls, cluster_id: &str) -> Result<Arc<Self>> {
        let revoked_env = tls.revoked.clone();
        let revoked = revocation_list(&revoked_env, tls.revoked_file.as_deref(), None)?;
        let ca = Arc::new(CaTrust::load(&tls.ca_path)?);
        let files = IdentityFiles {
            cert_path: tls.cert_path.clone(),
            cert_var: "FELIX_RAFT_TLS_CERT",
            key_path: tls.key_path.clone(),
            key_var: "FELIX_RAFT_TLS_KEY",
        };
        let mut member = None;
        let identity = LoadedIdentity::load(&files, &provider(), |chain| {
            member = Some(check_member(chain, ca.roots(), cluster_id, None, &revoked)?);
            Ok(())
        })
        .context("raft peer TLS")?;
        let member = member.context("raft peer certificate names no member")?;
        let live = Arc::new(Self {
            files,
            ca_path: tls.ca_path.clone(),
            cluster_id: cluster_id.to_string(),
            trust: ArcSwap::from_pointee(PeerTrust {
                ca,
                identity: Arc::new(identity),
            }),
            revoked: ArcSwap::from_pointee(revoked),
            revoked_env,
            revoked_file: tls.revoked_file.clone(),
            member,
            #[cfg(test)]
            commits: std::sync::Mutex::default(),
        });
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            drop(runtime.spawn(reload_while_held(Arc::downgrade(&live))));
        }
        Ok(live)
    }

    /// Whether `der` is listed in `FELIX_RAFT_TLS_REVOKED` or its file now.
    pub(super) fn is_revoked(&self, der: &[u8]) -> bool {
        self.revoked.load().contains(&fingerprint(der))
    }

    /// The CA, its verifiers and this member's certificate as of now. Read
    /// it once per check, so all of them come from the same reload.
    pub(super) fn trust(&self) -> Arc<PeerTrust> {
        self.trust.load_full()
    }

    /// When a connection opened under the client chain `chain` must close:
    /// its earliest `notAfter`, spread over the 30 s before it so the members
    /// do not all reconnect at once. A chain that does not parse closes at
    /// once.
    pub(crate) fn close_at(&self, chain: &[CertificateDer<'static>]) -> Option<i64> {
        felix_common::tls::ChainExpiry::new(
            Arc::clone(self.trust.load().roots()),
            &provider(),
            Presenter::Client,
        )
        .close_at(chain, CLOSE_SPREAD)
    }

    /// Re-read everything once. The revocation list goes first, so a rotated
    /// certificate is checked against the newest one.
    pub(super) fn reload(&self) {
        let current = self.revoked.load_full();
        match revocation_list(
            &self.revoked_env,
            self.revoked_file.as_deref(),
            Some(&current),
        ) {
            Ok(list) => {
                if *current != list {
                    tracing::info!(entries = list.len(), "raft peer revocation list reloaded");
                    self.revoked.store(Arc::new(list));
                }
            }
            Err(err) => tracing::warn!(
                error = %format!("{err:#}"),
                "could not reload FELIX_RAFT_TLS_REVOKED_FILE; keeping the current list",
            ),
        }
        let current = self.trust.load_full();
        let ca = match CaTrust::load(&self.ca_path) {
            Ok(ca) if ca.client.bundle() != current.ca.client.bundle() => Some(Arc::new(ca)),
            Ok(_) => None,
            Err(err) => {
                tracing::warn!(
                    error = %format!("{err:#}"),
                    "could not reload FELIX_RAFT_TLS_CA; keeping the current one",
                );
                None
            }
        };
        let waiting = match load_pem_certs(self.files.cert_var, &self.files.cert_path) {
            Ok(chain) if chain != current.chain() => Some(chain),
            Ok(_) => None,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "could not reload the raft peer certificate; keeping the current one",
                );
                None
            }
        };
        if let Some(next) = self.next_trust(&current, ca, waiting) {
            self.commit(next);
        }
    }

    /// What `current` becomes given a changed bundle `ca` and a changed
    /// certificate `waiting`, each checked against what it would be used
    /// with. `None` keeps `current`.
    fn next_trust(
        &self,
        current: &PeerTrust,
        ca: Option<Arc<CaTrust>>,
        waiting: Option<Vec<CertificateDer<'static>>>,
    ) -> Option<PeerTrust> {
        let leaf_under = |roots: &rustls::RootCertStore| {
            waiting.clone().map(|chain| {
                current
                    .identity
                    .rotate(&self.files, chain, &provider(), true, |chain| {
                        self.check(chain, roots)
                    })
                    .map(Arc::new)
            })
        };
        if let Some(ca) = ca {
            // A jump to a new root lands the bundle and a leaf under it in
            // the same tick, and each alone fails against the other's old
            // counterpart, so the pair is tried first.
            let leaf = match leaf_under(ca.roots()) {
                Some(Ok(identity)) => {
                    tracing::info!("raft peer CA rotated; new handshakes use it");
                    log_rotated(&self.files);
                    return Some(PeerTrust { ca, identity });
                }
                leaf => leaf.and_then(Result::err),
            };
            match issued_by(current.chain(), ca.roots()) {
                Ok(()) => {
                    if let Some(err) = leaf {
                        log_refused(&err);
                    }
                    tracing::info!("raft peer CA rotated; new handshakes use it");
                    return Some(PeerTrust {
                        ca,
                        identity: Arc::clone(&current.identity),
                    });
                }
                // A bundle that verifies neither this member's certificate
                // nor the one waiting would make it refuse every peer issued
                // alongside it.
                Err(why) => tracing::error!(
                    error = %match &leaf {
                        Some(waiting) => format!("{why}; the certificate on disk: {waiting}"),
                        None => why,
                    },
                    "could not reload FELIX_RAFT_TLS_CA; keeping the current one",
                ),
            }
        }
        match leaf_under(current.roots())? {
            Ok(identity) => {
                log_rotated(&self.files);
                Some(PeerTrust {
                    ca: Arc::clone(&current.ca),
                    identity,
                })
            }
            Err(err) => {
                log_refused(&err);
                None
            }
        }
    }

    fn commit(&self, next: PeerTrust) {
        let next = Arc::new(next);
        #[cfg(test)]
        self.commits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Arc::clone(&next));
        self.trust.store(next);
    }

    /// The startup checks against `roots`, for the member this is.
    fn check(
        &self,
        chain: &[CertificateDer<'static>],
        roots: &rustls::RootCertStore,
    ) -> Result<(), String> {
        check_member(
            chain,
            roots,
            &self.cluster_id,
            Some(self.member),
            &self.revoked.load(),
        )
        .map(drop)
    }

    fn key(&self) -> Arc<CertifiedKey> {
        self.trust.load().identity.key()
    }
}

impl CaTrust {
    fn load(path: &str) -> Result<Self> {
        let client = ClientTrust::load("FELIX_RAFT_TLS_CA", path, &provider())?;
        let server = rustls::client::WebPkiServerVerifier::builder_with_provider(
            Arc::clone(client.roots()),
            provider(),
        )
        .build()
        .context("raft peer server verifier")?;
        Ok(Self { client, server })
    }

    fn roots(&self) -> &rustls::RootCertStore {
        self.client.roots()
    }
}

fn log_rotated(files: &IdentityFiles) {
    tracing::info!(
        cert = %files.cert_path,
        "raft peer TLS certificate rotated; new connections use it",
    );
}

fn log_refused(err: &TlsFileError) {
    match err {
        TlsFileError::Refused { .. } => tracing::error!(
            error = %err,
            "refused the rotated raft peer certificate; keeping the current one",
        ),
        _ => tracing::warn!(
            error = %err,
            "could not reload the raft peer certificate; keeping the current one",
        ),
    }
}

impl rustls::server::ResolvesServerCert for LivePeerTls {
    fn resolve(&self, _hello: rustls::server::ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.key())
    }
}

impl rustls::client::ResolvesClientCert for LivePeerTls {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[rustls::SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        Some(self.key())
    }

    fn has_certs(&self) -> bool {
        true
    }
}

impl ClientCertVerifier for LivePeerTls {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    // No hints: a borrow of the current roots cannot outlive the load.
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.trust
            .load()
            .ca
            .client
            .verifier()
            .verify_client_cert(end_entity, intermediates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.trust
            .load()
            .ca
            .client
            .verifier()
            .verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.trust
            .load()
            .ca
            .client
            .verifier()
            .verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.trust
            .load()
            .ca
            .client
            .verifier()
            .supported_verify_schemes()
    }
}

/// How far before a certificate's end its connections spread their close.
/// The same spread as the broker's internal port.
const CLOSE_SPREAD: std::time::Duration = std::time::Duration::from_secs(30);

async fn reload_while_held(live: Weak<LivePeerTls>) {
    let mut ticker = tokio::time::interval(felix_common::tls::RELOAD_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let Some(live) = live.upgrade() else {
            return;
        };
        live.reload();
    }
}

/// Whether `chain` verifies against `roots` now as both client and server,
/// the way every peer checks it.
fn issued_by(chain: &[CertificateDer<'_>], roots: &rustls::RootCertStore) -> Result<(), String> {
    [Presenter::Client, Presenter::Server]
        .into_iter()
        .try_for_each(|presenter| verify_chain(chain, roots, &provider(), presenter))
}

/// The startup checks, applied to every certificate this member presents:
/// it verifies against `roots` as both client and server, names exactly one
/// member of `cluster_id` (`expected` when given), and is not `revoked`.
/// Returns the member it names.
fn check_member(
    chain: &[CertificateDer<'_>],
    roots: &rustls::RootCertStore,
    cluster_id: &str,
    expected: Option<super::super::NodeId>,
    revoked: &BTreeSet<String>,
) -> Result<super::super::NodeId, String> {
    issued_by(chain, roots)?;
    let leaf = chain.first().ok_or("no certificate")?;
    let named = member_of(leaf, cluster_id)?;
    if let Some(expected) = expected.filter(|expected| *expected != named) {
        return Err(format!(
            "the certificate is issued to raft member {named}, but this is member {expected}"
        ));
    }
    if revoked.contains(&fingerprint(leaf)) {
        return Err("the certificate is listed in FELIX_RAFT_TLS_REVOKED".to_string());
    }
    Ok(named)
}

/// `FELIX_RAFT_TLS_REVOKED` with the fingerprints in
/// `FELIX_RAFT_TLS_REVOKED_FILE` added, when that is set.
///
/// On a reload, `current` is the list in force. A file that lists nothing
/// while `current` holds entries from it is refused unless it carries the
/// [`CLEAR_MARKER`] line: a file caught mid-write, blank or cut after a
/// comment header, looks the same, and taking it would unrevoke everything.
fn revocation_list(
    env: &BTreeSet<String>,
    file: Option<&str>,
    current: Option<&BTreeSet<String>>,
) -> Result<BTreeSet<String>> {
    let mut list = env.clone();
    if let Some(path) = file {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read FELIX_RAFT_TLS_REVOKED_FILE {path}"))?;
        let entries = raw
            .lines()
            .map(|line| line.split('#').next().unwrap_or_default())
            .collect::<Vec<_>>()
            .join(",");
        let listed = parse_fingerprints("FELIX_RAFT_TLS_REVOKED_FILE", &entries)?;
        anyhow::ensure!(
            !listed.is_empty()
                || current.is_none_or(|current| current.is_subset(env))
                || raw.lines().any(|line| line.trim() == CLEAR_MARKER),
            "FELIX_RAFT_TLS_REVOKED_FILE {path} lists nothing but listed fingerprints before; \
             keeping them (a line `{CLEAR_MARKER}` clears the list)",
        );
        list.extend(listed);
    }
    Ok(list)
}

/// The line that marks a revocation file emptied on purpose.
const CLEAR_MARKER: &str = "# felix: none";

/// Comma- or whitespace-separated SHA-256 fingerprints. Colons and case are
/// ignored, as tools print both.
pub(crate) fn parse_fingerprints(var: &str, raw: &str) -> Result<BTreeSet<String>> {
    raw.split(|c: char| c == ',' || c.is_whitespace())
        .map(|entry| entry.trim().replace(':', "").to_ascii_lowercase())
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            if entry.len() == 64 && entry.bytes().all(|b| b.is_ascii_hexdigit()) {
                Ok(entry)
            } else {
                Err(anyhow::anyhow!(
                    "{var} holds `{entry}`, which is not a SHA-256 fingerprint"
                ))
            }
        })
        .collect()
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}
