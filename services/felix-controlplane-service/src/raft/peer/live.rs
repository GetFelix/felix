//! Peer mTLS material that follows rotation: this member's certificate and
//! key, the cluster CA, and the revocation list, each re-read every
//! [`felix_common::tls::RELOAD_INTERVAL`].
//!
//! A rotated certificate must pass the startup checks: it verifies against
//! the cluster CA now as both client and server, covers every name the
//! current one does, names the same member of the same cluster, and is not
//! revoked. A rotated CA bundle must verify this member's own certificate or
//! the one waiting on disk (then that one must be taken too, or the bundle
//! is put back), and a revocation file that lists nothing clears the list
//! only with an explicit marker line. Whatever fails is refused with an
//! error log and the current material stays, so a bad file never takes a
//! member off the group.
//!
//! A reload changes only the next handshake; connections already up close
//! when the certificate they were opened under ends (see
//! `server::tls::serve_tls` and [`LivePeerTls::close_at`]).
use std::collections::BTreeSet;
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use felix_common::tls::{
    IdentityCheck, IdentityFiles, Presenter, ReloadingClientCa, ReloadingIdentity, load_pem_certs,
    verify_chain,
};
use rustls::pki_types::CertificateDer;

use super::{PeerTls, fingerprint, member_of};

/// The live peer mTLS state one member shares across its listener and every
/// peer client it builds.
pub(crate) struct LivePeerTls {
    pub(super) identity: Arc<ReloadingIdentity>,
    pub(super) ca: Arc<ReloadingClientCa>,
    server_verifier: ArcSwap<rustls::client::WebPkiServerVerifier>,
    revoked: Arc<ArcSwap<BTreeSet<String>>>,
    revoked_env: BTreeSet<String>,
    revoked_file: Option<String>,
    /// The member the startup certificate names; every rotated one must
    /// name it too.
    pub(super) member: super::super::NodeId,
}

impl std::fmt::Debug for LivePeerTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LivePeerTls")
            .field("identity", &self.identity)
            .field("ca", &self.ca)
            .field("member", &self.member)
            .finish_non_exhaustive()
    }
}

impl LivePeerTls {
    /// Read every file, and start re-reading them on the current runtime.
    /// The reload task stops once the last holder is dropped.
    pub(super) fn load(tls: &PeerTls, cluster_id: &str) -> Result<Arc<Self>> {
        let revoked_env = tls.revoked.clone();
        let revoked = Arc::new(ArcSwap::from_pointee(revocation_list(
            &revoked_env,
            tls.revoked_file.as_deref(),
            None,
        )?));
        let ca = ReloadingClientCa::load("FELIX_RAFT_TLS_CA", &tls.ca_path, provider())?;
        let server_verifier = ArcSwap::new(server_verifier(&ca)?);
        let member = Arc::new(std::sync::OnceLock::new());
        let identity = Arc::new(
            ReloadingIdentity::load_checked(
                IdentityFiles {
                    cert_path: tls.cert_path.clone(),
                    cert_var: "FELIX_RAFT_TLS_CERT",
                    key_path: tls.key_path.clone(),
                    key_var: "FELIX_RAFT_TLS_KEY",
                },
                provider(),
                Some(identity_check(
                    cluster_id.to_string(),
                    Arc::clone(&ca),
                    Arc::clone(&revoked),
                    Arc::clone(&member),
                )),
            )
            .context("raft peer TLS")?
            .keep_names(),
        );
        let member = *member
            .get()
            .context("raft peer certificate names no member")?;
        let live = Arc::new(Self {
            identity,
            ca,
            server_verifier,
            revoked,
            revoked_env,
            revoked_file: tls.revoked_file.clone(),
            member,
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

    /// The CA's server verifier as of now.
    pub(super) fn server_verifier(&self) -> Arc<rustls::client::WebPkiServerVerifier> {
        self.server_verifier.load_full()
    }

    /// When a connection opened under the client chain `chain` must close:
    /// its earliest `notAfter`, spread over the 30 s before it so the members
    /// do not all reconnect at once. A chain that does not parse closes at
    /// once.
    pub(crate) fn close_at(&self, chain: &[CertificateDer<'static>]) -> Option<i64> {
        felix_common::tls::ChainExpiry::new(
            self.ca.roots(),
            &provider(),
            felix_common::tls::Presenter::Client,
        )
        .not_after(chain)
        .unwrap_or(Some(0))
        .map(|at| felix_common::tls::spread_before(at, CLOSE_SPREAD))
    }

    /// Re-read everything once. The revocation list and CA go first, so a
    /// rotated certificate is checked against the newest of both.
    pub(super) fn reload(&self) {
        let current = self.revoked.load_full();
        match revocation_list(
            &self.revoked_env,
            self.revoked_file.as_deref(),
            Some(&current),
        ) {
            Ok(list) => {
                if **self.revoked.load() != list {
                    tracing::info!(entries = list.len(), "raft peer revocation list reloaded");
                    self.revoked.store(Arc::new(list));
                }
            }
            Err(err) => tracing::warn!(
                error = %format!("{err:#}"),
                "could not reload FELIX_RAFT_TLS_REVOKED_FILE; keeping the current list",
            ),
        }
        // A bundle that verifies neither this member's certificate nor the
        // one waiting on disk would make it refuse every peer issued
        // alongside it. The waiting one counts so that a jump to a new root
        // (bundle and leaf in the same tick) is taken.
        let own = self.identity.chain();
        let waiting = load_pem_certs("FELIX_RAFT_TLS_CERT", self.identity.cert_path())
            .ok()
            .filter(|chain| *chain != own);
        let saved = (self.ca.saved(), self.server_verifier.load_full());
        let mut on_waiting = false;
        let ca = self
            .ca
            .reload_checked(|roots| {
                issued_by(&own, roots).or_else(|why| match &waiting {
                    Some(chain) => issued_by(chain, roots)
                        .map(|()| on_waiting = true)
                        .map_err(|waiting| format!("{why}; the certificate on disk: {waiting}")),
                    None => Err(why),
                })
            })
            .map_err(anyhow::Error::from);
        let ca_rotated =
            match ca.and_then(|changed| changed.then(|| server_verifier(&self.ca)).transpose()) {
                Ok(Some(verifier)) => {
                    self.server_verifier.store(verifier);
                    true
                }
                Ok(None) => false,
                Err(err) => {
                    tracing::error!(
                        error = %format!("{err:#}"),
                        "could not reload FELIX_RAFT_TLS_CA; keeping the current one",
                    );
                    false
                }
            };
        let rotated = match self.identity.reload() {
            Ok(rotated) => rotated,
            Err(err @ felix_common::tls::TlsFileError::Refused { .. }) => {
                tracing::error!(
                    error = %err,
                    "refused the rotated raft peer certificate; keeping the current one",
                );
                false
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "could not reload the raft peer certificate; keeping the current one",
                );
                false
            }
        };
        // A bundle taken on the waiting certificate's word only stands if
        // that certificate is now presented: the current one does not chain
        // to it, so keeping it would cut this member off from its peers.
        if ca_rotated && on_waiting && !rotated {
            self.ca.restore(saved.0);
            self.server_verifier.store(saved.1);
            tracing::error!(
                "kept the current FELIX_RAFT_TLS_CA: the new bundle only verifies the \
                 certificate on disk, which was not taken",
            );
        } else if ca_rotated {
            tracing::info!("raft peer CA rotated; new handshakes use it");
        }
        if rotated {
            tracing::info!(
                cert = %self.identity.cert_path(),
                "raft peer TLS certificate rotated; new connections use it",
            );
        }
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
/// it verifies against the cluster CA as both client and server, names
/// exactly one member of `cluster_id`, the same one as at startup, and is
/// not revoked.
fn identity_check(
    cluster_id: String,
    ca: Arc<ReloadingClientCa>,
    revoked: Arc<ArcSwap<BTreeSet<String>>>,
    member: Arc<std::sync::OnceLock<super::super::NodeId>>,
) -> IdentityCheck {
    Arc::new(move |chain| {
        issued_by(chain, &ca.roots())?;
        let leaf = chain.first().ok_or("no certificate")?;
        let named = member_of(leaf, &cluster_id)?;
        let first = *member.get_or_init(|| named);
        if named != first {
            return Err(format!(
                "the certificate is issued to raft member {named}, but this is member {first}"
            ));
        }
        if revoked.load().contains(&fingerprint(leaf)) {
            return Err("the certificate is listed in FELIX_RAFT_TLS_REVOKED".to_string());
        }
        Ok(())
    })
}

fn server_verifier(ca: &ReloadingClientCa) -> Result<Arc<rustls::client::WebPkiServerVerifier>> {
    rustls::client::WebPkiServerVerifier::builder_with_provider(ca.roots(), provider())
        .build()
        .context("raft peer server verifier")
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
