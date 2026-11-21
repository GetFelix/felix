//! Peer mTLS material that follows rotation: this member's certificate and
//! key, the cluster CA, and the revocation list, each re-read every
//! [`felix_common::tls::RELOAD_INTERVAL`].
//!
//! A rotated certificate must pass the startup checks: it verifies against
//! the cluster CA now as both client and server, covers every name the
//! current one does, names the same member of the same cluster, and is not
//! revoked. A rotated CA bundle must still verify this member's own
//! certificate, and a revocation file read back blank is not taken to clear
//! the list. Whatever fails is refused with an error log and the current
//! material stays, so a bad file never takes a member off the group.
//!
//! A reload changes only the next handshake; connections already up close
//! when the certificate they were opened under ends (see
//! `server::tls::serve_tls` and [`LivePeerTls::close_at`]).
use std::collections::BTreeSet;
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use felix_common::tls::{
    IdentityCheck, IdentityFiles, Presenter, ReloadingClientCa, ReloadingIdentity, verify_chain,
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
        // A bundle that no longer verifies this member's own certificate
        // would make it refuse every peer issued alongside it.
        let own = self.identity.chain();
        let ca = self
            .ca
            .reload_checked(|roots| {
                [Presenter::Client, Presenter::Server]
                    .into_iter()
                    .try_for_each(|presenter| verify_chain(&own, roots, &provider(), presenter))
            })
            .map_err(anyhow::Error::from);
        match ca.and_then(|changed| changed.then(|| server_verifier(&self.ca)).transpose()) {
            Ok(Some(verifier)) => {
                self.server_verifier.store(verifier);
                tracing::info!("raft peer CA rotated; new handshakes use it");
            }
            Ok(None) => {}
            Err(err) => tracing::error!(
                error = %format!("{err:#}"),
                "could not reload FELIX_RAFT_TLS_CA; keeping the current one",
            ),
        }
        match self.identity.reload() {
            Ok(true) => tracing::info!(
                cert = %self.identity.cert_path(),
                "raft peer TLS certificate rotated; new connections use it",
            ),
            Ok(false) => {}
            Err(err @ felix_common::tls::TlsFileError::Refused { .. }) => tracing::error!(
                error = %err,
                "refused the rotated raft peer certificate; keeping the current one",
            ),
            Err(err) => tracing::warn!(
                error = %err,
                "could not reload the raft peer certificate; keeping the current one",
            ),
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
        let roots = ca.roots();
        for presenter in [Presenter::Client, Presenter::Server] {
            verify_chain(chain, &roots, &provider(), presenter)?;
        }
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
/// On a reload, `current` is the list in force. A file read back blank while
/// `current` holds entries from it is refused: that is what a file caught
/// between truncate and write looks like, and taking it would unrevoke
/// everything. To clear the list on purpose, leave a comment in the file.
fn revocation_list(
    env: &BTreeSet<String>,
    file: Option<&str>,
    current: Option<&BTreeSet<String>>,
) -> Result<BTreeSet<String>> {
    let mut list = env.clone();
    if let Some(path) = file {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read FELIX_RAFT_TLS_REVOKED_FILE {path}"))?;
        anyhow::ensure!(
            !raw.trim().is_empty() || current.is_none_or(|current| current.len() <= env.len()),
            "FELIX_RAFT_TLS_REVOKED_FILE {path} is blank but listed fingerprints before; \
             keeping them (write a comment to clear the list)",
        );
        let entries = raw
            .lines()
            .map(|line| line.split('#').next().unwrap_or_default())
            .collect::<Vec<_>>()
            .join(",");
        list.extend(parse_fingerprints("FELIX_RAFT_TLS_REVOKED_FILE", &entries)?);
    }
    Ok(list)
}

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
