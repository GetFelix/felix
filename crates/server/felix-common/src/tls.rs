//! Certificate and key files that are re-read when they change.
//!
//! Rotation is a file swap: cert-manager, a Kubernetes Secret volume and most
//! renewal tools replace the files in place. [`ReloadingIdentity`] is a rustls
//! certificate resolver whose certificate and key are checked on a timer and
//! installed for the *next* handshake, so a rotation never drops a connection
//! that is already up. A file that is missing or mid-write keeps the current
//! identity in place; the next check tries again.
//!
//! A rotated certificate is presented only if it is valid now and, with
//! [`ReloadingIdentity::keep_names`], covers every DNS name and IP address the
//! current one does; otherwise the current one stays and the reload reports
//! [`TlsFileError::Refused`]. A service can
//! attach an [`IdentityCheck`] on top, which the startup certificate and
//! every rotated one must pass. [`verify_chain`] is the CA check such a rule
//! usually wants.
//!
//! [`ReloadingClientCa`] does the same for an mTLS listener's client CA, and
//! [`load_roots`] reads a CA bundle once.
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::sign::CertifiedKey;

mod binding;
mod client_ca;
pub use binding::{PRINCIPAL_URI_PREFIX, UriSanError, check_subject_binding, uri_sans_after};
pub use client_ca::{ReloadingClientCa, SavedClientCa};
mod expiry;
mod rotation;
pub use expiry::{ChainExpiry, Presenter, UnreadableCertificate, spread_before};
pub use rotation::verify_chain;

/// How often services check their certificate files for a rotation.
pub const RELOAD_INTERVAL: Duration = Duration::from_secs(30);

/// A rule every certificate must pass before it is presented, at startup and
/// on each reload: an `Err` says why it is refused.
pub type IdentityCheck =
    Arc<dyn Fn(&[CertificateDer<'static>]) -> Result<(), String> + Send + Sync>;

/// A certificate chain and key read from PEM files, swapped whole when the
/// files change.
pub struct ReloadingIdentity {
    files: IdentityFiles,
    provider: Arc<CryptoProvider>,
    current: ArcSwap<Loaded>,
    check: Option<IdentityCheck>,
    keep_names: bool,
}

impl ReloadingIdentity {
    /// Read the certificate and key.
    ///
    /// Fails rather than starting without them: a listener that comes up with
    /// unreadable key material is a misconfiguration, not a runtime condition
    /// to retry.
    pub fn load(files: IdentityFiles, provider: Arc<CryptoProvider>) -> Result<Self, TlsFileError> {
        Self::load_checked(files, provider, None)
    }

    /// [`load`](Self::load), with `check` applied to the startup certificate
    /// and to every rotated one. A rotated certificate that fails it is not
    /// presented; the current one stays.
    pub fn load_checked(
        files: IdentityFiles,
        provider: Arc<CryptoProvider>,
        check: Option<IdentityCheck>,
    ) -> Result<Self, TlsFileError> {
        let loaded = load(&files, &provider, check.as_ref())?;
        Ok(Self {
            files,
            provider,
            current: ArcSwap::from_pointee(loaded),
            check,
            keep_names: false,
        })
    }

    /// Refuse a rotated certificate that does not cover every DNS name and IP
    /// address the current one does, since peers may be dialling any of
    /// them. Off where the name is the identity and a rotation may correct
    /// it.
    pub fn keep_names(self) -> Self {
        Self {
            keep_names: true,
            ..self
        }
    }

    /// Where the certificate is read from.
    pub fn cert_path(&self) -> &str {
        &self.files.cert_path
    }

    /// The certificate chain currently presented, leaf first.
    pub fn chain(&self) -> Vec<CertificateDer<'static>> {
        self.current.load().cert.clone()
    }

    /// Re-read the files. `Ok(true)` when the certificate changed and the new
    /// one is now presented; an error leaves the current one in place.
    pub fn reload(&self) -> Result<bool, TlsFileError> {
        let cert = load_pem_certs(self.files.cert_var, &self.files.cert_path)?;
        let current = self.current.load();
        if cert == current.cert {
            return Ok(false);
        }
        if let Some(why) = rotation::refusal(&cert[0], &current.cert[0], self.keep_names) {
            return Err(TlsFileError::Refused {
                var: self.files.cert_var,
                path: self.files.cert_path.clone(),
                why,
            });
        }
        let fresh = certified(&self.files, cert, &self.provider, self.check.as_ref())?;
        self.current.store(Arc::new(fresh));
        Ok(true)
    }

    /// Check the files every [`RELOAD_INTERVAL`], for ever. `what` names the
    /// listener in the log lines. Callers race it against their shutdown.
    pub async fn reload_periodically(self: Arc<Self>, what: &'static str) {
        let mut ticker = tokio::time::interval(RELOAD_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick is immediate and the files were just read.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match self.reload() {
                Ok(true) => tracing::info!(
                    cert = %self.files.cert_path,
                    "{what} TLS certificate rotated; new connections use it",
                ),
                Ok(false) => {}
                // A certificate that reads fine but fails the checks will
                // not fix itself on the next tick, unlike a half-written one.
                Err(err @ TlsFileError::Refused { .. }) => tracing::error!(
                    error = %err,
                    "refused the rotated {what} TLS certificate; keeping the current one",
                ),
                Err(err) => tracing::warn!(
                    error = %err,
                    "could not reload the {what} TLS certificate; keeping the current one",
                ),
            }
        }
    }

    fn key(&self) -> Arc<CertifiedKey> {
        Arc::clone(&self.current.load().key)
    }
}

impl rustls::server::ResolvesServerCert for ReloadingIdentity {
    fn resolve(&self, _hello: rustls::server::ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.key())
    }
}

impl rustls::client::ResolvesClientCert for ReloadingIdentity {
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

impl std::fmt::Debug for ReloadingIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Paths, never material.
        f.debug_struct("ReloadingIdentity")
            .field("files", &self.files)
            .finish()
    }
}

/// Where a [`ReloadingIdentity`] reads from, and the variables that named the
/// paths so an error points at the setting to fix.
#[derive(Debug, Clone)]
pub struct IdentityFiles {
    pub cert_path: String,
    pub cert_var: &'static str,
    pub key_path: String,
    pub key_var: &'static str,
}

/// Why certificate material could not be used. Never carries key bytes.
#[derive(Debug, thiserror::Error)]
pub enum TlsFileError {
    #[error("read {var} {path}: {source}")]
    Read {
        var: &'static str,
        path: String,
        source: rustls::pki_types::pem::Error,
    },
    #[error("no certificates in {var} {path}")]
    Empty { var: &'static str, path: String },
    #[error("{var} {path} holds a certificate that cannot be a trust root: {source}")]
    Root {
        var: &'static str,
        path: String,
        source: rustls::Error,
    },
    #[error("{var} {path}: {why}")]
    Refused {
        var: &'static str,
        path: String,
        why: String,
    },
    #[error("the key in {key_path} does not match the certificate in {cert_path}: {source}")]
    Mismatch {
        cert_path: String,
        key_path: String,
        source: rustls::Error,
    },
}

/// Read every certificate in a PEM bundle. An empty bundle is an error: a
/// file with nothing in it is a mistake, not "trust nothing".
pub fn load_pem_certs(
    var: &'static str,
    path: &str,
) -> Result<Vec<CertificateDer<'static>>, TlsFileError> {
    let read_error = |source| TlsFileError::Read {
        var,
        path: path.to_string(),
        source,
    };
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(read_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(read_error)?;
    if certs.is_empty() {
        return Err(TlsFileError::Empty {
            var,
            path: path.to_string(),
        });
    }
    Ok(certs)
}

/// Read a CA bundle into a root store.
pub fn load_roots(var: &'static str, path: &str) -> Result<rustls::RootCertStore, TlsFileError> {
    roots_from(var, path, &load_pem_certs(var, path)?)
}

fn roots_from(
    var: &'static str,
    path: &str,
    bundle: &[CertificateDer<'static>],
) -> Result<rustls::RootCertStore, TlsFileError> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in bundle.iter().cloned() {
        roots.add(cert).map_err(|source| TlsFileError::Root {
            var,
            path: path.to_string(),
            source,
        })?;
    }
    Ok(roots)
}

/// The certificate and key currently presented.
struct Loaded {
    cert: Vec<CertificateDer<'static>>,
    key: Arc<CertifiedKey>,
}

fn load(
    files: &IdentityFiles,
    provider: &CryptoProvider,
    check: Option<&IdentityCheck>,
) -> Result<Loaded, TlsFileError> {
    let cert = load_pem_certs(files.cert_var, &files.cert_path)?;
    certified(files, cert, provider, check)
}

/// `cert` with the key on disk, once it passes `check`.
fn certified(
    files: &IdentityFiles,
    cert: Vec<CertificateDer<'static>>,
    provider: &CryptoProvider,
    check: Option<&IdentityCheck>,
) -> Result<Loaded, TlsFileError> {
    if let Some(check) = check {
        check(&cert).map_err(|why| TlsFileError::Refused {
            var: files.cert_var,
            path: files.cert_path.clone(),
            why,
        })?;
    }
    let key =
        PrivateKeyDer::from_pem_file(&files.key_path).map_err(|source| TlsFileError::Read {
            var: files.key_var,
            path: files.key_path.clone(),
            source,
        })?;
    let key = CertifiedKey::from_der(cert.clone(), key, provider).map_err(|source| {
        TlsFileError::Mismatch {
            cert_path: files.cert_path.clone(),
            key_path: files.key_path.clone(),
            source,
        }
    })?;
    Ok(Loaded {
        cert,
        key: Arc::new(key),
    })
}

#[cfg(test)]
mod tests;
