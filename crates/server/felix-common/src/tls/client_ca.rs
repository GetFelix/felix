//! A CA bundle that follows rotation, used as the client-certificate
//! verifier of an mTLS listener.

use std::sync::Arc;

use arc_swap::ArcSwap;
use rustls::RootCertStore;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};

use super::{RELOAD_INTERVAL, TlsFileError, load_pem_certs};

/// A client CA bundle re-read on a timer, and the verifier built from it.
///
/// A bundle that does not read or parse keeps the current one, as a
/// half-written certificate does. Root hints are not sent: a borrow of the
/// current bundle cannot outlive the load that produced it.
pub struct ReloadingClientCa {
    var: &'static str,
    path: String,
    provider: Arc<CryptoProvider>,
    current: ArcSwap<ClientTrust>,
}

/// A bundle [`ReloadingClientCa::saved`] set aside.
pub struct SavedClientCa(Arc<ClientTrust>);

struct ClientTrust {
    bundle: Vec<CertificateDer<'static>>,
    roots: Arc<RootCertStore>,
    verifier: Arc<dyn ClientCertVerifier>,
}

impl ReloadingClientCa {
    /// Read the bundle `var` names. Fails on an unreadable or empty one.
    pub fn load(
        var: &'static str,
        path: &str,
        provider: Arc<CryptoProvider>,
    ) -> Result<Arc<Self>, TlsFileError> {
        let trust = client_trust(var, path, &provider)?;
        Ok(Arc::new(Self {
            var,
            path: path.to_string(),
            provider,
            current: ArcSwap::from_pointee(trust),
        }))
    }

    /// The roots currently trusted.
    pub fn roots(&self) -> Arc<RootCertStore> {
        Arc::clone(&self.current.load().roots)
    }

    /// Re-read the bundle. `Ok(true)` when it changed.
    pub fn reload(&self) -> Result<bool, TlsFileError> {
        self.reload_checked(|_| Ok(()))
    }

    /// [`reload`](Self::reload), installing a changed bundle only if `accept`
    /// passes its roots; otherwise the current one stays and this returns
    /// [`TlsFileError::Refused`].
    pub fn reload_checked(
        &self,
        accept: impl FnOnce(&RootCertStore) -> Result<(), String>,
    ) -> Result<bool, TlsFileError> {
        let fresh = client_trust(self.var, &self.path, &self.provider)?;
        if fresh.bundle == self.current.load().bundle {
            return Ok(false);
        }
        accept(&fresh.roots).map_err(|why| TlsFileError::Refused {
            var: self.var,
            path: self.path.clone(),
            why,
        })?;
        self.current.store(Arc::new(fresh));
        Ok(true)
    }

    /// The trust in force now, to put back with [`restore`](Self::restore).
    pub fn saved(&self) -> SavedClientCa {
        SavedClientCa(self.current.load_full())
    }

    /// Go back to `saved`. The next reload sees the bundle on disk as a
    /// change again and re-checks it.
    pub fn restore(&self, saved: SavedClientCa) {
        self.current.store(saved.0);
    }

    /// Check the bundle every [`RELOAD_INTERVAL`], for ever. Callers race it
    /// against their shutdown.
    pub async fn reload_periodically(self: Arc<Self>, what: &'static str) {
        let mut ticker = tokio::time::interval(RELOAD_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match self.reload() {
                Ok(true) => tracing::info!(ca = %self.path, "{what} client CA rotated"),
                Ok(false) => {}
                Err(err) => tracing::warn!(
                    error = %err,
                    "could not reload the {what} client CA; keeping the current one",
                ),
            }
        }
    }
}

impl std::fmt::Debug for ReloadingClientCa {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReloadingClientCa")
            .field("path", &self.path)
            .finish()
    }
}

impl ClientCertVerifier for ReloadingClientCa {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.current
            .load()
            .verifier
            .verify_client_cert(end_entity, intermediates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.current
            .load()
            .verifier
            .verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.current
            .load()
            .verifier
            .verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.current.load().verifier.supported_verify_schemes()
    }
}

/// Read the bundle once and build the roots and verifier from it.
fn client_trust(
    var: &'static str,
    path: &str,
    provider: &Arc<CryptoProvider>,
) -> Result<ClientTrust, TlsFileError> {
    let bundle = load_pem_certs(var, path)?;
    let roots = Arc::new(super::roots_from(var, path, &bundle)?);
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::clone(&roots),
        Arc::clone(provider),
    )
    .build()
    .map_err(|err| TlsFileError::Refused {
        var,
        path: path.to_string(),
        why: format!("cannot build a client verifier: {err}"),
    })?;
    Ok(ClientTrust {
        bundle,
        roots,
        verifier,
    })
}
