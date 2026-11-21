//! When a presented certificate chain stops being valid.
//!
//! The handshake checks validity once. A connection that should not outlive
//! its certificate closes at the instant [`ChainExpiry::not_after`] returns.

use std::sync::Arc;
use std::time::Duration;

use rustls::RootCertStore;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, SignatureVerificationAlgorithm, UnixTime};

/// Which side of the handshake presented the chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Presenter {
    Client,
    Server,
}

/// The leaf certificate does not parse, so when it ends is unknown. Treat the
/// credential as refused.
#[derive(Debug, thiserror::Error)]
#[error("the peer's certificate does not parse")]
pub struct UnreadableCertificate;

/// Finds when a chain stops being valid, against the roots the handshake
/// verified it with.
#[derive(Clone)]
pub struct ChainExpiry {
    roots: Arc<RootCertStore>,
    algs: &'static [&'static dyn SignatureVerificationAlgorithm],
    usage: webpki::KeyUsage,
}

impl std::fmt::Debug for ChainExpiry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainExpiry")
            .field("roots", &self.roots.len())
            .field("usage", &self.usage)
            .finish()
    }
}

impl ChainExpiry {
    pub fn new(roots: Arc<RootCertStore>, provider: &CryptoProvider, presenter: Presenter) -> Self {
        Self {
            roots,
            algs: provider.signature_verification_algorithms.all,
            usage: match presenter {
                Presenter::Client => webpki::KeyUsage::client_auth(),
                Presenter::Server => webpki::KeyUsage::server_auth(),
            },
        }
    }

    /// The earliest `notAfter` of the leaf and the intermediates on its path
    /// to a trust anchor, in seconds since the epoch. `None` for an empty
    /// chain.
    ///
    /// Never later than the leaf's own `notAfter`. Trust anchors are not
    /// counted, since the handshake never checks their validity, and neither
    /// is an extra certificate the peer sent that is not on the path.
    pub fn not_after(
        &self,
        chain: &[CertificateDer<'_>],
    ) -> Result<Option<i64>, UnreadableCertificate> {
        let Some((leaf, rest)) = chain.split_first() else {
            return Ok(None);
        };
        let leaf_end = not_after(leaf).ok_or(UnreadableCertificate)?;
        Ok(Some(
            self.intermediates_end(leaf, rest)
                .map_or(leaf_end, |end| end.min(leaf_end)),
        ))
    }

    fn intermediates_end(
        &self,
        leaf: &CertificateDer<'_>,
        rest: &[CertificateDer<'_>],
    ) -> Option<i64> {
        if rest.is_empty() {
            return None;
        }
        let now = UnixTime::since_unix_epoch(
            crate::clock::system_time()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default(),
        );
        // Build the path again the way the handshake did.
        if let Ok(end_entity) = webpki::EndEntityCert::try_from(leaf)
            && let Ok(path) = end_entity.verify_for_usage(
                self.algs,
                &self.roots.roots,
                rest,
                now,
                self.usage,
                None,
                None,
            )
        {
            return path
                .intermediate_certificates()
                .filter_map(|cert| not_after(&cert.der()))
                .min();
        }
        // No path now: an intermediate has since expired, or the chain was
        // never verified (a forwarded one). Count everything that could be on
        // a path, which errs toward closing early.
        rest.iter()
            .filter(|cert| self.could_be_intermediate(cert))
            .filter_map(not_after)
            .min()
    }

    /// Not self-signed and not a trust anchor. A cert webpki cannot parse is
    /// never on a path it built.
    fn could_be_intermediate(&self, cert: &CertificateDer<'_>) -> bool {
        let Ok(parsed) = webpki::EndEntityCert::try_from(cert) else {
            return false;
        };
        parsed.subject() != parsed.issuer()
            && !self
                .roots
                .roots
                .iter()
                .any(|anchor| anchor.subject.as_ref() == parsed.subject())
    }
}

fn not_after(cert: &CertificateDer<'_>) -> Option<i64> {
    x509_parser::parse_x509_certificate(cert.as_ref())
        .ok()
        .map(|(_, cert)| cert.validity().not_after.timestamp())
}

/// `not_after` moved earlier by a random amount up to `spread`, so connections
/// whose certificates were issued together do not all close at once. Never
/// later than `not_after`, and never by more than a quarter of the time left,
/// so a connection made just before its certificate ends is not closed at
/// once.
pub fn spread_before(not_after: i64, spread: Duration) -> i64 {
    let quarter_left = until_unix(not_after).as_secs() / 4;
    let back = crate::jitter::up_to(spread.as_secs().min(quarter_left));
    not_after.saturating_sub(i64::try_from(back).unwrap_or(i64::MAX))
}

/// Sleep until `at` (seconds since the epoch) on the wall clock; returns at
/// once if it has passed. Checked against the clock again on wake, so a
/// suspended host or a stepped clock cannot end it early or keep it past `at`
/// by more than the re-check interval.
pub async fn sleep_until_unix(at: i64) {
    loop {
        let left = until_unix(at);
        if left.is_zero() {
            return;
        }
        // Monotonic sleeps drift from the wall clock across a suspend, so
        // wake at least once a minute to look again.
        tokio::time::sleep(left.min(Duration::from_secs(60))).await;
    }
}

/// How long until `at` (seconds since the epoch); zero once it has passed.
pub fn until_unix(at: i64) -> Duration {
    let now = crate::clock::system_time()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    Duration::from_secs(u64::try_from(at).unwrap_or(0)).saturating_sub(now)
}

#[cfg(test)]
mod tests;
