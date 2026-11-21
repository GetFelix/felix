//! When a presented certificate chain stops being valid.
//!
//! The handshake checks validity once. A connection that should not outlive
//! its certificate closes at the instant this returns.

use rustls::pki_types::CertificateDer;

/// The earliest `notAfter` across `chain`, in seconds since the epoch.
///
/// The handshake verified every certificate the peer presented, so the chain
/// is only as valid as its shortest-lived member. `None` for an empty chain or
/// one that does not parse; a chain the handshake accepted always parses.
pub fn chain_not_after(chain: &[CertificateDer<'_>]) -> Option<i64> {
    chain
        .iter()
        .map(|cert| {
            x509_parser::parse_x509_certificate(cert.as_ref())
                .ok()
                .map(|(_, cert)| cert.validity().not_after.timestamp())
        })
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .min()
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
        tokio::time::sleep(left.min(std::time::Duration::from_secs(60))).await;
    }
}

/// How long until `at` (seconds since the epoch); zero once it has passed.
pub fn until_unix(at: i64) -> std::time::Duration {
    let now = crate::clock::system_time()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    std::time::Duration::from_secs(u64::try_from(at).unwrap_or(0)).saturating_sub(now)
}

#[cfg(test)]
mod tests;
