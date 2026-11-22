//! What a rotated certificate must satisfy before it replaces the one being
//! presented. Peers check validity and names on every handshake, so a
//! rotated certificate that fails here would be refused by all of them;
//! keeping the current one is the only safe move.

use rustls::RootCertStore;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};

use super::Presenter;

/// Why `candidate` cannot replace `current`: it is not valid now, or, with
/// `keep_names`, it does not cover a DNS name or IP address `current` covers.
pub(super) fn refusal(
    candidate: &CertificateDer<'_>,
    current: &CertificateDer<'_>,
    keep_names: bool,
) -> Option<String> {
    let Ok((_, parsed)) = x509_parser::parse_x509_certificate(candidate.as_ref()) else {
        return Some("the certificate does not parse".to_string());
    };
    let now = now().as_secs() as i64;
    let validity = parsed.validity();
    if now < validity.not_before.timestamp() {
        return Some(format!(
            "the certificate is not valid until {}",
            validity.not_before
        ));
    }
    if now > validity.not_after.timestamp() {
        return Some(format!("the certificate expired at {}", validity.not_after));
    }
    let Ok(end_entity) = webpki::EndEntityCert::try_from(candidate) else {
        return Some("the certificate does not parse".to_string());
    };
    if !keep_names {
        return None;
    }
    for name in names(current) {
        if end_entity.verify_is_valid_for_subject_name(&name).is_err() {
            return Some(format!(
                "the certificate does not cover {}, which the current one does",
                name.to_str()
            ));
        }
    }
    None
}

/// Whether `chain` verifies against `roots` now, for `presenter`'s side of a
/// handshake, the way a peer's verifier would check it. Names are not checked.
pub fn verify_chain(
    chain: &[CertificateDer<'_>],
    roots: &RootCertStore,
    provider: &CryptoProvider,
    presenter: Presenter,
) -> Result<(), String> {
    let (leaf, intermediates) = chain.split_first().ok_or("no certificate")?;
    let end_entity = webpki::EndEntityCert::try_from(leaf)
        .map_err(|err| format!("the certificate does not parse: {err}"))?;
    let usage = match presenter {
        Presenter::Client => webpki::KeyUsage::client_auth(),
        Presenter::Server => webpki::KeyUsage::server_auth(),
    };
    end_entity
        .verify_for_usage(
            provider.signature_verification_algorithms.all,
            &roots.roots,
            intermediates,
            UnixTime::since_unix_epoch(now()),
            usage,
            None,
            None,
        )
        .map(|_| ())
        .map_err(|err| format!("the certificate does not verify against the CA as a {presenter:?} certificate: {err}"))
}

fn now() -> std::time::Duration {
    crate::clock::system_time()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
}

/// The DNS names and IP addresses `cert` is valid for. A wildcard stands for
/// one name under it.
fn names(cert: &CertificateDer<'_>) -> Vec<ServerName<'static>> {
    let Ok((_, parsed)) = x509_parser::parse_x509_certificate(cert.as_ref()) else {
        return Vec::new();
    };
    let Ok(Some(san)) = parsed.subject_alternative_name() else {
        return Vec::new();
    };
    san.value
        .general_names
        .iter()
        .filter_map(|name| match name {
            x509_parser::extensions::GeneralName::DNSName(dns) => {
                let dns = match dns.strip_prefix("*.") {
                    Some(rest) => format!("rotation-check.{rest}"),
                    None => (*dns).to_string(),
                };
                ServerName::try_from(dns).ok()
            }
            x509_parser::extensions::GeneralName::IPAddress(ip) => match ip.len() {
                4 => <[u8; 4]>::try_from(*ip)
                    .ok()
                    .map(|b| ServerName::from(std::net::IpAddr::from(b))),
                16 => <[u8; 16]>::try_from(*ip)
                    .ok()
                    .map(|b| ServerName::from(std::net::IpAddr::from(b))),
                _ => None,
            },
            _ => None,
        })
        .collect()
}
