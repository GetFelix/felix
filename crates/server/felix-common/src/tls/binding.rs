//! Binding a client certificate to the principal its bearer token names.
//!
//! The listener verified the chain in the handshake; this checks that the
//! certificate was issued to the token's subject, so a stolen token is no use
//! without the key of a certificate issued to the same principal. The broker
//! and the control plane apply the same rules.

/// The URI subject alternative name that binds a certificate to a principal:
/// `felix:principal:<sub>`, with the token's `sub` verbatim.
pub const PRINCIPAL_URI_PREFIX: &str = "felix:principal:";

/// Whether a token's `subject`, or its `actor`, is a principal the client's
/// certificate was issued to, when the client presented one.
///
/// The chain was verified in the handshake; this binds the certificate to
/// the principal the token claims, so a stolen token is no use without the
/// key of a certificate issued to the same principal. The certificate binds a
/// subject if its SANs include the URI `felix:principal:<subject>` (exact
/// match), or, for a human-readable subject, a DNS or IP name the subject
/// matches the way a server name would, wildcards included. Control-plane
/// principal ids are 64 hex characters, one label longer than DNS allows,
/// so they can only bind through the URI.
///
/// A token the control plane minted by delegation names its actor (`act`):
/// the gateway that presents it for the user in `sub`. Such a token also
/// binds when the certificate is issued to the actor, by the same rules, so
/// a gateway's own certificate carries its users' tokens and nobody else's.
/// The control plane only names an actor it authenticated, and the claim is
/// covered by the token's signature.
///
/// A client with no certificate passes, since only a listener configured
/// with a client CA asks for one, and that listener refuses the handshake
/// without it.
pub fn check_subject_binding(
    peer_certs: Option<&[rustls::pki_types::CertificateDer<'_>]>,
    subject: &str,
    actor: Option<&str>,
) -> std::result::Result<(), String> {
    let Some(leaf) = peer_certs.and_then(|certs| certs.first()) else {
        return Ok(());
    };
    // The messages name no values: they end up in logs, and the subject comes
    // from the token.
    if binds(leaf, subject)? {
        return Ok(());
    }
    if let Some(actor) = actor
        && binds(leaf, actor)?
    {
        return Ok(());
    }
    Err(match actor {
        None => "the client certificate is not issued to the token's subject: no matching \
                 felix:principal URI, DNS or IP name"
            .to_string(),
        Some(_) => "the client certificate is not issued to the token's subject or its \
                    actor: no matching felix:principal URI, DNS or IP name"
            .to_string(),
    })
}

/// Whether `leaf` is issued to `principal`: a `felix:principal:` URI SAN, or
/// a DNS or IP name it matches.
fn binds(
    leaf: &rustls::pki_types::CertificateDer<'_>,
    principal: &str,
) -> std::result::Result<bool, String> {
    if carries_principal_uri(leaf, principal)? {
        return Ok(true);
    }
    let Ok(name) = rustls::pki_types::ServerName::try_from(principal) else {
        return Ok(false);
    };
    let cert = webpki::EndEntityCert::try_from(leaf)
        .map_err(|err| format!("the client certificate does not parse: {err}"))?;
    Ok(cert.verify_is_valid_for_subject_name(&name).is_ok())
}

/// Whether `leaf` has the URI SAN `felix:principal:<subject>`. An empty
/// subject never matches, so the bare prefix names nobody.
fn carries_principal_uri(
    leaf: &rustls::pki_types::CertificateDer<'_>,
    subject: &str,
) -> std::result::Result<bool, String> {
    if subject.is_empty() {
        return Ok(false);
    }
    let (_, cert) = x509_parser::parse_x509_certificate(leaf.as_ref())
        .map_err(|err| format!("the client certificate does not parse: {err}"))?;
    let sans = cert
        .subject_alternative_name()
        .map_err(|err| format!("the client certificate's SANs do not parse: {err}"))?;
    Ok(sans.is_some_and(|sans| {
        sans.value.general_names.iter().any(|name| {
            matches!(
                name,
                x509_parser::extensions::GeneralName::URI(uri)
                    if uri.strip_prefix(PRINCIPAL_URI_PREFIX) == Some(subject)
            )
        })
    }))
}
