//! TLS for the client side of a QUIC connection.
//!
//! QUIC has no unencrypted mode, so every client needs a trust decision. Two
//! are offered and no third: the platform trust store (what a broker with a
//! real certificate needs) or an explicit CA file (what a self-signed
//! development broker needs). There is deliberately no "skip verification"
//! switch — it is the one setting that silently turns a secure deployment
//! insecure, and a CA file covers the development case without it. The Python
//! binding makes the same two-way choice, for the same reason.
//!
//! `offer_alpn` offers the `felix/1` ALPN, which a broker with
//! `FELIX_TLS_REQUIRE_ALPN=true` needs. It is off by default because a broker
//! that predates ALPN refuses a client that offers it.
use std::sync::Arc;

use crate::errors::invalid;
use quinn::ClientConfig;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;

pub(crate) fn client_config(ca_file: Option<&str>, offer_alpn: bool) -> napi::Result<ClientConfig> {
    match ca_file {
        Some(path) => {
            let mut roots = RootCertStore::empty();
            let mut file = std::io::BufReader::new(
                std::fs::File::open(path)
                    .map_err(|err| invalid(format!("could not read caFile {path:?}: {err}")))?,
            );
            let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut file)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| invalid(format!("caFile {path:?} is not valid PEM: {err}")))?;
            if certs.is_empty() {
                return Err(invalid(format!(
                    "caFile {path:?} contained no certificates"
                )));
            }
            for cert in certs {
                roots.add(cert).map_err(|err| {
                    invalid(format!(
                        "caFile {path:?} holds an unusable certificate: {err}"
                    ))
                })?;
            }
            felix_client::quic_client_config(Some(Arc::new(roots)), offer_alpn)
                .map_err(|err| invalid(format!("could not build a TLS config: {err:#}")))
        }
        None => felix_client::quic_client_config(None, offer_alpn).map_err(|err| {
            invalid(format!(
                "could not use the platform trust store: {err:#}. \
                 Pass caFile to trust a specific CA instead."
            ))
        }),
    }
}
