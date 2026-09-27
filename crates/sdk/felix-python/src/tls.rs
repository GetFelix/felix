//! TLS for the client side of a QUIC connection.
//!
//! QUIC has no unencrypted mode, so every Python client needs a trust
//! decision. Two are offered and no third: the platform trust store (what a
//! broker with a real certificate needs) or an explicit CA file (what a
//! self-signed development broker needs). There is deliberately no
//! "skip verification" switch — it is the one setting that silently turns a
//! secure deployment insecure, and a CA file covers the development case
//! without it.
//!
//! `offer_alpn` offers the `felix/1` ALPN, which a broker with
//! `FELIX_TLS_REQUIRE_ALPN=true` needs. It is off by default because a broker
//! that predates ALPN refuses a client that offers it.
use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use quinn::ClientConfig;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;

pub(crate) fn client_config(ca_file: Option<&str>, offer_alpn: bool) -> PyResult<ClientConfig> {
    match ca_file {
        Some(path) => {
            let mut roots = RootCertStore::empty();
            let mut file = std::io::BufReader::new(std::fs::File::open(path).map_err(|err| {
                PyValueError::new_err(format!("could not read ca_file {path:?}: {err}"))
            })?);
            let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut file)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| {
                    PyValueError::new_err(format!("ca_file {path:?} is not valid PEM: {err}"))
                })?;
            if certs.is_empty() {
                return Err(PyValueError::new_err(format!(
                    "ca_file {path:?} contained no certificates"
                )));
            }
            for cert in certs {
                roots.add(cert).map_err(|err| {
                    PyValueError::new_err(format!(
                        "ca_file {path:?} holds an unusable certificate: {err}"
                    ))
                })?;
            }
            felix_client::quic_client_config(Some(Arc::new(roots)), offer_alpn).map_err(|err| {
                PyValueError::new_err(format!("could not build a TLS config: {err:#}"))
            })
        }
        None => felix_client::quic_client_config(None, offer_alpn).map_err(|err| {
            PyValueError::new_err(format!(
                "could not use the platform trust store: {err:#}. \
                 Pass ca_file= to trust a specific CA instead."
            ))
        }),
    }
}
