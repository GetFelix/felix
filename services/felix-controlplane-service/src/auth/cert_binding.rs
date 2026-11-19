//! Binding a bearer token to the client certificate it arrived with
//! (`FELIX_CONTROLPLANE_TLS_CLIENT_CERT_BIND_SUBJECT`).
//!
//! One check in front of every API route, so no handler can forget it. It
//! reads the claims before the handler verifies the signature; a token that
//! names a subject it was not issued with still fails that verification, so
//! a request passes only when both hold.
use std::sync::Arc;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use rustls::pki_types::CertificateDer;

use crate::api::error::ApiError;
use crate::auth::bearer::{Refusal, bearer_token, refused, unverified_claims};

/// The chain the client presented in the TLS handshake, leaf first. The TLS
/// listener puts it on every request of the connection.
#[derive(Clone)]
pub(crate) struct PeerCertificates(pub(crate) Arc<[CertificateDer<'static>]>);

/// Refuse a request whose bearer token is not issued to the client's
/// certificate. A request without a bearer token is left to its handler.
pub(crate) async fn require_bound_subject(request: Request, next: Next) -> Response {
    if let Err(err) = check(&request) {
        return err.into_response();
    }
    next.run(request).await
}

fn check(request: &Request) -> Result<(), ApiError> {
    let Some(token) = bearer_token(request.headers()) else {
        return Ok(());
    };
    let claims = unverified_claims(token)?;
    let subject = claims
        .get("sub")
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            refused(
                Refusal::CertificateMismatch,
                "the token has no subject to bind to the client certificate",
            )
        })?;
    let actor = claims
        .get("act")
        .and_then(|act| act.get("sub"))
        .and_then(|value| value.as_str());
    let peer = request.extensions().get::<PeerCertificates>();
    felix_common::tls::check_subject_binding(peer.map(|certs| &certs.0[..]), subject, actor)
        .map_err(|reason| refused(Refusal::CertificateMismatch, &reason))
}
