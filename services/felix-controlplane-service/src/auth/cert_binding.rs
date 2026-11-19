//! Binding a bearer token to the client certificate it arrived with
//! (`FELIX_CONTROLPLANE_TLS_CLIENT_CERT_BIND_SUBJECT`).
//!
//! One check in front of every API route, so no handler can forget it. It
//! reads the claims before the handler verifies the signature; a token that
//! names a subject it was not issued with still fails that verification, so
//! a request passes only when both hold.
//!
//! Token exchange is the one route whose bearer is not a Felix token: it is
//! the identity provider's, and its raw `sub` is not a Felix principal. There
//! the binding applies to the principal the exchange resolves instead (see
//! [`ExchangeBinding`]). Refresh carries no bearer at all, so it is bound to
//! the principal (or actor) its refresh token was issued for.
use std::sync::Arc;

use axum::Router;
use axum::extract::{MatchedPath, Request};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use rustls::pki_types::CertificateDer;

use crate::api::error::ApiError;
use crate::auth::bearer::{Refusal, bearer_token, refused, unverified_claims};

/// The token exchange route, whose bearer is an identity provider's token.
pub(crate) const EXCHANGE_ROUTE: &str = "/v1/tenants/{tenant_id}/token/exchange";

/// The token refresh route, whose credential is in the body.
pub(crate) const REFRESH_ROUTE: &str = "/v1/tenants/{tenant_id}/token/refresh";

/// The chain the client presented in the TLS handshake, leaf first. The TLS
/// listener puts it on every request of the connection.
#[derive(Clone)]
pub struct PeerCertificates(pub Arc<[CertificateDer<'static>]>);

/// Put on a token exchange or refresh when binding is on: the token it mints
/// must be issued to the presented certificate, by its principal or its
/// `may_act`.
#[derive(Clone)]
pub struct ExchangeBinding(pub(crate) Option<PeerCertificates>);

impl ExchangeBinding {
    /// Whether `principal`, or the `actor` that may present its token, is
    /// what the client's certificate was issued to.
    pub(crate) fn check(&self, principal: &str, actor: Option<&str>) -> Result<(), ApiError> {
        let peer = self.0.as_ref().map(|certs| &certs.0[..]);
        felix_common::tls::check_subject_binding(peer, principal, actor)
            .map_err(|reason| refused(Refusal::CertificateMismatch, &reason))
    }
}

/// `router` with every bearer token bound to the client certificate.
pub fn bind_subjects(router: Router) -> Router {
    router.layer(axum::middleware::from_fn(require_bound_subject))
}

/// Refuse a request whose bearer token is not issued to the client's
/// certificate. A request without a bearer token is left to its handler.
async fn require_bound_subject(mut request: Request, next: Next) -> Response {
    let mints = request
        .extensions()
        .get::<MatchedPath>()
        .is_some_and(|path| matches!(path.as_str(), EXCHANGE_ROUTE | REFRESH_ROUTE));
    if mints {
        let peer = request.extensions().get::<PeerCertificates>().cloned();
        request.extensions_mut().insert(ExchangeBinding(peer));
    } else if let Err(err) = check(&request) {
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
