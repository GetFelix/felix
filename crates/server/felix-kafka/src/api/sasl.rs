//! SASL/PLAIN: the username is the tenant, the password is a Felix token.
//!
//! The token is verified exactly as the QUIC `Auth` frame's is, by the
//! cluster's [`crate::Cluster::authenticate`]. A failed attempt is answered and
//! then the connection closes, as Kafka brokers do.

use anyhow::Result;
use bytes::Bytes;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::{
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
};
use kafka_protocol::protocol::StrBytes;

use super::{SaslState, Session};
use crate::service::Shared;

const PLAIN: &str = "PLAIN";

pub(super) fn handshake(
    session: &mut Session,
    request: SaslHandshakeRequest,
    version: i16,
) -> Result<(Bytes, i16)> {
    let mut response =
        SaslHandshakeResponse::default().with_mechanisms(vec![StrBytes::from_static_str(PLAIN)]);
    if request.mechanism.as_str() == PLAIN && version >= 1 {
        session.sasl = SaslState::Handshaken;
    } else {
        response.error_code = ResponseError::UnsupportedSaslMechanism.code();
        session.closing = true;
    }
    let error = response.error_code;
    super::encode(&response, version, error)
}

pub(super) async fn authenticate(
    shared: &Shared,
    session: &mut Session,
    request: SaslAuthenticateRequest,
    version: i16,
) -> Result<(Bytes, i16)> {
    let outcome = if session.sasl != SaslState::Handshaken {
        Err((
            ResponseError::IllegalSaslState,
            "SaslAuthenticate before a SaslHandshake choosing PLAIN".to_string(),
        ))
    } else {
        match parse_plain(&request.auth_bytes) {
            Some((tenant, token)) => shared
                .cluster
                .authenticate_peer(&tenant, &token, &session.peer_certs)
                .await
                .map_err(|reason| (ResponseError::SaslAuthenticationFailed, reason)),
            None => Err((
                ResponseError::SaslAuthenticationFailed,
                "malformed SASL/PLAIN message".to_string(),
            )),
        }
    };
    session.sasl = SaslState::Idle;
    let response = match outcome {
        Ok(principal) => {
            tracing::debug!(tenant = principal.tenant_id(), "kafka client authenticated");
            session.principal = Some(principal);
            SaslAuthenticateResponse::default()
        }
        Err((error, reason)) => {
            tracing::info!(reason = %reason, "kafka client failed to authenticate");
            session.closing = true;
            SaslAuthenticateResponse::default()
                .with_error_code(error.code())
                .with_error_message(Some(StrBytes::from_string(format!(
                    "Felix: {reason}. Use the tenant id as the username and a Felix token as the password."
                ))))
        }
    };
    let error = response.error_code;
    super::encode(&response, version, error)
}

/// `[authzid] NUL authcid NUL passwd`, as RFC 4616 has it. An authorization
/// id naming anyone but the authenticating tenant is refused rather than
/// ignored: honouring it would be acting as a tenant the token was not checked
/// against.
pub(crate) fn parse_plain(message: &[u8]) -> Option<(String, String)> {
    let mut parts = message.split(|byte| *byte == 0);
    let authzid = parts.next()?;
    let authcid = parts.next()?;
    let passwd = parts.next()?;
    if parts.next().is_some() || authcid.is_empty() || passwd.is_empty() {
        return None;
    }
    if !authzid.is_empty() && authzid != authcid {
        return None;
    }
    Some((
        String::from_utf8(authcid.to_vec()).ok()?,
        String::from_utf8(passwd.to_vec()).ok()?,
    ))
}
