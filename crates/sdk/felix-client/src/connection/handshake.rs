//! Opening an authenticated stream, and the capability negotiation that
//! rides on it.

use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_transport::QuicConnection;
use felix_wire::Message;
use quinn::{RecvStream, SendStream};
use tracing::debug;

use crate::auth::TokenProvider;
use crate::frame_io::{read_message_with_limit, write_message};

/// The tenant and token source every stream authenticates with.
pub(crate) struct Credentials {
    tenant_id: String,
    tokens: Arc<dyn TokenProvider>,
    /// The feature bits offered in `Auth`.
    features: u32,
    /// The frame-flag bits offered in `Auth`.
    flags: u16,
}

impl Credentials {
    pub(crate) fn new(tenant_id: String, tokens: Arc<dyn TokenProvider>) -> Self {
        Self {
            tenant_id,
            tokens,
            // Each costs something on every exchange it changes, so it is
            // offered only when the application asks.
            features: felix_wire::KNOWN_FEATURES
                & !felix_wire::FEATURE_ACK_ON_COMMIT
                & !felix_wire::FEATURE_GROUP_PUBLISHER
                & !felix_wire::FEATURE_RECORD_TIMESTAMPS,
            flags: felix_wire::KNOWN_FLAGS
                & !felix_wire::FLAG_EVENT_BATCH_PUBLISHER
                & !felix_wire::FLAG_EVENT_BATCH_TIMESTAMPS,
        }
    }

    /// The same offers, authenticating as a different principal.
    pub(crate) fn for_identity(&self, tenant_id: String, tokens: Arc<dyn TokenProvider>) -> Self {
        Self {
            tenant_id,
            tokens,
            features: self.features,
            flags: self.flags,
        }
    }

    /// Also ask for each record's append time. See
    /// [`felix_wire::FLAG_EVENT_BATCH_TIMESTAMPS`].
    pub(crate) fn with_timestamps(mut self, timestamps: bool) -> Self {
        if timestamps {
            self.features |= felix_wire::FEATURE_RECORD_TIMESTAMPS;
            self.flags |= felix_wire::FLAG_EVENT_BATCH_TIMESTAMPS;
        }
        self
    }

    /// Also ask to be told who published each event. See
    /// [`felix_wire::FLAG_EVENT_BATCH_PUBLISHER`].
    pub(crate) fn with_publishers(mut self, publishers: bool) -> Self {
        if publishers {
            self.features |= felix_wire::FEATURE_GROUP_PUBLISHER;
            self.flags |= felix_wire::FLAG_EVENT_BATCH_PUBLISHER;
        }
        self
    }

    /// Also ask for acknowledgements after the write. See
    /// [`felix_wire::FEATURE_ACK_ON_COMMIT`].
    pub(crate) fn with_ack_on_commit(mut self, ack_on_commit: bool) -> Self {
        if ack_on_commit {
            self.features |= felix_wire::FEATURE_ACK_ON_COMMIT;
        }
        self
    }

    /// Authenticate `first`, a stream just opened on `connection`, with the
    /// current token.
    ///
    /// If the broker refuses the token and the provider has a different one,
    /// retry once on a new stream (the broker closes a stream after a failed
    /// auth). This catches a token that expired earlier than `exp` suggested,
    /// e.g. from clock skew.
    pub(crate) async fn open(
        &self,
        connection: &QuicConnection,
        first: (SendStream, RecvStream),
        max_frame_bytes: usize,
    ) -> Result<(SendStream, RecvStream, Negotiated)> {
        let mut token = self.tokens.token().await?;
        let mut retried = false;
        let mut next = Some(first);
        loop {
            let (mut send, mut recv) = match next.take() {
                Some(pair) => pair,
                None => connection.open_bi().await?,
            };
            match authenticate_stream(
                &mut send,
                &mut recv,
                &self.tenant_id,
                &token,
                self.flags,
                self.features,
                max_frame_bytes,
            )
            .await
            {
                Ok(negotiated) => return Ok((send, recv, negotiated)),
                Err(err) if !retried && err.downcast_ref::<AuthRejected>().is_some() => {
                    self.tokens.invalidate(&token);
                    let fresh = self.tokens.token().await?;
                    if fresh == token {
                        return Err(err);
                    }
                    debug!("auth refused; retrying with a fresh token");
                    token = fresh;
                    retried = true;
                }
                Err(err) => return Err(err),
            }
        }
    }
}

impl Credentials {
    /// Authenticate `connection` without waiting for an answer, on a uni
    /// stream that carries only the `Auth`.
    ///
    /// A broker closes a connection that has authenticated nothing within its
    /// auth timeout, and a pooled connection may sit idle until much later.
    /// Nothing is read back: a refused token shows up on the first real
    /// stream, which authenticates again.
    pub(crate) async fn announce(&self, connection: &QuicConnection) -> Result<()> {
        let token = self.tokens.token().await?;
        let mut send = connection.open_uni().await?;
        write_message(
            &mut send,
            Message::Auth {
                tenant_id: self.tenant_id.clone(),
                token,
                client_flags: None,
                client_features: None,
                client_features_hi: None,
            },
        )
        .await
        .context("send auth")?;
        send.finish().context("finish auth stream")?;
        Ok(())
    }
}

/// What one authenticated stream agreed with the broker.
#[derive(Debug, Clone)]
pub(crate) struct Negotiated {
    /// Frame-flag bits: how payloads may be laid out.
    pub(crate) server_flags: u16,
    /// Feature bits: which optional requests the broker implements.
    pub(crate) server_features: u32,
    /// Every port the broker's client-facing listeners are bound to, when it
    /// reported more than one. Empty otherwise, which is the same instruction:
    /// keep using the address already dialled.
    pub(crate) listener_ports: Vec<u16>,
    /// How many acknowledged publishes the connection may have unanswered,
    /// with answers in request order. `0` when the broker does not pipeline.
    pub(crate) publish_window: u32,
}

/// The broker refused a stream's credentials.
#[derive(Debug, thiserror::Error)]
#[error("auth rejected: {0}")]
struct AuthRejected(String);

/// Authenticate a stream and negotiate capabilities.
///
/// Returns what the broker agreed to: its frame-flag and feature bits, and its
/// listener ports. Negotiation rides on the auth handshake because that is
/// already the first round trip on every stream, so it costs no extra latency.
///
/// A broker that predates negotiation ignores `client_flags` (serde skips
/// unknown fields) and answers with a plain `Ok`. That silence is not treated
/// as "supports everything": it resolves to [`felix_wire::ORIGINAL_V1_FLAGS`],
/// the three bits that existed before negotiation, which is the only
/// assumption that is safe against a broker we cannot interrogate.
async fn authenticate_stream(
    send: &mut SendStream,
    recv: &mut RecvStream,
    tenant_id: &str,
    token: &str,
    flags: u16,
    features: u32,
    max_frame_bytes: usize,
) -> Result<Negotiated> {
    let (features, features_hi) =
        felix_wire::offer_features(features, felix_wire::KNOWN_FEATURES_HI);
    write_message(
        send,
        Message::Auth {
            tenant_id: tenant_id.to_string(),
            token: token.to_string(),
            client_flags: Some(flags),
            client_features: Some(features),
            client_features_hi: features_hi,
        },
    )
    .await
    .context("send auth")?;
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    match read_message_with_limit(recv, &mut scratch, max_frame_bytes).await? {
        Some(Message::AuthOk {
            server_flags,
            server_features,
            // No extended feature yet. The first one reads it with
            // `felix_wire::peer_features_hi`.
            server_features_hi: _,
            listener_ports,
            publish_window,
        }) => Ok(Negotiated {
            server_flags,
            // Absent means a broker that predates features. It implements none:
            // an unrecognised message type is fatal to the broker's control
            // loop, so a client that guessed would cost itself the connection.
            server_features: server_features.unwrap_or(0),
            listener_ports: listener_ports.unwrap_or_default(),
            // Only meaningful with the feature bit; a window without it would
            // be a broker promising an order it never agreed to.
            publish_window: publish_window
                .filter(|_| {
                    felix_wire::supports_feature(
                        server_features.unwrap_or(0),
                        felix_wire::FEATURE_PUBLISH_PIPELINE,
                    )
                })
                .unwrap_or(0),
        }),
        // Legacy broker: no advertisement, so assume only the original bits.
        Some(Message::Ok) => Ok(Negotiated {
            server_flags: felix_wire::ORIGINAL_V1_FLAGS,
            server_features: 0,
            listener_ports: Vec::new(),
            publish_window: 0,
        }),
        Some(Message::Error {
            message,
            code: Some(code),
            retry,
            detail,
        }) => {
            Err(
                crate::error::refused("auth rejected", message.clone(), Some(code), retry, detail)
                    .context(AuthRejected(message)),
            )
        }
        Some(Message::Error { message, .. }) => Err(AuthRejected(message).into()),
        Some(other) => Err(anyhow::anyhow!("unexpected auth response: {other:?}")),
        None => Err(anyhow::anyhow!("auth response missing")),
    }
}

#[cfg(test)]
mod tests;
