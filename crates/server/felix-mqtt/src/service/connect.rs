//! `CONNECT`: the first packet, and the only one accepted before the client has
//! proved who it is.

use std::time::Duration;

use felix_mqtt_wire::{ConnAck, Connect, Packet, Properties, QoS, ReasonCode, Utf8, Version};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::time::{Instant, timeout_at};
use tokio_util::sync::CancellationToken;

use super::Shared;
use super::connection::{Connection, End};
use crate::auth::{AuthError, Principal};
use crate::settings::Settings;

/// A connection that got past `CONNECT`.
pub(super) struct Session {
    pub(super) version: Version,
    /// How long the client may be silent, before the grace is added.
    pub(super) keep_alive: Duration,
    pub(super) principal: Principal,
    /// The producer id every write on this connection is made under.
    pub(super) producer_id: u64,
    /// Whether the client allows reason strings on acknowledgements (5.0
    /// `Request Problem Information`).
    pub(super) reason_strings: bool,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> Connection<S> {
    /// Read `CONNECT`, check the credential, and answer with `CONNACK`.
    pub(super) async fn accept(
        &mut self,
        shared: &Shared,
        peer_certs: &[Vec<u8>],
        shutdown: &CancellationToken,
    ) -> Result<Session, End> {
        let deadline = Instant::now() + shared.settings.auth_timeout;
        let connect = self
            .read_connect(&shared.settings, deadline, shutdown)
            .await?;
        let version = connect.version;
        self.version = Some(version);

        // Shutdown cuts the check short, so a stalled control plane cannot
        // hold a drain open.
        let checked = tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                return Err(End::Disconnect(ReasonCode::ServerShuttingDown, None));
            }
            checked = timeout_at(deadline, self.authenticate(shared, &connect, peer_certs)) => checked,
        };
        let principal = match checked {
            Ok(Ok(principal)) => principal,
            Ok(Err((reason, metric))) => return self.reject(reason, metric),
            // A DISCONNECT may not precede the CONNACK, so a 5.0 client is
            // answered with one.
            Err(_) => return self.reject(ReasonCode::ServerUnavailable, "auth_timeout"),
        };
        // A will is a publish deferred to when the client vanishes, and the
        // listener does not send them yet. Refusing it now beats never
        // sending it.
        if connect.will.is_some() {
            return self.reject(ReasonCode::NotAuthorized, "will_unsupported");
        }
        // 3.1.1 has no assigned identifier to hand back, so a client that
        // wants its session kept must name itself.
        if connect.client_id.is_empty() && !version_assigns_id(&connect) {
            return self.reject(ReasonCode::ClientIdentifierNotValid, "client_id_invalid");
        }

        let keep_alive = effective_keep_alive(&shared.settings, connect.keep_alive);
        let ack = connack(&shared.settings, &connect, keep_alive);
        let reason_strings = connect
            .properties
            .request_problem_information
            .unwrap_or(true);
        self.queue(&Packet::ConnAck(ack))?;
        self.flush().await?;
        tracing::debug!(tenant = principal.tenant_id(), "mqtt client connected");
        Ok(Session {
            version,
            keep_alive: Duration::from_secs(keep_alive.into()),
            principal,
            producer_id: shared.writes.new_producer_id(),
            reason_strings,
        })
    }

    async fn read_connect(
        &mut self,
        settings: &Settings,
        deadline: Instant,
        shutdown: &CancellationToken,
    ) -> Result<Connect, End> {
        let limits = settings.limits_before_connect();
        loop {
            match self.next_packet(None, &limits) {
                Ok(Some(Packet::Connect(connect))) => return Ok(connect),
                Ok(Some(_)) => {
                    return Err(End::Disconnect(
                        ReasonCode::ProtocolError,
                        Some("protocol_error"),
                    ));
                }
                Ok(None) => {}
                Err(end) => return Err(self.unsupported_version_reply(end)),
            }
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    return Err(End::Disconnect(ReasonCode::ServerShuttingDown, None));
                }
                read = timeout_at(deadline, self.io.read_buf(&mut self.inbound)) => match read {
                    Err(_) => return Err(End::Close("auth_timeout")),
                    Ok(Ok(0) | Err(_)) => return Err(End::Gone),
                    Ok(Ok(_)) => {}
                },
            }
        }
    }

    /// A client that asks for a protocol level this listener lacks is told so
    /// in the one form every version understands, then closed.
    fn unsupported_version_reply(&mut self, end: End) -> End {
        if let End::Disconnect(ReasonCode::UnsupportedProtocolVersion, _) = end {
            let refusal = Packet::ConnAck(ConnAck {
                session_present: false,
                reason: ReasonCode::UnsupportedProtocolVersion,
                properties: Properties::default(),
            });
            let _ = self.queue(&refusal);
            return End::Close("unsupported_version");
        }
        end
    }

    /// The tenant and token in `CONNECT`, checked. Without credentials a
    /// connection is the anonymous tenant's, if the broker has one.
    async fn authenticate(
        &self,
        shared: &Shared,
        connect: &Connect,
        peer_certs: &[Vec<u8>],
    ) -> Result<Principal, (ReasonCode, &'static str)> {
        let (Some(tenant), password) = (&connect.username, &connect.password) else {
            // A password with no username is credentials, wrong ones, and is
            // not downgraded to anonymous.
            if connect.password.is_some() {
                return Err((ReasonCode::BadUserNameOrPassword, "auth_failed"));
            }
            return match &shared.settings.anonymous_tenant {
                Some(tenant) => Ok(Principal::anonymous(tenant.clone())),
                None => Err((ReasonCode::NotAuthorized, "auth_required")),
            };
        };
        let token = password
            .as_deref()
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .filter(|token| !token.is_empty())
            .ok_or((ReasonCode::BadUserNameOrPassword, "auth_failed"))?;
        match shared
            .auth
            .authenticate(tenant.as_str(), token, peer_certs)
            .await
        {
            Ok(principal) => Ok(principal),
            Err(AuthError::Rejected(reason)) => {
                tracing::debug!(reason = %reason, "mqtt client failed to authenticate");
                Err((ReasonCode::BadUserNameOrPassword, "auth_failed"))
            }
            Err(AuthError::Unavailable) => Err((ReasonCode::ServerUnavailable, "auth_unavailable")),
        }
    }

    /// Refuse the `CONNECT` with a `CONNACK` and close.
    fn reject<T>(&mut self, reason: ReasonCode, metric: &'static str) -> Result<T, End> {
        self.queue(&Packet::ConnAck(ConnAck {
            session_present: false,
            reason,
            properties: Properties::default(),
        }))?;
        Err(End::Close(metric))
    }
}

/// 5.0 hands back an identifier for an empty one; 3.1.1 may only when the
/// session is clean.
fn version_assigns_id(connect: &Connect) -> bool {
    connect.version == Version::V5 || connect.clean_start
}

/// The keepalive that is enforced. A client that asks for none, or for more
/// than the broker allows, gets the broker's longest, so a silent connection
/// cannot hold a slot forever.
fn effective_keep_alive(settings: &Settings, asked: u16) -> u16 {
    match asked {
        0 => settings.keepalive_max_secs,
        asked => asked.min(settings.keepalive_max_secs),
    }
}

fn connack(settings: &Settings, connect: &Connect, keep_alive: u16) -> ConnAck {
    let mut properties = Properties::default();
    if connect.version == Version::V5 {
        if keep_alive != connect.keep_alive {
            properties.server_keep_alive = Some(keep_alive);
        }
        if connect.client_id.is_empty() {
            let id = format!("felix-{}", uuid::Uuid::new_v4().simple());
            properties.assigned_client_identifier = Utf8::new(id).ok();
        }
        // Sessions do not outlive the connection yet.
        if connect
            .properties
            .session_expiry_interval
            .is_some_and(|secs| secs > 0)
        {
            properties.session_expiry_interval = Some(0);
        }
        properties.maximum_packet_size = Some(settings.max_packet_bytes);
        properties.receive_maximum = Some(settings.max_inflight);
        // QoS 2 comes with release marks in a later slice.
        properties.maximum_qos = Some(QoS::AtLeastOnce);
        properties.retain_available = Some(false);
        properties.shared_subscription_available = Some(false);
        properties.subscription_identifiers_available = Some(false);
    }
    ConnAck {
        session_present: false,
        reason: ReasonCode::Success,
        properties,
    }
}
