//! The Kafka listener's settings, all from `FELIX_KAFKA_*`.
//!
//! Off unless `FELIX_KAFKA_LISTEN` is set. What the listener serves and why is
//! in `docs/kafka-compatibility.md`.

use std::net::SocketAddr;

use anyhow::{Context, Result, bail};

/// Connections accepted at once before new ones are closed on arrival.
const DEFAULT_MAX_CONNECTIONS: usize = 1024;
/// Connections one source address may hold. A Kafka client keeps one or two
/// per broker, so this leaves room for many clients behind one NAT while
/// keeping a single host from taking every slot.
const DEFAULT_MAX_CONNECTIONS_PER_IP: usize = 128;
/// How long a connection has to finish SASL. Clients authenticate right after
/// connecting, so this only ever closes one that is not going to.
const DEFAULT_AUTH_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaListenerConfig {
    /// Where the listener binds.
    pub listen: SocketAddr,
    /// `host:port` clients are told to connect to for this broker.
    pub advertise: String,
    /// Serve TLS (clients use `SASL_SSL`) with the broker's certificate.
    pub tls: bool,
    /// Serve unauthenticated connections as this tenant. A development
    /// switch.
    pub anonymous_tenant: Option<String>,
    /// Where a topic without a dot is looked up.
    pub default_namespace: Option<String>,
    pub max_connections: usize,
    /// Connections one source IP may hold; `0` is unlimited.
    pub max_connections_per_ip: usize,
    /// How long an unauthenticated connection is kept.
    pub auth_timeout_ms: u64,
}

impl KafkaListenerConfig {
    pub fn from_env() -> Result<Option<Self>> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// [`Self::from_env`] over any source of variables, so the parsing is
    /// testable without touching the process environment.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Option<Self>> {
        let get = |name: &str| {
            lookup(name)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        let Some(listen) = get("FELIX_KAFKA_LISTEN") else {
            return Ok(None);
        };
        let listen: SocketAddr = listen
            .parse()
            .with_context(|| format!("FELIX_KAFKA_LISTEN {listen:?} is not an ip:port address"))?;
        let advertise = get("FELIX_KAFKA_ADVERTISE_ADDR").unwrap_or_else(|| listen.to_string());
        if felix_kafka::Endpoint::parse("", &advertise).is_none() {
            bail!("FELIX_KAFKA_ADVERTISE_ADDR {advertise:?} is not a host:port address");
        }
        let tls = match get("FELIX_KAFKA_TLS").as_deref() {
            None | Some("true" | "1") => true,
            Some("false" | "0") => false,
            Some(other) => bail!("FELIX_KAFKA_TLS must be true or false, not {other:?}"),
        };
        let max_connections = match get("FELIX_KAFKA_MAX_CONNECTIONS") {
            None => DEFAULT_MAX_CONNECTIONS,
            Some(value) => value
                .parse::<usize>()
                .ok()
                .filter(|max| *max > 0)
                .with_context(|| {
                    format!("FELIX_KAFKA_MAX_CONNECTIONS must be a positive integer, not {value:?}")
                })?,
        };
        let max_connections_per_ip = match get("FELIX_KAFKA_MAX_CONNECTIONS_PER_IP") {
            None => DEFAULT_MAX_CONNECTIONS_PER_IP,
            Some(value) => value.parse::<usize>().ok().with_context(|| {
                format!(
                    "FELIX_KAFKA_MAX_CONNECTIONS_PER_IP must be a non-negative integer, not {value:?}"
                )
            })?,
        };
        let auth_timeout_ms = match get("FELIX_KAFKA_AUTH_TIMEOUT_MS") {
            None => DEFAULT_AUTH_TIMEOUT_MS,
            Some(value) => value
                .parse::<u64>()
                .ok()
                .filter(|ms| *ms > 0)
                .with_context(|| {
                    format!("FELIX_KAFKA_AUTH_TIMEOUT_MS must be a positive integer, not {value:?}")
                })?,
        };
        Ok(Some(Self {
            listen,
            advertise,
            tls,
            anonymous_tenant: get("FELIX_KAFKA_ANONYMOUS_TENANT"),
            default_namespace: get("FELIX_KAFKA_DEFAULT_NAMESPACE"),
            max_connections,
            max_connections_per_ip,
            auth_timeout_ms,
        }))
    }
}
