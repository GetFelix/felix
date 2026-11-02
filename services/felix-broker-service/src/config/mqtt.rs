//! The MQTT listener's settings, all from `FELIX_MQTT_*`.
//!
//! Off unless `FELIX_MQTT_LISTEN` is set. What the listener serves and why is
//! in `docs/mqtt-design.md`.

use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use felix_mqtt::Settings;

/// Threads in the listener's own runtime.
const DEFAULT_WORKER_THREADS: usize = 2;
/// Connections served at once before new ones are closed on arrival.
const DEFAULT_MAX_CONNECTIONS: usize = 100_000;
/// Connections one source address may hold. Higher than Kafka's because a
/// device fleet often sits behind one carrier NAT.
const DEFAULT_MAX_CONNECTIONS_PER_IP: usize = 1024;
/// New connections accepted per second.
const DEFAULT_CONNECT_RATE: u32 = 2000;

#[derive(Debug, Clone, PartialEq)]
pub struct MqttListenerConfig {
    /// Where the listener binds.
    pub listen: SocketAddr,
    /// Serve TLS with the broker's certificate.
    pub tls: bool,
    pub worker_threads: usize,
    pub max_connections: usize,
    /// Connections one source IP may hold; `0` is unlimited.
    pub max_connections_per_ip: usize,
    /// Connections accepted per second; `0` is unlimited.
    pub connect_rate: u32,
    /// What each connection is held to.
    pub settings: Settings,
}

impl MqttListenerConfig {
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
        let Some(listen) = get("FELIX_MQTT_LISTEN") else {
            return Ok(None);
        };
        let listen: SocketAddr = listen
            .parse()
            .with_context(|| format!("FELIX_MQTT_LISTEN {listen:?} is not an ip:port address"))?;
        let tls = match get("FELIX_MQTT_TLS").as_deref() {
            None | Some("true" | "1") => true,
            Some("false" | "0") => false,
            Some(other) => bail!("FELIX_MQTT_TLS must be true or false, not {other:?}"),
        };
        // Each number is parsed the same way: absent is the default, and a
        // value out of range names the variable instead of being clamped.
        let number = |name: &str, default: u64, min: u64, max: u64| -> Result<u64> {
            let Some(value) = get(name) else {
                return Ok(default);
            };
            u64::from_str(&value)
                .ok()
                .filter(|n| (min..=max).contains(n))
                .with_context(|| {
                    format!("{name} must be an integer from {min} to {max}, not {value:?}")
                })
        };
        let defaults = Settings::default();
        let settings = Settings {
            anonymous_tenant: get("FELIX_MQTT_ANONYMOUS_TENANT"),
            auth_timeout: Duration::from_millis(number(
                "FELIX_MQTT_AUTH_TIMEOUT_MS",
                defaults.auth_timeout.as_millis() as u64,
                1,
                u64::MAX / 1000,
            )?),
            // A packet is at least a fixed header and a length, and the
            // format cannot describe one over 256 MiB.
            max_packet_bytes: number(
                "FELIX_MQTT_MAX_PACKET_BYTES",
                defaults.max_packet_bytes.into(),
                1024,
                268_435_455,
            )? as u32,
            max_topic_bytes: number(
                "FELIX_MQTT_MAX_TOPIC_BYTES",
                defaults.max_topic_bytes as u64,
                1,
                65_535,
            )? as usize,
            max_topic_levels: number(
                "FELIX_MQTT_MAX_TOPIC_LEVELS",
                defaults.max_topic_levels as u64,
                1,
                65_535,
            )? as usize,
            max_subscriptions: number(
                "FELIX_MQTT_MAX_SUBSCRIPTIONS",
                defaults.max_subscriptions as u64,
                1,
                65_535,
            )? as usize,
            keepalive_max_secs: number(
                "FELIX_MQTT_KEEPALIVE_MAX_SECS",
                defaults.keepalive_max_secs.into(),
                1,
                65_535,
            )? as u16,
        };
        Ok(Some(Self {
            listen,
            tls,
            worker_threads: number(
                "FELIX_MQTT_WORKER_THREADS",
                DEFAULT_WORKER_THREADS as u64,
                1,
                256,
            )? as usize,
            max_connections: number(
                "FELIX_MQTT_MAX_CONNECTIONS",
                DEFAULT_MAX_CONNECTIONS as u64,
                1,
                u32::MAX.into(),
            )? as usize,
            max_connections_per_ip: number(
                "FELIX_MQTT_MAX_CONNECTIONS_PER_IP",
                DEFAULT_MAX_CONNECTIONS_PER_IP as u64,
                0,
                u32::MAX.into(),
            )? as usize,
            connect_rate: number(
                "FELIX_MQTT_CONNECT_RATE",
                DEFAULT_CONNECT_RATE.into(),
                0,
                u32::MAX.into(),
            )? as u32,
            settings,
        }))
    }
}
