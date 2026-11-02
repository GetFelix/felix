//! MQTT for a Felix broker.
//!
//! **Start at [`MqttService`]**: the broker service accepts a connection,
//! terminates TLS, and hands the stream to [`MqttService::serve_connection`].
//! Whether a token is good is asked through [`Authenticator`], which the
//! service implements. `docs/mqtt-design.md` has the whole design and
//! `docs/mqtt-compatibility.md` what works today.

mod auth;
mod metrics;
mod service;
mod settings;
pub mod ws;

pub use auth::{AuthError, Authenticator, Principal};
pub use service::MqttService;
pub use settings::Settings;
