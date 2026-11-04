//! MQTT for a Felix broker.
//!
//! **Start at [`MqttService`]**: the broker service accepts a connection,
//! terminates TLS, and hands the stream to [`MqttService::serve_connection`].
//! Whether a token is good is asked through [`Authenticator`], which
//! stream a topic belongs to through [`Mounts`], and how a message is written
//! through [`Writes`]; the service implements all three.
//! `docs/mqtt-design.md` has the whole design and `docs/mqtt-compatibility.md`
//! what works today.

mod auth;
mod metrics;
mod mount;
mod service;
mod settings;
mod topic;
mod write;
pub mod ws;

pub use auth::{AuthError, Authenticator, Credential, Principal};
pub use mount::{MountedTopic, Mounts, NoMounts};
pub use service::MqttService;
pub use settings::Settings;
pub use write::{NoWrites, Record, Refusal, WriteBatch, WriteError, Writes};
