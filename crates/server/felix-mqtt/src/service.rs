//! Serving one MQTT client connection.
//!
//! A connection is one task: it reads packets, answers them in order, and
//! writes the answers once per batch it read. Before `CONNECT` is accepted the
//! only things it can do are authenticate or be closed.

mod connect;
mod connection;
mod refuse;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

use crate::auth::Authenticator;
use crate::settings::Settings;

/// Answers MQTT clients. Cheap to clone.
#[derive(Clone)]
pub struct MqttService {
    shared: Arc<Shared>,
}

/// What every connection shares.
pub(crate) struct Shared {
    pub(crate) auth: Arc<dyn Authenticator>,
    pub(crate) settings: Settings,
}

impl MqttService {
    pub fn new(auth: Arc<dyn Authenticator>, settings: Settings) -> Self {
        Self {
            shared: Arc::new(Shared { auth, settings }),
        }
    }

    /// Serve one connection until the client closes it, it breaks the
    /// protocol, its credential ends, or `shutdown` is cancelled. `peer_certs`
    /// is the client's certificate chain (DER, leaf first) on a TLS connection
    /// that asked for one, empty otherwise.
    pub async fn serve_connection<S>(
        &self,
        stream: S,
        peer_certs: Vec<Vec<u8>>,
        shutdown: CancellationToken,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        connection::serve(&self.shared, stream, peer_certs, shutdown).await;
    }
}
