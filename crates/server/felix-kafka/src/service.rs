//! Serving one Kafka client connection.
//!
//! Requests on a connection are answered one at a time and in order, as Kafka
//! brokers do: a client may pipeline requests, but it matches responses to
//! requests by order as much as by correlation id. A long-polling fetch
//! therefore holds its connection, which is why a consumer keeps one
//! connection per broker for fetching.

pub(crate) mod connection;

use std::sync::Arc;
use std::time::Duration;

use felix_broker::Broker;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

use crate::cluster::Cluster;

/// Answers Kafka requests from a broker's streams. Cheap to clone.
#[derive(Clone)]
pub struct KafkaService {
    shared: Arc<Shared>,
}

/// How the service behaves, beyond what the cluster answers.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Serve unauthenticated connections as this tenant, with read and write
    /// access to every stream in it. A development switch: without it, a
    /// connection must authenticate with SASL/PLAIN before it can do anything.
    pub anonymous_tenant: Option<String>,
    /// The namespace a topic without a dot is looked up in.
    pub default_namespace: Option<String>,
    /// Reported as the Kafka cluster id.
    pub cluster_id: String,
    /// How long a connection has to authenticate before it is closed. Until
    /// it does, its requests are also capped at a few kilobytes, so an
    /// unauthenticated peer can hold neither a connection slot nor memory.
    pub auth_timeout: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            anonymous_tenant: None,
            default_namespace: None,
            cluster_id: String::new(),
            auth_timeout: Duration::from_secs(10),
        }
    }
}

/// What every connection shares.
pub(crate) struct Shared {
    pub(crate) broker: Arc<Broker>,
    pub(crate) cluster: Arc<dyn Cluster>,
    pub(crate) settings: Settings,
}

impl KafkaService {
    pub fn new(broker: Arc<Broker>, cluster: Arc<dyn Cluster>, settings: Settings) -> Self {
        Self {
            shared: Arc::new(Shared {
                broker,
                cluster,
                settings,
            }),
        }
    }

    /// Serve one connection until the client closes it, it breaks the
    /// protocol, or `shutdown` is cancelled. Shutdown is noticed between
    /// requests and ends a long-polling fetch early, so a draining broker is
    /// not held up by consumers waiting for data.
    pub async fn serve_connection<S>(&self, stream: S, shutdown: CancellationToken)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        self.serve_connection_with_peer(stream, Vec::new(), shutdown)
            .await;
    }

    /// [`Self::serve_connection`] for a TLS connection whose client presented
    /// `peer_certs` (DER, leaf first). SASL hands them to
    /// [`Cluster::authenticate_peer`] with the token.
    pub async fn serve_connection_with_peer<S>(
        &self,
        stream: S,
        peer_certs: Vec<Vec<u8>>,
        shutdown: CancellationToken,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        crate::metrics::connection_opened();
        if let Err(err) = connection::serve(&self.shared, stream, peer_certs, &shutdown).await {
            tracing::debug!(error = %err, "kafka connection ended");
        }
        crate::metrics::connection_closed();
    }
}
