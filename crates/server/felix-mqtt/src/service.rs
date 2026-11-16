//! Serving one MQTT client connection.
//!
//! A connection is one task: it reads packets, answers them in order, and
//! writes the answers once per batch it read. A PUBLISH is answered when its
//! write is acknowledged, in `publish.rs`. Before `CONNECT` is accepted the
//! only things it can do are authenticate or be closed.

mod connect;
mod connection;
mod publish;
mod refuse;
mod subscribe;
#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

use crate::auth::Authenticator;
use crate::mount::Mounts;
use crate::read::{NoReads, Reads};
use crate::settings::Settings;
use crate::write::Writes;

/// Answers MQTT clients. Cheap to clone.
#[derive(Clone)]
pub struct MqttService {
    shared: Arc<Shared>,
}

/// What every connection shares.
pub(crate) struct Shared {
    pub(crate) auth: Arc<dyn Authenticator>,
    pub(crate) mounts: Arc<dyn Mounts>,
    pub(crate) writes: Arc<dyn Writes>,
    pub(crate) reads: Arc<dyn Reads>,
    pub(crate) settings: Settings,
    pub(crate) readers: Arc<ReaderSlots>,
}

/// How many reader tasks run on the listener, against `Settings::max_readers`.
#[derive(Default)]
pub(crate) struct ReaderSlots(AtomicUsize);

impl ReaderSlots {
    /// `count` slots, or `None` when that would pass `max`. They are given
    /// back when the guards drop.
    pub(crate) fn acquire(self: &Arc<Self>, count: usize, max: usize) -> Option<Vec<ReaderSlot>> {
        let mut held = self.0.load(Ordering::Relaxed);
        loop {
            if held + count > max {
                return None;
            }
            match self.0.compare_exchange_weak(
                held,
                held + count,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(now) => held = now,
            }
        }
        metrics::gauge!("felix_mqtt_readers").increment(count as f64);
        Some((0..count).map(|_| ReaderSlot(self.clone())).collect())
    }
}

pub(crate) struct ReaderSlot(Arc<ReaderSlots>);

impl Drop for ReaderSlot {
    fn drop(&mut self) {
        self.0.0.fetch_sub(1, Ordering::AcqRel);
        metrics::gauge!("felix_mqtt_readers").decrement(1.0);
    }
}

impl MqttService {
    pub fn new(
        auth: Arc<dyn Authenticator>,
        mounts: Arc<dyn Mounts>,
        writes: Arc<dyn Writes>,
        settings: Settings,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                auth,
                mounts,
                writes,
                reads: Arc::new(NoReads),
                settings,
                readers: Arc::default(),
            }),
        }
    }

    /// Serve SUBSCRIBE from `reads`. Without it every SUBSCRIBE is refused.
    pub fn with_reads(mut self, reads: Arc<dyn Reads>) -> Self {
        let shared = Arc::get_mut(&mut self.shared).expect("set before the service is cloned");
        shared.reads = reads;
        self
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
