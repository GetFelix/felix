//! Keeping a long-lived stream on the provider's current token.
//!
//! The broker authenticates a stream once and refuses a second `Auth` on it,
//! but a write it forwards to a shard's owner carries the token the stream
//! opened with, and the owner checks its expiry. A pooled stream that outlives
//! its token would so have its forwarded writes refused. Instead of waiting for
//! that, a stream's owner asks [`Renewal`] before each request whether the
//! provider has a newer token, and moves to a new stream authenticated with it.

use std::sync::Arc;
use std::time::Duration;

use felix_transport::QuicConnection;
use quinn::{RecvStream, SendStream};
use tokio::time::Instant;
use tracing::{debug, warn};

use super::Credentials;
use super::node::{Probe, try_open_bi};

/// How often a stream asks the provider whether its token changed. The
/// provider is called at most this often per stream, so a token that does not
/// cache is not asked per request.
const CHECK_EVERY: Duration = Duration::from_secs(5);

/// How long a check waits on the provider. A fetch that is still running
/// keeps going and the next check joins it; the stream carries on with the
/// token it has meanwhile.
const FETCH_WAIT: Duration = Duration::from_secs(1);

/// How long a moved stream waits for the broker to end the old one.
const DRAIN_WAIT: Duration = Duration::from_secs(5);

/// Extra time before asking again after a check that got no token.
const FAILED_CHECK_BACKOFF: Duration = Duration::from_secs(25);

/// One stream's token, and what it takes to replace the stream when the
/// provider has a newer one.
pub(crate) struct Renewal {
    credentials: Arc<Credentials>,
    connection: QuicConnection,
    /// The token the stream authenticated with.
    token: String,
    checked: Instant,
    every: Duration,
    max_frame_bytes: usize,
}

impl Renewal {
    pub(crate) fn new(
        credentials: Arc<Credentials>,
        connection: QuicConnection,
        token: String,
        max_frame_bytes: usize,
    ) -> Self {
        Self {
            credentials,
            connection,
            token,
            checked: Instant::now(),
            every: CHECK_EVERY,
            max_frame_bytes,
        }
    }

    /// Check on every request, so a test need not wait out the interval.
    #[cfg(test)]
    pub(crate) fn checking_every(mut self, every: Duration) -> Self {
        self.every = every;
        self
    }

    /// Whether [`Renewal::newer_token`] would ask the provider now. Cheap, so an
    /// owner can test it per request and settle its in-flight work only when
    /// it is true.
    pub(crate) fn due(&self) -> bool {
        self.checked.elapsed() >= self.every
    }

    /// The provider's token if it differs from the stream's. Starts the
    /// interval over, whatever the answer.
    ///
    /// A provider that is slow or fails reads as no change: the stream
    /// carries on with the token it has and the next check asks again.
    pub(crate) async fn newer_token(&mut self) -> Option<String> {
        self.checked = Instant::now();
        // Only a provider that bounds its own fetch is cut off here: dropping
        // another's call mid-way could lose a refresh token it was rotating.
        let fetch = self.credentials.current_token();
        let fresh = if self.credentials.bounds_its_own_fetch() {
            tokio::time::timeout(FETCH_WAIT, fetch).await.ok()
        } else {
            Some(fetch.await)
        };
        let fresh = match fresh {
            Some(Ok(fresh)) => fresh,
            other => {
                if let Some(Err(err)) = other {
                    debug!(error = %err, "token renewal check could not get a token");
                }
                // A provider that is down is not asked again at once, so its
                // wait is not paid on every interval.
                self.checked = Instant::now() + FAILED_CHECK_BACKOFF;
                return None;
            }
        };
        (fresh != self.token).then_some(fresh)
    }

    /// Authenticate a new stream on the same connection with `fresh`, from
    /// [`Renewal::newer_token`], and finish the old one. `true` means `send`
    /// and `recv` are the new stream's: the caller must have nothing in
    /// flight on the old one and must forget any per-stream state, such as a
    /// declared publish width.
    ///
    /// Never fails the stream. If the new stream cannot be opened or is
    /// refused, the old one stays in use and the next check tries again.
    pub(crate) async fn move_to(
        &mut self,
        fresh: String,
        send: &mut SendStream,
        recv: &mut RecvStream,
    ) -> bool {
        // Without waiting for credit: a connection with none to spare would
        // hold this stream's work behind a stream that cannot open.
        let Probe::Opened(pair) = try_open_bi(&self.connection) else {
            warn!("no stream credit to move a stream onto the renewed token; retrying");
            return false;
        };
        match self
            .credentials
            .open(&self.connection, pair, fresh, self.max_frame_bytes)
            .await
        {
            Ok((new_send, new_recv, _, token)) => {
                let _ = send.finish();
                *send = new_send;
                let mut old = std::mem::replace(recv, new_recv);
                // Wait for the broker to end the old stream, so a write sent
                // on it without an ack is applied before anything on the new
                // one, and an error it sent is not left unread.
                let _ = tokio::time::timeout(DRAIN_WAIT, async {
                    let mut scratch = [0u8; 4096];
                    while let Ok(Some(_)) = old.read(&mut scratch).await {}
                })
                .await;
                self.token = token;
                debug!("stream moved onto the renewed token");
                true
            }
            Err(err) => {
                warn!(error = %err, "renewed token was not accepted on a new stream; keeping the old one");
                false
            }
        }
    }
}

#[cfg(test)]
mod tests;
