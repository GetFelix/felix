//! Reading a replay to its end, and telling a replay that ended from one
//! that was cut short.
//!
//! A subscription from the start of a stream never finishes on its own: after
//! the history it turns live. A test therefore reads until nothing has come
//! for a while. What it must not do is read a subscription the broker ended
//! the same way. A broker that stops serving the shard part way through the
//! history resets the event stream, and the client is expected to resume from
//! its last offset. The records read up to then are a prefix, and asserting on
//! them reports acknowledged records as lost (#3112).
use std::future::Future;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use felix_client::Event;

/// Where a replay's events come from. [`felix_client::Subscription`] in a
/// test of a cluster.
pub trait Events {
    /// See [`felix_client::Subscription::next_event`].
    fn next_event(&mut self) -> impl Future<Output = Result<Option<Event>>> + Send;

    /// Whether the broker ended it because the shard moved: a deliberate end
    /// that asks the client to subscribe again, not a short replay.
    fn moved(&self) -> bool {
        false
    }
}

impl Events for felix_client::Subscription {
    fn next_event(&mut self) -> impl Future<Output = Result<Option<Event>>> + Send {
        felix_client::Subscription::next_event(self)
    }

    fn moved(&self) -> bool {
        self.shard_moved().is_some()
    }
}

/// Every event `events` yields until none has come for `quiet`.
///
/// An error, with nothing returned, when the subscription is lost or ended
/// first: what was read is then a prefix of what the broker holds, not a
/// replay. The caller subscribes again, as an application would.
pub async fn drain<E: Events>(events: &mut E, quiet: Duration) -> Result<Vec<Event>> {
    let mut read = Vec::new();
    loop {
        match tokio::time::timeout(quiet, events.next_event()).await {
            Err(_) => return Ok(read),
            Ok(Ok(Some(event))) => read.push(event),
            Ok(Ok(None)) if events.moved() => bail!(
                "the shard moved after {} events; subscribe again for the whole replay",
                read.len()
            ),
            Ok(Ok(None)) => return Err(ReplayEnded { events: read.len() }.into()),
            Ok(Err(err)) => {
                return Err(err).with_context(|| {
                    format!(
                        "the subscription was lost after {} events; the replay is not complete",
                        read.len()
                    )
                });
            }
        }
    }
}

/// The broker ended the subscription cleanly before the replay went quiet.
///
/// Not retried by a caller: a reset or a refusal is the broker's signal to
/// subscribe again, while a clean end part way through the history is a
/// broker answering a short replay as if it were whole.
#[derive(Debug)]
pub struct ReplayEnded {
    /// Events read before the end.
    pub events: usize,
}

impl std::fmt::Display for ReplayEnded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the broker ended the subscription after {} events; the replay is not complete",
            self.events
        )
    }
}

impl std::error::Error for ReplayEnded {}

/// The payloads of `events`, as text.
pub fn payloads(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .map(|event| String::from_utf8_lossy(&event.payload).into_owned())
        .collect()
}

#[cfg(test)]
mod tests;
