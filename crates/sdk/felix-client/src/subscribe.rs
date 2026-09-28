//! Subscriptions: [`Subscription`] and [`Event`], and the pipeline that
//! feeds them.
//!
//! A subscribe request goes out on a short-lived bi-directional stream. The
//! broker then opens a uni stream for the events, which the connection's
//! event router hands to the waiting subscription (see
//! `crate::connection`). Each subscription reads its uni stream with one I/O
//! task and decodes with one dispatch task, joined by bounded queues whose
//! overflow behaviour is the configured [`crate::ClientSubQueuePolicy`].

mod pipeline;
mod queue;

pub(crate) use pipeline::SubscriptionPipelineConfig;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
#[cfg(feature = "telemetry")]
use std::time::Instant;

use anyhow::Result;
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::connection::StreamLease;
use crate::telemetry::record_e2e_latency;
#[cfg(feature = "telemetry")]
use crate::timings;

/// Events from one shard of one stream.
///
/// Built by [`crate::Client::subscribe`] and its variants; read with
/// [`Subscription::next_event`].
pub struct Subscription {
    event_rx: mpsc::Receiver<QueuedEvent>,
    #[cfg(feature = "telemetry")]
    pub(crate) last_poll: Option<Instant>,
    pub(crate) tenant_id: Arc<str>,
    pub(crate) namespace: Arc<str>,
    pub(crate) stream: Arc<str>,
    pub(crate) event_conn_index: usize,
    _lease: StreamLease,
    pub(crate) event_conn_counts: Arc<Vec<AtomicUsize>>,
    #[cfg(feature = "telemetry")]
    bench_embed_ts: bool,
    start_offset: Option<u64>,
    live_offset: Option<u64>,
    /// Filled by the dispatch task before it closes the event queue.
    shard_moved: Arc<OnceLock<ShardMoved>>,
}

impl Subscription {
    pub(crate) fn with_join(mut self, start_offset: Option<u64>, live_offset: Option<u64>) -> Self {
        self.start_offset = start_offset;
        self.live_offset = live_offset;
        self
    }

    /// The first offset this subscription delivers.
    ///
    /// `None` for a plain tail subscribe, an in-memory stream, or an older
    /// broker.
    pub fn start_offset(&self) -> Option<u64> {
        self.start_offset
    }

    /// The stream's tail when this subscription was registered.
    ///
    /// An event below it was already in the stream; one at or past it was
    /// written after, and none are skipped in between. From `Latest` this
    /// equals [`Self::start_offset`].
    pub fn live_offset(&self) -> Option<u64> {
        self.live_offset
    }

    /// Why the subscription ended, when it ended because its shard moved to
    /// another broker.
    ///
    /// Set before [`Self::next_event`] returns `None`, so check it then:
    /// `Some` means resubscribe where it says, `None` means the stream simply
    /// closed. [`crate::ClusterClient::subscribe`] does this on its own.
    pub fn shard_moved(&self) -> Option<&ShardMoved> {
        self.shard_moved.get()
    }

    /// The next event, or `None` once the broker has closed the event stream.
    ///
    /// Losing the connection is an error, [`crate::SubscriptionLost`], not
    /// `None`. An error is the last thing a subscription yields: the pipeline
    /// stops after reporting it.
    pub async fn next_event(&mut self) -> Result<Option<Event>> {
        #[cfg(feature = "telemetry")]
        {
            let now = Instant::now();
            if let Some(last) = self.last_poll {
                let gap_ns = now.duration_since(last).as_nanos() as u64;
                t_histogram!("client_sub_consumer_gap_ns").record(gap_ns as f64);
                timings::record_sub_consumer_gap_ns(gap_ns);
            }
            self.last_poll = Some(now);
        }

        let Some(queued) = self.event_rx.recv().await else {
            return Ok(None);
        };
        match queued {
            QueuedEvent::Payload(payload, offset, skipped_before) => {
                record_e2e_latency(
                    &payload,
                    #[cfg(feature = "telemetry")]
                    self.bench_embed_ts,
                );
                Ok(Some(Event {
                    tenant_id: Arc::clone(&self.tenant_id),
                    namespace: Arc::clone(&self.namespace),
                    stream: Arc::clone(&self.stream),
                    payload,
                    offset,
                    skipped_before,
                }))
            }
            QueuedEvent::Error(err) => Err(err),
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // Update connection-level subscription counts for metrics.
        let counter = &self.event_conn_counts[self.event_conn_index];
        let mut current = counter.load(Ordering::Relaxed);
        while current > 0 {
            match counter.compare_exchange(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    t_gauge!(
                        "felix_client_event_conn_subscriptions",
                        "conn" => self.event_conn_index.to_string()
                    )
                    .set((current - 1) as f64);
                    break;
                }
                Err(next) => current = next,
            }
        }
    }
}

/// One record delivered to a [`Subscription`].
pub struct Event {
    /// The tenant of the stream the event was read from.
    pub tenant_id: Arc<str>,
    /// The namespace of the stream the event was read from.
    pub namespace: Arc<str>,
    /// The stream the event was read from.
    pub stream: Arc<str>,
    /// The record as it was published.
    pub payload: Bytes,
    /// Log offset of this event on a durable stream, or `None` for an in-memory
    /// one and for any broker that did not negotiate offsets.
    ///
    /// Two uses. Record it to resume from `offset + 1` after a reconnect. And
    /// to see drops: a durable stream's log also holds records that are not
    /// events (a promoted leader's generation-start record), so a jump between
    /// consecutive events is a drop exactly when it is larger than
    /// [`Event::skipped_before`] explains --
    /// `offset - previous - 1 - skipped_before` events were dropped. A broker
    /// that predates the skip count reports none, and a generation start then
    /// reads as a drop of one.
    pub offset: Option<u64>,
    /// How many offsets immediately before [`Event::offset`] hold no event.
    ///
    /// Zero almost always. Non-zero on the first event after a leader change,
    /// whose generation-start record took an offset. Only ever describes the
    /// offsets just before this event, so if the event carrying it was itself
    /// dropped, the next jump counts those offsets as dropped too: a drop is
    /// still reported, only its size is overstated.
    pub skipped_before: u64,
}

/// Where a subscription's shard went, sent by the broker as the last frame
/// before it ended the subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMoved {
    /// The first offset the old owner did not hand to this subscription.
    /// Resuming at the larger of this and the last delivered offset plus one
    /// neither repeats nor skips a record. `None` for an in-memory stream, and
    /// for a cache watch whose shard was still taking writes when it ended.
    pub resume_from: Option<u64>,
    /// The broker taking the shard, when the old owner knew. A hint only: the
    /// shard may have moved again, and that broker then redirects.
    pub node_id: Option<String>,
    /// That broker's client address, when the cluster publishes one.
    pub addr: Option<String>,
    /// The assignment generation that moved the shard.
    pub generation: u64,
}

enum QueuedEvent {
    /// A payload, for a durable stream the log offset it sits at, and how
    /// many offsets just before it hold no event.
    Payload(Bytes, Option<u64>, u64),
    Error(anyhow::Error),
}

#[cfg(test)]
mod tests;
