//! Every shard of a stream, read as one thing.
//!
//! A subscription reads **one** shard: a stream's shards can have different
//! owners, and a subscription is bound to one connection to one broker. So
//! consuming a whole multi-shard stream means one subscription per shard,
//! following each shard's own redirect, and merging what comes back.
//!
//! This does that. What it deliberately does not do is pretend the result is a
//! single ordered stream — see [`ShardedSubscription`] for what is and is not
//! promised.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use felix_wire::StartPosition;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::ShardOffsets;
use crate::client::Client;
use crate::cluster::{ClusterClient, resume_position};
use crate::subscribe::{Event, ShardMoved, Subscription};

/// How much to wait between attempts to re-establish one shard.
const RECONNECT_BACKOFF: Duration = Duration::from_millis(250);

/// Every shard of one stream, merged into a single channel.
///
/// **Ordering is per shard, and nothing more.** Two records from the same shard
/// arrive in the order they were written. Two records from different shards
/// arrive in an arbitrary order, and no amount of merging can restore an order
/// that never existed — Felix orders per key, and a key always resolves to one
/// shard. Do not infer stream-wide ordering from the fact that these arrive on
/// one channel.
///
/// **Resumption is a vector.** [`ShardedSubscription::positions`] hands back one
/// offset per shard; pass it to [`ClusterClient::subscribe_sharded`] to resume.
///
/// Dropping this stops every shard's task and closes their connections.
pub struct ShardedSubscription {
    events: mpsc::Receiver<ShardEvent>,
    shards: u32,
    /// The highest offset delivered from each shard, updated as records are
    /// handed out rather than as they arrive — so a position is never ahead of
    /// what the caller has actually seen.
    positions: ShardOffsets,
    stop: Arc<AtomicBool>,
    tasks: Vec<JoinHandle<()>>,
}

impl ShardedSubscription {
    /// How many shards this subscription covers.
    pub fn shards(&self) -> u32 {
        self.shards
    }

    /// Where each shard has reached, for resuming later.
    ///
    /// Only shards that have delivered something appear. Resuming with this map
    /// starts each listed shard at `offset + 1` and leaves the rest where the
    /// resuming call asks them to start.
    pub fn positions(&self) -> ShardOffsets {
        self.positions.clone()
    }

    /// The next item from any shard.
    ///
    /// `None` means every shard's task has ended, which happens when this is
    /// dropped or the client is shutting down.
    pub async fn next(&mut self) -> Option<ShardEvent> {
        let item = self.events.recv().await?;
        if let ShardEvent::Record { shard, event } = &item
            && let Some(offset) = event.offset
        {
            // `max` rather than assignment: a shard that reconnects and replays
            // must not walk its own position backwards.
            let at = self.positions.entry(*shard).or_insert(offset);
            *at = (*at).max(offset);
        }
        Some(item)
    }
}

impl Drop for ShardedSubscription {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// What arrives from a sharded subscription.
///
/// Losing a shard is an **item**, not an error and not silence. A consumer that
/// ignores these variants is choosing to read an incomplete stream, which is
/// different from doing it by accident.
#[non_exhaustive]
pub enum ShardEvent {
    /// A record, and the shard it was read from.
    Record { shard: u32, event: Event },
    /// This shard's owner stopped answering. Every other shard is unaffected
    /// and still delivering; this one is being re-established.
    ShardLost { shard: u32, error: String },
    /// This shard is delivering again, resuming after the last offset seen from
    /// it. Records written to it while it was lost are read now, so nothing is
    /// skipped — but they arrive later than records written to other shards at
    /// the same time, which was already true of any two shards.
    ShardRecovered { shard: u32 },
    /// This shard moved to another broker, and is being followed there. The
    /// records that follow resume where the old owner left off; if the new
    /// owner cannot be reached, a [`ShardEvent::ShardLost`] comes next.
    ShardMoved { shard: u32, moved: ShardMoved },
}

impl std::fmt::Debug for ShardEvent {
    /// Deliberately does not print a record's payload: these end up in logs,
    /// and a payload is the tenant's data.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Record { shard, event } => f
                .debug_struct("Record")
                .field("shard", shard)
                .field("offset", &event.offset)
                .field("bytes", &event.payload.len())
                .finish(),
            Self::ShardLost { shard, error } => f
                .debug_struct("ShardLost")
                .field("shard", shard)
                .field("error", error)
                .finish(),
            Self::ShardRecovered { shard } => f
                .debug_struct("ShardRecovered")
                .field("shard", shard)
                .finish(),
            Self::ShardMoved { shard, moved } => f
                .debug_struct("ShardMoved")
                .field("shard", shard)
                .field("moved", moved)
                .finish(),
        }
    }
}

/// Open one subscription per shard and forward all of them into one channel.
///
/// Every shard is opened before this returns. If any shard cannot be reached,
/// the whole call fails naming that shard: a sharded subscription that quietly
/// covered three shards of four would be the worst of the available answers,
/// because nothing downstream could tell.
pub(crate) async fn subscribe_sharded(
    cluster: &Arc<ClusterClient>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shards: u32,
    start: Option<StartPosition>,
    resume: Option<ShardOffsets>,
) -> Result<ShardedSubscription> {
    anyhow::ensure!(
        shards > 0,
        "a stream with no shards cannot be subscribed to"
    );

    // Opened concurrently: a four-shard stream on a cluster with a slow broker
    // should cost one round trip, not four in a row.
    let mut opening = Vec::with_capacity(shards as usize);
    for shard in 0..shards {
        let cluster = Arc::clone(cluster);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let at = shard_start(shard, start, resume.as_ref());
        opening.push(tokio::spawn(async move {
            let opened = cluster
                .open_shard(&tenant_id, &namespace, &stream, shard, at)
                .await;
            (shard, opened)
        }));
    }

    let mut opened: Vec<(u32, Arc<Client>, Subscription)> = Vec::with_capacity(shards as usize);
    let mut failures: Vec<String> = Vec::new();
    for task in opening {
        match task.await {
            Ok((shard, Ok((client, subscription)))) => opened.push((shard, client, subscription)),
            Ok((shard, Err(err))) => failures.push(format!("shard {shard}: {err:#}")),
            Err(err) => failures.push(format!("a shard's open task failed: {err}")),
        }
    }
    if !failures.is_empty() {
        // The successfully opened subscriptions drop here, closing their
        // connections. Nothing was delivered from them, so there is nothing to
        // account for.
        anyhow::bail!(
            "could not subscribe to every shard of {stream}, so the subscription would be \
             silently incomplete: {}",
            failures.join("; ")
        );
    }

    // Bounded, and sized per shard: an application that stops calling `next`
    // should apply backpressure to the forwarding tasks rather than let this
    // grow without limit.
    let (tx, rx) = mpsc::channel(64 * shards as usize);
    let stop = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::with_capacity(opened.len());
    for (shard, client, subscription) in opened {
        tasks.push(tokio::spawn(forward_shard(
            Arc::clone(cluster),
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            shard,
            client,
            subscription,
            tx.clone(),
            Arc::clone(&stop),
        )));
    }

    Ok(ShardedSubscription {
        events: rx,
        shards,
        positions: resume.unwrap_or_default(),
        stop,
        tasks,
    })
}

/// Where one shard should begin: after the last offset seen from it if we are
/// resuming, otherwise wherever the caller asked every shard to start.
fn shard_start(
    shard: u32,
    start: Option<StartPosition>,
    resume: Option<&ShardOffsets>,
) -> Option<StartPosition> {
    match resume.and_then(|offsets| offsets.get(&shard)) {
        Some(offset) => Some(StartPosition::Offset(offset.saturating_add(1))),
        None => start,
    }
}

/// Pump one shard into the shared channel, re-establishing it on its own.
///
/// A shard failing is reported and retried here rather than propagated, which
/// is the requirement that matters: losing one shard's owner must not tear down
/// the other three.
#[allow(clippy::too_many_arguments)]
async fn forward_shard(
    cluster: Arc<ClusterClient>,
    tenant_id: String,
    namespace: String,
    stream: String,
    shard: u32,
    client: Arc<Client>,
    subscription: Subscription,
    tx: mpsc::Sender<ShardEvent>,
    stop: Arc<AtomicBool>,
) {
    // The client and the subscription are held together because they have to
    // live together: dropping the client closes the connection its events
    // arrive on. Replacing the pair on reconnect drops the old connection,
    // which is the point.
    let mut feed = (client, subscription);
    // The last offset this task actually forwarded, so a reconnect resumes
    // after it rather than at whatever the original call asked for.
    let mut last_offset: Option<u64> = None;
    // Where the old owner said to resume, when following a move failed and the
    // reconnect loop below has to do it instead.
    let mut moved_resume_from: Option<u64> = None;

    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let error = match feed.1.next_event().await {
            Ok(Some(event)) => {
                if let Some(offset) = event.offset {
                    last_offset = Some(last_offset.map_or(offset, |seen| seen.max(offset)));
                }
                if tx.send(ShardEvent::Record { shard, event }).await.is_err() {
                    // The receiver is gone, so nothing is reading any shard.
                    return;
                }
                continue;
            }
            Ok(None) => match feed.1.shard_moved().cloned() {
                Some(moved) => {
                    let resume_from = moved.resume_from;
                    if tx
                        .send(ShardEvent::ShardMoved {
                            shard,
                            moved: moved.clone(),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                    match cluster
                        .follow_moved_shard(
                            &tenant_id,
                            &namespace,
                            &stream,
                            shard,
                            &moved,
                            last_offset,
                        )
                        .await
                    {
                        Ok(next) => {
                            feed = next;
                            continue;
                        }
                        Err(err) => {
                            moved_resume_from = resume_from;
                            format!("{err:#}")
                        }
                    }
                }
                // A clean end of stream is still the loss of this shard's feed:
                // the broker closed it, and the other shards are still going.
                None => "the shard's event stream ended".to_string(),
            },
            Err(err) => {
                if let Some(lagged) = err.downcast_ref::<crate::SubscriptionLagged>() {
                    moved_resume_from = Some(lagged.resume_from);
                }
                format!("{err:#}")
            }
        };

        if stop.load(Ordering::Relaxed) {
            return;
        }
        if tx
            .send(ShardEvent::ShardLost { shard, error })
            .await
            .is_err()
        {
            return;
        }

        // Re-establish, indefinitely. Giving up would turn a recoverable
        // failure into permanent silence on this shard, and the application has
        // already been told it is down.
        loop {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            tokio::time::sleep(RECONNECT_BACKOFF).await;
            // Resume after what was forwarded, or where a moved shard's old
            // owner said. `Latest` only with neither — there is no offset to
            // resume from, and replaying from the start would duplicate a
            // history the caller never asked for.
            let at = Some(resume_position(last_offset, moved_resume_from));
            if let Ok(next) = cluster
                .subscribe_shard_following_redirects(&tenant_id, &namespace, &stream, shard, at)
                .await
            {
                feed = next;
                moved_resume_from = None;
                if tx.send(ShardEvent::ShardRecovered { shard }).await.is_err() {
                    return;
                }
                break;
            }
        }
    }
}
