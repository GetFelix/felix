//! What `recv` reports, and what a caller is entitled to conclude from it.
//!
//! `None` is the end of the subscription. Every caller treats it that way — the
//! broker's subscribe handler, the client, and the benchmarks all stop when
//! they see it. So `None` must mean the channel closed and nothing else.

use std::sync::Arc;

use super::*;
use crate::stream::DeliveryEnvelope;

fn payload(value: &str) -> Bytes {
    Bytes::copy_from_slice(value.as_bytes())
}

/// A subscription resuming at `skip_below`, with a sender to feed it.
fn resuming_at(skip_below: u64) -> (mpsc::Sender<QueuedDelivery>, Subscription) {
    let (tx, rx) = mpsc::channel(16);
    let subscription = Subscription {
        receiver: SubscriptionReceiver::new(
            rx,
            Default::default(),
            Default::default(),
            Default::default(),
        ),
        // No stream to unregister from; the guard's `Weak` simply never
        // upgrades, which is the same thing it does after a stream is dropped.
        guard: SubscriptionGuard {
            stream_state: Weak::new(),
            subscriber_id: 0,
        },
        pending: VecDeque::new(),
        skip_below: Some(skip_below),
    };
    (tx, subscription)
}

fn deliver(tx: &mpsc::Sender<QueuedDelivery>, base_offset: u64, values: &[&str]) {
    let payloads: Vec<Bytes> = values.iter().map(|v| payload(v)).collect();
    let envelope = DeliveryEnvelope::with_base_offset(&payloads, Some(base_offset));
    tx.try_send(QueuedDelivery::new(
        envelope,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    ))
    .expect("queue the delivery");
}

/// **A batch entirely below the resume point is skipped, not an end of stream.**
///
/// A publish claims its disk offsets before the record reaches the replay ring,
/// so a cursor taken from the durable tail can name an offset the ring has not
/// seen, and the records in between arrive live and below the cursor. Dropping
/// them is correct — the caller already has them. Reporting `None` is not: every
/// caller reads that as "the broker closed my subscription" and stops, losing
/// every record after the cursor.
#[tokio::test]
async fn a_batch_wholly_below_the_resume_point_is_skipped_rather_than_ending_the_stream() {
    let (tx, mut subscription) = resuming_at(105);

    // Offsets 100-102: all below the resume point, so nothing to deliver.
    deliver(&tx, 100, &["a", "b", "c"]);
    // And then the record the caller is actually waiting for.
    deliver(&tx, 105, &["f"]);

    assert_eq!(
        subscription.recv().await,
        Some(payload("f")),
        "a skipped batch was reported as the end of the subscription",
    );
}

/// Several skipped batches in a row are still not an end of stream.
#[tokio::test]
async fn consecutive_skipped_batches_do_not_end_the_stream() {
    let (tx, mut subscription) = resuming_at(105);

    deliver(&tx, 100, &["a"]);
    deliver(&tx, 101, &["b"]);
    deliver(&tx, 102, &["c"]);
    deliver(&tx, 105, &["f"]);

    assert_eq!(subscription.recv().await, Some(payload("f")));
}

/// A batch straddling the resume point yields only the part at or above it.
#[tokio::test]
async fn a_batch_straddling_the_resume_point_yields_only_its_tail() {
    let (tx, mut subscription) = resuming_at(102);

    deliver(&tx, 100, &["a", "b", "c", "d"]);

    assert_eq!(subscription.recv().await, Some(payload("c")));
    assert_eq!(subscription.recv().await, Some(payload("d")));
}

/// Once past the resume point the filter retires, so later batches are
/// delivered whole.
#[tokio::test]
async fn the_filter_retires_once_the_resume_point_is_reached() {
    let (tx, mut subscription) = resuming_at(100);

    deliver(&tx, 100, &["a"]);
    deliver(&tx, 101, &["b"]);

    assert_eq!(subscription.recv().await, Some(payload("a")));
    assert_eq!(subscription.recv().await, Some(payload("b")));
}

/// **`None` still means the channel closed.** The fix must not turn an ended
/// subscription into a hang.
#[tokio::test]
async fn a_closed_channel_still_reports_the_end_of_the_stream() {
    let (tx, mut subscription) = resuming_at(105);
    deliver(&tx, 100, &["a"]);
    drop(tx);

    assert_eq!(
        subscription.recv().await,
        None,
        "a closed channel must still end the subscription",
    );
}

/// `try_recv` has the same trap: a skipped batch is not "nothing queued".
#[tokio::test]
async fn try_recv_sees_past_a_skipped_batch() {
    let (tx, mut subscription) = resuming_at(105);
    deliver(&tx, 100, &["a", "b"]);
    deliver(&tx, 105, &["f"]);

    assert_eq!(
        subscription
            .try_recv()
            .expect("the record above the cursor"),
        payload("f"),
    );
}

/// And `try_recv` still reports an empty queue as empty rather than blocking or
/// inventing a record.
#[tokio::test]
async fn try_recv_reports_an_empty_queue_as_empty() {
    let (tx, mut subscription) = resuming_at(105);
    deliver(&tx, 100, &["a"]);

    assert!(matches!(
        subscription.try_recv(),
        Err(mpsc::error::TryRecvError::Empty),
    ));
}

/// **Split into parts, the receiver still honours the resume point.**
///
/// The broker's subscribe handler delivers live records from the bare
/// receiver. When the resume point was dropped with the `Subscription`, a
/// `Latest` subscriber could be handed an in-flight record below the
/// `start_offset` it had just been told.
#[tokio::test]
async fn the_split_receiver_skips_below_the_resume_point() {
    let (tx, subscription) = resuming_at(105);
    let (mut receiver, _guard) = subscription.into_parts();

    deliver(&tx, 100, &["a", "b", "c"]);
    deliver(&tx, 103, &["d", "e", "f", "g"]);
    deliver(&tx, 107, &["h"]);

    let straddling = receiver.recv().await.expect("a batch");
    assert_eq!(straddling.base_offset(), Some(105));
    assert_eq!(straddling.payloads(), &[payload("f"), payload("g")]);
    let next = receiver.try_recv().expect("the next batch");
    assert_eq!(next.base_offset(), Some(107));
}

/// A receiver already waiting on an empty queue when the drop is recorded
/// still ends: a dropped batch means no further delivery will wake it.
#[tokio::test]
async fn a_receiver_waiting_when_the_queue_drops_ends() {
    let (_tx, rx) = mpsc::channel(1);
    let lag = Arc::new(Lag::default());
    let mut receiver =
        SubscriptionReceiver::new(rx, Default::default(), Arc::clone(&lag), Default::default());
    receiver.end_on_lag();
    let waiting = tokio::spawn(async move { (receiver.recv().await.is_none(), receiver.lagged()) });
    tokio::task::yield_now().await;
    lag.dropped(3);
    let (ended, lagged) = tokio::time::timeout(std::time::Duration::from_secs(2), waiting)
        .await
        .expect("woken by the drop")
        .expect("join");
    assert!(ended);
    assert_eq!(lagged, Some(3));
}

/// Drops a resume already filled from the log do not end the subscription,
/// but one after them still does, from no later than where the fill stopped.
#[test]
fn drops_below_a_covered_offset_are_forgotten() {
    let lag = Lag::default();
    lag.dropped(4);
    lag.covered_below(10);
    assert_eq!(lag.first_dropped(), None);
    lag.dropped(12);
    assert_eq!(lag.first_dropped(), Some(10));

    let lag = Lag::default();
    lag.covered_below(10);
    lag.dropped(15);
    assert_eq!(lag.first_dropped(), Some(15));
}
