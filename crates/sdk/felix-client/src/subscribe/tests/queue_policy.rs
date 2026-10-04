use tokio::sync::mpsc;

use crate::config::ClientSubQueuePolicy;
use crate::subscribe::queue::{Enqueued, enqueue_with_policy};

const ENQUEUED: &str = "test_enqueued";
const DROPPED: &str = "test_dropped";
const DROP_OLD: &str = "test_drop_old_emulated";

async fn enqueue(
    tx: &mpsc::Sender<u64>,
    item: u64,
    policy: ClientSubQueuePolicy,
    capacity: usize,
) -> bool {
    enqueue_with_policy(tx, item, policy, capacity, ENQUEUED, DROPPED, DROP_OLD).await
        != Enqueued::Closed
}

/// Room in the queue: every policy takes the item and reports success.
#[tokio::test]
async fn an_item_that_fits_is_enqueued_under_every_policy() {
    for policy in [
        ClientSubQueuePolicy::Block,
        ClientSubQueuePolicy::DropNew,
        ClientSubQueuePolicy::DropOld,
    ] {
        let (tx, mut rx) = mpsc::channel(2);

        assert!(enqueue(&tx, 1, policy, 2).await, "{policy:?}");
        assert_eq!(rx.recv().await, Some(1), "{policy:?}");
    }
}

/// **A full queue is not a closed one.** Dropping is the point of these
/// policies, so the subscription carries on — returning `false` here would end
/// the read loop over a single slow moment in the application.
#[tokio::test]
async fn a_full_queue_drops_without_ending_the_subscription() {
    for policy in [ClientSubQueuePolicy::DropNew, ClientSubQueuePolicy::DropOld] {
        let (tx, _rx) = mpsc::channel(1);
        assert!(enqueue(&tx, 1, policy, 1).await);

        assert!(
            enqueue(&tx, 2, policy, 1).await,
            "{policy:?} ended the subscription instead of dropping",
        );
    }
}

/// `DropNew` keeps what is already queued, so the oldest events survive.
#[tokio::test]
async fn drop_new_keeps_what_is_already_queued() {
    let (tx, mut rx) = mpsc::channel(1);
    assert!(enqueue(&tx, 1, ClientSubQueuePolicy::DropNew, 1).await);
    assert!(enqueue(&tx, 2, ClientSubQueuePolicy::DropNew, 1).await);

    assert_eq!(rx.recv().await, Some(1));
    assert!(rx.try_recv().is_err(), "the dropped item was queued anyway");
}

/// **A closed receiver ends the loop.** The application has gone away, so
/// unlike a full queue there is nothing to carry on for.
#[tokio::test]
async fn a_closed_receiver_ends_the_subscription() {
    for policy in [
        ClientSubQueuePolicy::Block,
        ClientSubQueuePolicy::DropNew,
        ClientSubQueuePolicy::DropOld,
    ] {
        let (tx, rx) = mpsc::channel::<u64>(1);
        drop(rx);

        assert!(
            !enqueue(&tx, 1, policy, 1).await,
            "{policy:?} carried on writing to a receiver that had gone",
        );
    }
}

/// `Block` waits for room rather than dropping — the guarantee an application
/// chooses it for.
#[tokio::test]
async fn block_waits_for_room_rather_than_dropping() {
    let (tx, mut rx) = mpsc::channel(1);
    assert!(enqueue(&tx, 1, ClientSubQueuePolicy::Block, 1).await);

    let writer = tokio::spawn(async move {
        enqueue(&tx, 2, ClientSubQueuePolicy::Block, 1).await;
    });

    assert_eq!(rx.recv().await, Some(1));
    assert_eq!(rx.recv().await, Some(2), "the blocked item was dropped");
    writer.await.expect("the writer should finish");
}
