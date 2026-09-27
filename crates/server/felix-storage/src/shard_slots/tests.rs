use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

fn closed() -> StorageError {
    StorageError::Closed("k".into())
}

#[tokio::test]
async fn an_open_during_a_close_is_refused_and_a_later_one_starts_fresh() {
    let slots: Arc<ShardSlots<&'static str, usize>> = Arc::new(ShardSlots::new());
    let opens = AtomicUsize::new(0);
    let open = || Ok(opens.fetch_add(1, Ordering::SeqCst));
    assert_eq!(slots.get_or_open(&"k", open, closed).expect("open"), 0);

    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel::<()>();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let closing = tokio::spawn({
        let slots = Arc::clone(&slots);
        async move {
            slots
                .close(&"k", move |_| async move {
                    let _ = entered_tx.send(());
                    let _ = release_rx.await;
                    Ok(())
                })
                .await
        }
    });
    entered_rx.await.expect("close started");

    // Mid-close: opening a second writer over the same files is exactly what
    // this must refuse.
    assert!(matches!(
        slots.get_or_open(&"k", open, closed),
        Err(StorageError::Closed(_))
    ));
    assert!(slots.open_values().is_empty());

    release_tx.send(()).expect("release");
    closing.await.expect("join").expect("close");
    assert_eq!(slots.get_or_open(&"k", open, closed).expect("reopen"), 1);
    assert_eq!(slots.open_values(), vec![1]);
}

#[tokio::test]
async fn a_close_finishes_even_if_its_caller_gives_up() {
    let slots: Arc<ShardSlots<&'static str, usize>> = Arc::new(ShardSlots::new());
    slots.get_or_open(&"k", || Ok(7), closed).expect("open");

    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<usize>();
    let abandoned = tokio::time::timeout(
        std::time::Duration::from_millis(20),
        slots.close(&"k", move |value| async move {
            let _ = release_rx.await;
            let _ = done_tx.send(value);
            Ok(())
        }),
    )
    .await;
    assert!(abandoned.is_err(), "the close was still waiting");

    release_tx.send(()).expect("release");
    assert_eq!(done_rx.await.expect("the close ran to the end"), 7);
    // And let the slot go, rather than leaving the shard stuck closing.
    tokio::task::yield_now().await;
    let reopened = loop {
        match slots.get_or_open(&"k", || Ok(8), closed) {
            Ok(value) => break value,
            Err(_) => tokio::task::yield_now().await,
        }
    };
    assert_eq!(reopened, 8);
}

#[tokio::test]
async fn a_failed_open_leaves_the_shard_openable() {
    let slots: ShardSlots<&'static str, usize> = ShardSlots::new();
    assert!(
        slots
            .get_or_open(&"k", || Err(StorageError::NotFound), closed)
            .is_err()
    );
    assert!(slots.open_values().is_empty());
    assert_eq!(slots.get_or_open(&"k", || Ok(3), closed).expect("open"), 3);
}
