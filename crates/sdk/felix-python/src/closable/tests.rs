use std::sync::Arc;
use std::time::Duration;

use super::Closable;

async fn read_idle(closable: Arc<Closable<u32>>) -> bool {
    let Some(mut locked) = closable.lock().await else {
        return false;
    };
    let finished = closable
        .until_closed(std::future::pending::<()>())
        .await
        .is_some();
    if !finished {
        locked.end();
    }
    finished
}

#[tokio::test(flavor = "multi_thread")]
async fn close_returns_while_a_reader_is_parked_and_the_reader_stops() {
    let closable = Arc::new(Closable::new(7));
    let reader = tokio::spawn(read_idle(Arc::clone(&closable)));
    tokio::time::sleep(Duration::from_millis(50)).await;

    let closer = Arc::clone(&closable);
    tokio::time::timeout(Duration::from_secs(2), async move { closer.close() })
        .await
        .expect("close waited on the reader");
    assert!(closable.is_closed());
    let finished = tokio::time::timeout(Duration::from_secs(2), reader)
        .await
        .expect("reader still parked")
        .unwrap();
    assert!(!finished);
    assert!(closable.lock().await.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_reader_behind_the_first_stops_on_close() {
    let closable = Arc::new(Closable::new(7));
    let first = tokio::spawn(read_idle(Arc::clone(&closable)));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let second = tokio::spawn(read_idle(Arc::clone(&closable)));
    tokio::time::sleep(Duration::from_millis(50)).await;

    closable.close();
    for reader in [first, second] {
        tokio::time::timeout(Duration::from_secs(2), reader)
            .await
            .expect("reader still parked")
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn closing_while_a_reader_holds_the_value_drops_it_on_release() {
    let value = Arc::new(());
    let closable = Closable::new(Arc::clone(&value));
    let held = closable.lock().await.unwrap();
    closable.close();
    assert_eq!(Arc::strong_count(&value), 2);
    drop(held);
    assert_eq!(Arc::strong_count(&value), 1);
}
