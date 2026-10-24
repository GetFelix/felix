use super::*;

/// A producer whose task only records what it is handed, in order.
fn recording(ids: Arc<std::sync::Mutex<Vec<u8>>>) -> IdempotentProducer {
    let (requests, mut queued) = mpsc::channel::<Request>(QUEUED);
    let driver = tokio::spawn(async move {
        while let Some(request) = queued.recv().await {
            let tag = request.batches[0][0][0];
            ids.lock().expect("ids").push(tag);
            let _ = request.answer.send(Ok(vec![Some(u64::from(tag))]));
        }
    });
    IdempotentProducer {
        producer_id: 1,
        numbered_by: Arc::new(std::sync::Mutex::new(hashbrown::HashMap::with_hasher(
            ahash::RandomState::new(),
        ))),
        requests,
        driver,
    }
}

#[tokio::test]
async fn calls_handed_off_in_turn_reach_the_task_in_turn() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let producer = recording(Arc::clone(&seen));
    // More than the queue holds, with no answer awaited between them.
    let mut handed = Vec::new();
    for tag in 0..100u8 {
        handed.push(
            producer
                .hand_off_batch("t", "n", "s", None, vec![vec![tag]])
                .await
                .expect("hand off"),
        );
    }
    for (tag, one) in handed.into_iter().enumerate() {
        assert_eq!(one.offset().await.expect("answer"), Some(tag as u64));
    }
    assert_eq!(*seen.lock().expect("ids"), (0..100u8).collect::<Vec<_>>());
}
