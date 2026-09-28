//! The per-subscriber event writer: batching, flushing and encodings.

use super::*;

#[tokio::test]
async fn run_event_writer_single_closes_on_channel_close() -> Result<()> {
    crate::observability::timings::enable_collection(1);
    crate::observability::timings::set_enabled(true);

    let (tx, rx) = mpsc::channel(4);
    let config = EventWriterConfig {
        offsets_enabled: false,
        skip_enabled: false,
        shard_moved_enabled: false,
        subscription_id: 1,
        max_events: 1,
        max_bytes: 1024,
        flush_delay: Duration::from_millis(10),
        single_event_mode: true,
        flush_max_items: 64,
        flush_max_delay: Duration::from_micros(200),
        max_bytes_per_write: 256 * 1024,
    };

    let (server_task, connection) = spawn_event_writer(rx, config).await?;
    let accept_uni = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni());
    tx.send(make_payload(b"hello")).await?;
    let mut event_recv = accept_uni.await.context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let frame = crate::serving::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("event frame");
    let batch = felix_wire::binary::decode_event_batch(&frame).context("decode batch")?;
    assert_eq!(batch.subscription_id, 1);
    assert_eq!(batch.payloads.len(), 1);
    assert_eq!(batch.payloads[0].as_ref(), b"hello");

    drop(tx);
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn run_event_writer_single_binary_uses_batch_encoding() -> Result<()> {
    crate::observability::timings::enable_collection(1);
    crate::observability::timings::set_enabled(true);

    let (tx, rx) = mpsc::channel(4);
    let config = EventWriterConfig {
        offsets_enabled: false,
        skip_enabled: false,
        shard_moved_enabled: false,
        subscription_id: 9,
        max_events: 1,
        max_bytes: 1024,
        flush_delay: Duration::from_millis(10),
        single_event_mode: true,
        flush_max_items: 64,
        flush_max_delay: Duration::from_micros(200),
        max_bytes_per_write: 256 * 1024,
    };

    let (server_task, connection) = spawn_event_writer(rx, config).await?;
    let accept_uni = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni());
    tx.send(make_payload(b"bin")).await?;
    let mut event_recv = accept_uni.await.context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let frame = crate::serving::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("event frame");
    let batch = felix_wire::binary::decode_event_batch(&frame).context("decode batch")?;
    assert_eq!(batch.subscription_id, 9);
    assert_eq!(batch.payloads.len(), 1);
    assert_eq!(batch.payloads[0].as_ref(), b"bin");

    drop(tx);
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn run_event_writer_batches_with_pending_payload() -> Result<()> {
    crate::observability::timings::enable_collection(1);
    crate::observability::timings::set_enabled(true);

    let (tx, rx) = mpsc::channel(4);
    let config = EventWriterConfig {
        offsets_enabled: false,
        skip_enabled: false,
        shard_moved_enabled: false,
        subscription_id: 7,
        max_events: 10,
        max_bytes: 5,
        flush_delay: Duration::from_millis(50),
        single_event_mode: false,
        flush_max_items: 64,
        flush_max_delay: Duration::from_micros(200),
        max_bytes_per_write: 256 * 1024,
    };

    let (server_task, connection) = spawn_event_writer(rx, config).await?;
    let accept_uni = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni());
    tx.send(make_payload(b"aaaa")).await?;
    tx.send(make_payload(b"bbb")).await?;
    tx.send(make_payload(b"c")).await?;
    let mut event_recv = accept_uni.await.context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let frame1 = crate::serving::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame1");
    let batch1 = felix_wire::binary::decode_event_batch(&frame1).context("decode batch1")?;
    assert_eq!(batch1.subscription_id, 7);
    assert_eq!(batch1.payloads.len(), 1);
    assert_eq!(batch1.payloads[0].as_ref(), b"aaaa");

    let frame2 = crate::serving::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame2");
    let batch2 = felix_wire::binary::decode_event_batch(&frame2).context("decode batch2")?;
    assert_eq!(batch2.subscription_id, 7);
    assert_eq!(batch2.payloads.len(), 2);
    assert_eq!(batch2.payloads[0].as_ref(), b"bbb");
    assert_eq!(batch2.payloads[1].as_ref(), b"c");

    drop(tx);
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn run_event_writer_flushes_by_count_and_deadline() -> Result<()> {
    let (tx, rx) = mpsc::channel(8);
    let config = EventWriterConfig {
        offsets_enabled: false,
        skip_enabled: false,
        shard_moved_enabled: false,
        subscription_id: 44,
        max_events: 2,
        max_bytes: 1024,
        flush_delay: Duration::from_millis(20),
        single_event_mode: false,
        flush_max_items: 64,
        flush_max_delay: Duration::from_micros(200),
        max_bytes_per_write: 256 * 1024,
    };
    let (server_task, connection) = spawn_event_writer(rx, config).await?;
    tx.send(make_payload(b"a")).await?;
    tx.send(make_payload(b"b")).await?;
    let mut event_recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let frame = crate::serving::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("count-based frame");
    let batch = felix_wire::binary::decode_event_batch(&frame).expect("decode count batch");
    assert_eq!(batch.payloads.len(), 2);
    assert_eq!(batch.payloads[0].as_ref(), b"a");
    assert_eq!(batch.payloads[1].as_ref(), b"b");

    tx.send(make_payload(b"deadline")).await?;
    let frame = crate::serving::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("deadline frame");
    let batch = felix_wire::binary::decode_event_batch(&frame).expect("decode deadline batch");
    assert_eq!(batch.payloads.len(), 1);
    assert_eq!(batch.payloads[0].as_ref(), b"deadline");

    drop(tx);
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn run_event_writer_flushes_on_channel_close() -> Result<()> {
    let (tx, rx) = mpsc::channel(4);
    let config = EventWriterConfig {
        offsets_enabled: false,
        skip_enabled: false,
        shard_moved_enabled: false,
        subscription_id: 55,
        max_events: 8,
        max_bytes: 1024,
        flush_delay: Duration::from_secs(5),
        single_event_mode: false,
        flush_max_items: 64,
        flush_max_delay: Duration::from_micros(200),
        max_bytes_per_write: 256 * 1024,
    };

    let (server_task, connection) = spawn_event_writer(rx, config).await?;
    tx.send(make_payload(b"closed")).await?;
    drop(tx);
    // Keep the client connection open until the writer has flushed: dropping it
    // first races CONNECTION_CLOSE against the flush and fails intermittently.
    server_task.await.context("server join")??;
    drop(connection);
    Ok(())
}

#[tokio::test]
async fn run_event_writer_single_event_mode_writes_multiple_frames() -> Result<()> {
    let (tx, rx) = mpsc::channel(4);
    let config = EventWriterConfig {
        offsets_enabled: false,
        skip_enabled: false,
        shard_moved_enabled: false,
        subscription_id: 66,
        max_events: 2,
        max_bytes: 1024,
        flush_delay: Duration::from_millis(10),
        single_event_mode: true,
        flush_max_items: 64,
        flush_max_delay: Duration::from_micros(200),
        max_bytes_per_write: 256 * 1024,
    };

    let (server_task, connection) = spawn_event_writer(rx, config).await?;
    let accept_uni = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni());
    tx.send(make_payload(b"one")).await?;
    tx.send(make_payload(b"two")).await?;

    let mut event_recv = accept_uni.await.context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let frame1 = crate::serving::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame1");
    let batch1 = felix_wire::binary::decode_event_batch(&frame1).expect("decode batch1");
    assert_eq!(batch1.payloads.len(), 1);
    assert_eq!(batch1.payloads[0].as_ref(), b"one");

    let frame2 = crate::serving::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame2");
    let batch2 = felix_wire::binary::decode_event_batch(&frame2).expect("decode batch2");
    assert_eq!(batch2.payloads.len(), 1);
    assert_eq!(batch2.payloads[0].as_ref(), b"two");

    drop(tx);
    server_task.await.context("server join")??;
    Ok(())
}
