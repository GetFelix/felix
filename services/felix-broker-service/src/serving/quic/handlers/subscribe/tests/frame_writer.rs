//! Writing encoded parts to a stream: order, byte counts and parity with the plain encoder.

use super::*;

#[tokio::test]
async fn write_parts_preserves_order_and_bytes() -> Result<()> {
    let (mut tx, mut rx) = tokio::io::duplex(1024);
    let payloads = vec![Bytes::from_static(b"abc"), Bytes::from_static(b"defg")];
    let expected = felix_wire::binary::encode_event_batch_bytes(88, &payloads)?;
    let parts = felix_wire::binary::encode_event_batch_parts(88, &payloads)?;

    let write_task = tokio::spawn(async move { write_parts_to(&mut tx, &parts).await });

    let mut out = vec![0u8; expected.len()];
    rx.read_exact(&mut out).await?;
    assert_eq!(out, expected.as_ref());

    write_task.await??;
    Ok(())
}

#[tokio::test]
async fn write_parts_total_bytes_sanity() -> Result<()> {
    let (mut tx, mut rx) = tokio::io::duplex(64 * 1024);
    let payloads = (0..128)
        .map(|_| Bytes::from(vec![0xCD; 128]))
        .collect::<Vec<_>>();
    let parts = felix_wire::binary::encode_event_batch_parts(3, &payloads)?;
    let expected_bytes = parts.frame_len();

    let write_task = tokio::spawn(async move { write_parts_to(&mut tx, &parts).await });
    let mut read = 0usize;
    let mut buf = vec![0u8; 4096];
    while read < expected_bytes {
        let n = rx.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        read += n;
    }
    assert_eq!(read, expected_bytes);
    write_task.await??;
    Ok(())
}

#[tokio::test]
async fn writer_parts_match_legacy_encoded_bytes() -> Result<()> {
    let (mut tx, mut rx) = tokio::io::duplex(16 * 1024);
    let payloads = vec![
        Bytes::from(vec![0x11; 17]),
        Bytes::from(vec![0x22; 256]),
        Bytes::from(vec![0x33; 3]),
    ];
    let expected = felix_wire::binary::encode_event_batch_bytes(17, &payloads)?;
    let parts = felix_wire::binary::encode_event_batch_parts(17, &payloads)?;

    let write_task = tokio::spawn(async move { write_parts_to(&mut tx, &parts).await });
    let mut out = vec![0u8; expected.len()];
    rx.read_exact(&mut out).await?;

    write_task.await??;
    assert_eq!(out, expected.as_ref());
    Ok(())
}

#[tokio::test]
async fn write_parts_many_writes_two_frames_in_order() -> Result<()> {
    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;
    let (opened_tx, opened_rx) = tokio::sync::oneshot::channel();

    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let mut send = connection.open_uni().await?;
        let _ = opened_tx.send(());
        let frames = vec![
            felix_wire::binary::encode_event_batch_parts(1, &[Bytes::from_static(b"aa")])?,
            felix_wire::binary::encode_event_batch_parts(2, &[Bytes::from_static(b"bbb")])?,
        ];
        let result = write_parts_many(&mut send, frames).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        result
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    opened_rx.await.context("uni stream not opened")?;
    let mut recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = crate::serving::quic::FrameScratch::new();
    let frame1 =
        crate::serving::quic::codec::read_frame_limited_into(&mut recv, 16 * 1024, &mut scratch)
            .await?
            .expect("frame1");
    let batch1 = felix_wire::binary::decode_event_batch(&frame1).expect("decode frame1");
    assert_eq!(batch1.subscription_id, 1);
    assert_eq!(batch1.payloads[0].as_ref(), b"aa");

    let frame2 =
        crate::serving::quic::codec::read_frame_limited_into(&mut recv, 16 * 1024, &mut scratch)
            .await?
            .expect("frame2");
    let batch2 = felix_wire::binary::decode_event_batch(&frame2).expect("decode frame2");
    assert_eq!(batch2.subscription_id, 2);
    assert_eq!(batch2.payloads[0].as_ref(), b"bbb");

    let total = server_task.await.context("server join")??;
    assert!(total > 0);
    Ok(())
}
