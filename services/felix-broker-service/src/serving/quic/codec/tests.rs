use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_transport::{QuicClient, QuicConnection, QuicServer, TransportConfig};
use felix_wire::FrameHeader;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::PrivatePkcs8KeyDer;

use super::*;

/// A connected client/server pair; the server side is where frames are read.
async fn quic_pair() -> Result<(QuicClient, QuicConnection, QuicConnection)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let accept = tokio::spawn(async move { server.accept().await });

    let mut roots = RootCertStore::empty();
    roots.add(cert_der)?;
    let quinn = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    let client = QuicClient::bind("0.0.0.0:0".parse()?, quinn, TransportConfig::default())?;
    let client_conn = client.connect(addr, "localhost").await?;
    let server_conn = accept.await??;
    Ok((client, client_conn, server_conn))
}

fn header(length: u32) -> [u8; FrameHeader::LEN] {
    let mut bytes = [0u8; FrameHeader::LEN];
    FrameHeader::new(0, length).encode_into(&mut bytes);
    bytes
}

/// A header that promises a full-size frame and then delivers a few bytes must
/// not have made the broker allocate the full size.
#[tokio::test]
async fn declared_length_is_not_allocated_before_bytes_arrive() -> Result<()> {
    let (_client, client_conn, server_conn) = quic_pair().await?;
    let declared: usize = 16 * 1024 * 1024;

    let mut send = client_conn.open_uni().await?;
    send.write_all(&header(declared as u32)).await?;
    send.write_all(&[7u8; 100]).await?;
    send.finish()?;

    let mut recv = server_conn.accept_uni().await?;
    let mut scratch = FrameScratch::new();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        read_frame_limited_into(&mut recv, declared, &mut scratch),
    )
    .await
    .context("read finishes")?;
    assert!(result.is_err(), "a truncated frame is an error");
    assert!(
        scratch.buffered() < 1024 * 1024,
        "scratch holds {} bytes for 100 bytes of payload",
        scratch.buffered()
    );
    Ok(())
}

/// A payload far bigger than the initial reservation is reassembled whole from
/// however many chunks it arrives in.
#[tokio::test]
async fn large_payload_is_read_across_chunks() -> Result<()> {
    let (_client, client_conn, server_conn) = quic_pair().await?;
    let payload: Vec<u8> = (0..(3 * 1024 * 1024 + 17))
        .map(|i| (i % 251) as u8)
        .collect();

    let mut send = client_conn.open_uni().await?;
    let body = payload.clone();
    let writer = tokio::spawn(async move {
        send.write_all(&header(body.len() as u32)).await?;
        send.write_all(&body).await?;
        send.finish()?;
        anyhow::Ok(send)
    });

    let mut recv = server_conn.accept_uni().await?;
    let mut scratch = FrameScratch::new();
    let frame = read_frame_limited_into(&mut recv, 16 * 1024 * 1024, &mut scratch)
        .await?
        .context("a frame")?;
    assert_eq!(frame.header.length as usize, payload.len());
    assert_eq!(&frame.payload[..], &payload[..]);
    // The stream then ends cleanly on the frame boundary.
    assert!(
        read_frame_limited_into(&mut recv, 16 * 1024 * 1024, &mut scratch)
            .await?
            .is_none()
    );
    let _send = writer.await??;
    Ok(())
}

/// A length over the cap is refused from the header alone.
#[tokio::test]
async fn length_over_the_cap_is_refused_before_reading() -> Result<()> {
    let (_client, client_conn, server_conn) = quic_pair().await?;
    let mut send = client_conn.open_uni().await?;
    send.write_all(&header(64 * 1024 + 1)).await?;

    let mut recv = server_conn.accept_uni().await?;
    let mut scratch = FrameScratch::new();
    let err = read_frame_limited_into(&mut recv, 64 * 1024, &mut scratch)
        .await
        .expect_err("over the cap");
    assert!(err.to_string().contains("exceeds"), "{err}");
    assert!(scratch.buffered() <= FrameHeader::LEN);
    Ok(())
}

fn frame_bytes(payload: &[u8]) -> Vec<u8> {
    let mut bytes = header(payload.len() as u32).to_vec();
    bytes.extend_from_slice(payload);
    bytes
}

/// Frames written back to back come out one at a time with the right bytes,
/// however the stream splits them into chunks. A chunk read takes everything
/// buffered, so the bytes past one frame must be kept for the next.
#[tokio::test]
async fn back_to_back_frames_are_read_exactly() -> Result<()> {
    let (_client, client_conn, server_conn) = quic_pair().await?;
    let sizes = [0usize, 1, 5_000, 70_000, 3, 300_000, 12, 1_499];
    let payloads: Vec<Vec<u8>> = sizes
        .iter()
        .enumerate()
        .map(|(n, &len)| (0..len).map(|i| ((i * 31 + n) % 251) as u8).collect())
        .collect();
    let mut wire = Vec::new();
    for payload in &payloads {
        wire.extend(frame_bytes(payload));
    }

    let mut send = client_conn.open_uni().await?;
    let writer = tokio::spawn(async move {
        send.write_all(&wire).await?;
        send.finish()?;
        anyhow::Ok(send)
    });

    let mut recv = server_conn.accept_uni().await?;
    let mut scratch = FrameScratch::new();
    for (n, payload) in payloads.iter().enumerate() {
        let frame = read_frame_limited_into(&mut recv, 1 << 20, &mut scratch)
            .await?
            .with_context(|| format!("frame {n}"))?;
        assert_eq!(frame.payload.len(), payload.len(), "frame {n}");
        assert_eq!(&frame.payload[..], &payload[..], "frame {n}");
    }
    assert!(
        read_frame_limited_into(&mut recv, 1 << 20, &mut scratch)
            .await?
            .is_none()
    );
    assert_eq!(scratch.buffered(), 0);
    let _send = writer.await??;
    Ok(())
}

/// Decoded records share their frame's buffer, so reading the next frame must
/// not write over a batch that is still held.
#[tokio::test]
async fn held_records_survive_the_next_frame_read() -> Result<()> {
    let (_client, client_conn, server_conn) = quic_pair().await?;
    let first: Vec<Vec<u8>> = (0..64u8).map(|n| vec![n; 4096]).collect();
    let second: Vec<Vec<u8>> = (0..64u8).map(|n| vec![255 - n; 4096]).collect();
    let mut wire = Vec::new();
    for batch in [&first, &second] {
        let frame = felix_wire::binary::encode_publish_batch("t1", "default", "orders", batch)?;
        let mut header_bytes = [0u8; FrameHeader::LEN];
        frame.header.encode_into(&mut header_bytes);
        wire.extend_from_slice(&header_bytes);
        wire.extend_from_slice(&frame.payload);
    }

    let mut send = client_conn.open_uni().await?;
    let writer = tokio::spawn(async move {
        send.write_all(&wire).await?;
        send.finish()?;
        anyhow::Ok(send)
    });

    let mut recv = server_conn.accept_uni().await?;
    let mut scratch = FrameScratch::new();
    let frame = read_frame_limited_into(&mut recv, 1 << 20, &mut scratch)
        .await?
        .context("first frame")?;
    let held = felix_wire::binary::decode_publish_batch(&frame)?;
    drop(frame);
    let frame = read_frame_limited_into(&mut recv, 1 << 20, &mut scratch)
        .await?
        .context("second frame")?;
    let next = felix_wire::binary::decode_publish_batch(&frame)?;
    assert_eq!(held.payloads, first);
    assert_eq!(next.payloads, second);
    let _send = writer.await??;
    Ok(())
}

/// Leftover bytes belong to the stream they came from; reading another stream
/// through the same scratch is refused instead of misparsed.
#[test]
fn leftover_bytes_are_not_read_as_another_stream() {
    use quinn::{Dir, Side};
    let a = StreamId::new(Side::Client, Dir::Uni, 0);
    let b = StreamId::new(Side::Client, Dir::Uni, 1);
    let mut scratch = FrameScratch::new();
    scratch.bind(a).expect("first stream");
    scratch.chunks.push_back(Bytes::from_static(b"next frame"));
    scratch.len = 10;
    assert!(scratch.bind(a).is_ok());
    assert!(scratch.bind(b).is_err());
    scratch.drain(10, None);
    assert!(scratch.bind(b).is_ok(), "an empty scratch can move streams");
}
