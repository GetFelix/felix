use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

use super::frame::{Event, FrameDecoder, Violation};
use super::handshake::{AllowedOrigins, Settings, accept_key_for_test};
use super::{Refusal, accept, parse};

const KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";

fn request(extra: &str) -> String {
    format!(
        "GET /mqtt HTTP/1.1\r\nHost: broker\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: {KEY}\r\nSec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Protocol: mqtt\r\n{extra}\r\n"
    )
}

fn refused(head: &str, settings: &Settings) -> Refusal {
    parse(head.as_bytes(), settings).expect_err("refused")
}

/// A client frame, masked with a key that is not all zero.
fn masked(opcode: u8, fin: bool, payload: &[u8]) -> Vec<u8> {
    let key = [0x37, 0xfa, 0x21, 0x3d];
    let mut out = vec![if fin { 0x80 } else { 0 } | opcode];
    match payload.len() {
        0..=125 => out.push(0x80 | payload.len() as u8),
        126..=0xFFFF => {
            out.push(0x80 | 126);
            out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        }
        n => {
            out.push(0x80 | 127);
            out.extend_from_slice(&(n as u64).to_be_bytes());
        }
    }
    out.extend_from_slice(&key);
    out.extend(payload.iter().enumerate().map(|(i, b)| b ^ key[i % 4]));
    out
}

/// Decode everything in `bytes`, fed `step` bytes at a time. A frame that is
/// cut off at the end is left unread.
fn decode_all(bytes: &[u8], step: usize) -> Result<(Vec<u8>, Vec<Event>), Violation> {
    let mut decoder = FrameDecoder::new();
    let (mut src, mut data, mut events) = (BytesMut::new(), BytesMut::new(), Vec::new());
    for chunk in bytes.chunks(step) {
        src.extend_from_slice(chunk);
        while let Some(event) = decoder.next(&mut src, &mut data)? {
            if event != Event::Data {
                events.push(event);
            }
        }
    }
    Ok((data.to_vec(), events))
}

#[test]
fn accept_key_matches_the_rfc_example() {
    assert_eq!(accept_key_for_test(KEY), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
}

#[test]
fn a_valid_upgrade_is_accepted() {
    let settings = Settings::default();
    assert_eq!(parse(request("").as_bytes(), &settings).unwrap(), KEY);
    // Header names and the tokens in them are case-insensitive, and a list
    // may carry other protocols and connection options.
    let head = request("")
        .replace("Upgrade: websocket", "uPgRaDe: WebSocket")
        .replace("Connection: Upgrade", "connection: keep-alive, UPGRADE")
        .replace("Protocol: mqtt", "Protocol: mqttv3.1, mqtt");
    assert!(parse(head.as_bytes(), &settings).is_ok());
}

#[test]
fn a_request_that_is_not_an_upgrade_is_refused() {
    let settings = Settings::default();
    let cases = [
        (request("").replace("GET", "POST"), 400, "ws_bad_request"),
        (
            request("").replace("HTTP/1.1", "HTTP/1.0"),
            400,
            "ws_bad_request",
        ),
        (request("").replace("/mqtt", "mqtt"), 400, "ws_bad_request"),
        (
            request("").replace("Upgrade: websocket\r\n", ""),
            426,
            "ws_not_upgrade",
        ),
        (
            request("").replace("Connection: Upgrade\r\n", ""),
            426,
            "ws_not_upgrade",
        ),
        (
            request("").replace("Version: 13", "Version: 8"),
            426,
            "ws_version",
        ),
        (
            request("").replace("Sec-WebSocket-Protocol: mqtt\r\n", ""),
            400,
            "ws_subprotocol",
        ),
        (
            request("").replace("Protocol: mqtt", "Protocol: amqp"),
            400,
            "ws_subprotocol",
        ),
        (request("").replace(KEY, "c2hvcnQ="), 400, "ws_bad_key"),
        (
            request("").replace(KEY, "not base64 at all!!!!!!!"),
            400,
            "ws_bad_key",
        ),
        (
            request(&format!("Sec-WebSocket-Key: {KEY}\r\n")),
            400,
            "ws_bad_request",
        ),
        (request("Content-Length: 5\r\n"), 400, "ws_bad_request"),
        (
            request("Transfer-Encoding: chunked\r\n"),
            400,
            "ws_bad_request",
        ),
        (request(" folded: line\r\n"), 400, "ws_bad_request"),
        (request("no colon here\r\n"), 400, "ws_bad_request"),
        (request("Bad Name: x\r\n"), 400, "ws_bad_request"),
        (
            request("X: a\nOrigin: https://x.example\r\n"),
            400,
            "ws_bad_request",
        ),
        (request("X: caf\u{e9}\r\n"), 400, "ws_bad_request"),
    ];
    for (head, status, reason) in cases {
        assert_eq!(
            refused(&head, &settings),
            Refusal { status, reason },
            "{head}"
        );
    }
}

#[test]
fn too_many_headers_are_refused() {
    let extra: String = (0..70).map(|i| format!("X-{i}: a\r\n")).collect();
    assert_eq!(refused(&request(&extra), &Settings::default()).status, 431);
}

#[test]
fn a_browser_origin_must_be_allowed() {
    let with_origin = request("Origin: https://App.example.com\r\n");
    let mut settings = Settings::default();
    // Not configured: browsers are out, other clients (no Origin) are in.
    assert_eq!(refused(&with_origin, &settings).reason, "ws_origin");
    assert!(parse(request("").as_bytes(), &settings).is_ok());

    settings.allowed_origins = AllowedOrigins::Only(vec!["https://app.example.com".into()]);
    assert!(parse(with_origin.as_bytes(), &settings).is_ok());
    let other = request("Origin: https://evil.example.com\r\n");
    assert_eq!(refused(&other, &settings).status, 403);
    assert_eq!(refused(&request("Origin: null\r\n"), &settings).status, 403);

    settings.allowed_origins = AllowedOrigins::Any;
    assert!(parse(other.as_bytes(), &settings).is_ok());
}

#[tokio::test]
async fn the_upgrade_is_answered_and_early_frames_are_kept() {
    let (mut client, server) = duplex(4096);
    let mut early = request("").into_bytes();
    early.extend(masked(2, true, b"hi"));
    client.write_all(&early).await.unwrap();

    let mut ws = accept(server, &Settings::default()).await.unwrap();
    let mut reply = vec![0u8; 512];
    let n = client.read(&mut reply).await.unwrap();
    let reply = std::str::from_utf8(&reply[..n]).unwrap();
    assert!(
        reply.starts_with("HTTP/1.1 101 Switching Protocols\r\n"),
        "{reply}"
    );
    assert!(reply.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n"));
    assert!(reply.contains("Sec-WebSocket-Protocol: mqtt\r\n"));

    let mut got = [0u8; 2];
    ws.read_exact(&mut got).await.unwrap();
    assert_eq!(&got, b"hi");
}

#[tokio::test]
async fn a_refusal_is_sent_and_the_connection_closed() {
    let (mut client, server) = duplex(4096);
    client
        .write_all(request("Origin: https://x.example\r\n").as_bytes())
        .await
        .unwrap();
    let err = accept(server, &Settings::default()).await.err().unwrap();
    assert_eq!(err.status, 403);
    let mut reply = String::new();
    client.read_to_string(&mut reply).await.unwrap();
    assert!(reply.starts_with("HTTP/1.1 403 Forbidden\r\n"), "{reply}");
}

#[tokio::test]
async fn mqtt_sent_to_the_websocket_port_is_refused_at_once() {
    let (mut client, server) = duplex(4096);
    // A CONNECT's first bytes. No more is sent, so waiting for a blank line
    // would hang until the timeout.
    client
        .write_all(&[0x10, 0x0c, 0x00, 0x04, b'M', b'Q'])
        .await
        .unwrap();
    let err = accept(server, &Settings::default()).await.err().unwrap();
    assert_eq!(err.reason, "ws_not_http");
}

#[tokio::test]
async fn a_header_over_the_cap_is_refused_at_the_cap() {
    let settings = Settings {
        max_header_bytes: 256,
        ..Settings::default()
    };
    let (mut client, server) = duplex(64 * 1024);
    // Never ends the head. The reader must stop at the cap, not wait.
    let mut flood = b"GET / HTTP/1.1\r\n".to_vec();
    flood.extend(std::iter::repeat_n(b'a', 60_000));
    client.write_all(&flood).await.unwrap();
    let err = accept(server, &settings).await.err().unwrap();
    assert_eq!(
        err,
        Refusal {
            status: 431,
            reason: "ws_header_too_large"
        }
    );

    // A head that ends just past the cap is over it too.
    let (mut client, server) = duplex(64 * 1024);
    let head = request(&format!("X-Pad: {}\r\n", "a".repeat(300)));
    client.write_all(head.as_bytes()).await.unwrap();
    let err = accept(server, &settings).await.err().unwrap();
    assert_eq!(err.status, 431);
}

#[tokio::test(start_paused = true)]
async fn a_client_that_never_finishes_the_upgrade_is_dropped() {
    let settings = Settings {
        upgrade_timeout: Duration::from_secs(3),
        ..Settings::default()
    };
    let (mut client, server) = duplex(4096);
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: x\r\n")
        .await
        .unwrap();
    let err = accept(server, &settings).await.err().unwrap();
    assert_eq!(
        err,
        Refusal {
            status: 408,
            reason: "ws_upgrade_timeout"
        }
    );
}

#[test]
fn a_message_split_into_frames_and_bytes_decodes_whole() {
    let mut wire = masked(2, false, b"hel");
    wire.extend(masked(0, false, b""));
    wire.extend(masked(0, true, b"lo"));
    for step in [1, 2, 3, 7, wire.len()] {
        let (data, events) = decode_all(&wire, step).unwrap();
        assert_eq!(data, b"hello", "step {step}");
        assert!(events.is_empty());
    }
}

#[test]
fn messages_coalesced_in_one_read_decode_in_order() {
    let mut wire = masked(2, true, b"one");
    wire.extend(masked(2, true, b"two"));
    wire.extend(masked(2, true, &[]));
    let (data, _) = decode_all(&wire, wire.len()).unwrap();
    assert_eq!(data, b"onetwo");
}

#[test]
fn payload_lengths_use_every_form() {
    for len in [0, 125, 126, 65_535, 65_536, 200_000] {
        let payload: Vec<u8> = (0..len).map(|i| i as u8).collect();
        let (data, _) = decode_all(&masked(2, true, &payload), 4096).unwrap();
        assert_eq!(data, payload, "len {len}");
    }
}

#[test]
fn control_frames_may_interrupt_a_fragmented_message() {
    let mut wire = masked(2, false, b"ab");
    wire.extend(masked(9, true, b"ping!"));
    wire.extend(masked(0, true, b"cd"));
    wire.extend(masked(10, true, b""));
    wire.extend(masked(8, true, &[0x03, 0xe8, b'b', b'y', b'e']));
    for step in [1, 5, wire.len()] {
        let (data, events) = decode_all(&wire, step).unwrap();
        assert_eq!(data, b"abcd");
        assert_eq!(
            events,
            vec![
                Event::Ping(b"ping!".to_vec()),
                Event::Pong,
                Event::Close(Some(1000))
            ]
        );
    }
}

#[test]
fn frames_that_break_the_format_are_violations() {
    let unmasked = {
        let mut f = masked(2, true, b"x");
        f[1] &= 0x7f;
        f
    };
    let reserved = {
        let mut f = masked(2, true, b"x");
        f[0] |= 0x40;
        f
    };
    let huge = {
        let mut f = vec![0x82, 0x80 | 127];
        f.extend_from_slice(&(1u64 << 63).to_be_bytes());
        f.extend_from_slice(&[0; 4]);
        f
    };
    let mut interleaved = masked(2, false, b"a");
    interleaved.extend(masked(2, true, b"b"));
    let bad_control = masked(9, false, b"");
    let long_control = masked(9, true, &[0; 126]);
    for (name, wire) in [
        ("unmasked", unmasked),
        ("reserved bit", reserved),
        ("length high bit", huge),
        ("unknown opcode", masked(3, true, b"")),
        ("stray continuation", masked(0, true, b"x")),
        ("new message inside one", interleaved),
        ("fragmented control", bad_control),
        ("oversized control", long_control),
        ("one byte close", masked(8, true, &[1])),
    ] {
        assert!(
            matches!(decode_all(&wire, 3), Err(Violation::Protocol(_))),
            "{name}"
        );
    }
    assert_eq!(
        decode_all(&masked(1, true, b"text"), 3).unwrap_err(),
        Violation::Text
    );
}

/// The same answer however the bytes are cut into reads, also for streams that
/// are mangled. The fuzz target checks this on arbitrary input; this is the
/// deterministic part that runs with the tests.
#[test]
fn frames_decode_the_same_however_they_are_cut() {
    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for _ in 0..3000 {
        let mut wire = Vec::new();
        for _ in 0..(next() % 5) {
            let payload: Vec<u8> = (0..next() % 300).map(|_| next() as u8).collect();
            let opcode = [2, 2, 0, 9, 10, 8, 1][(next() % 7) as usize];
            wire.extend(masked(opcode, next() % 3 != 0, &payload));
        }
        if !wire.is_empty() && next() % 2 == 0 {
            let at = (next() as usize) % wire.len();
            wire[at] ^= 1 << (next() % 8);
        }
        let cut = 1 + (next() % 9) as usize;
        let whole = decode_all(&wire, wire.len().max(1));
        let pieces = decode_all(&wire, cut);
        match (&whole, &pieces) {
            (Ok(a), Ok(b)) => assert_eq!(a, b),
            (Err(a), Err(b)) => assert_eq!(a, b),
            _ => panic!("cut {cut}: {whole:?} vs {pieces:?}"),
        }
    }
}

#[test]
fn a_partial_header_consumes_nothing() {
    let wire = masked(2, true, &[1; 300]);
    let mut decoder = FrameDecoder::new();
    let mut data = BytesMut::new();
    for cut in 0..8 {
        let mut src = BytesMut::from(&wire[..cut]);
        assert_eq!(decoder.next(&mut src, &mut data).unwrap(), None);
        assert_eq!(src.len(), cut, "cut {cut}");
    }
}

mod stream {
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    use super::{BytesMut, masked};
    use crate::ws::WsStream;

    fn pair() -> (tokio::io::DuplexStream, WsStream<tokio::io::DuplexStream>) {
        let (client, server) = duplex(1 << 20);
        (client, WsStream::new(server, BytesMut::new()))
    }

    #[tokio::test]
    async fn bytes_are_read_across_frames() {
        let (mut client, mut ws) = pair();
        client.write_all(&masked(2, false, b"abc")).await.unwrap();
        client.write_all(&masked(0, true, b"def")).await.unwrap();
        let mut got = [0u8; 6];
        ws.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"abcdef");
    }

    #[tokio::test]
    async fn bytes_are_read_as_frames_arrive() {
        let (mut client, mut ws) = pair();
        client.write_all(&masked(2, true, b"abc")).await.unwrap();
        let mut buf = bytes::BytesMut::new();
        assert_eq!(ws.read_buf(&mut buf).await.unwrap(), 3);
        let reader = tokio::spawn(async move {
            let mut buf = bytes::BytesMut::new();
            let n = ws.read_buf(&mut buf).await.unwrap();
            (n, buf)
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        client.write_all(&masked(2, true, b"defg")).await.unwrap();
        let (n, buf) = reader.await.unwrap();
        assert_eq!((n, &buf[..]), (4, &b"defg"[..]));
    }

    #[tokio::test]
    async fn a_ping_is_answered_while_only_reading() {
        let (mut client, mut ws) = pair();
        client.write_all(&masked(9, true, b"hey")).await.unwrap();
        client.write_all(&masked(2, true, b"x")).await.unwrap();
        let mut got = [0u8; 1];
        ws.read_exact(&mut got).await.unwrap();
        let mut pong = [0u8; 5];
        client.read_exact(&mut pong).await.unwrap();
        assert_eq!(&pong, &[0x8a, 3, b'h', b'e', b'y']);
    }

    #[tokio::test]
    async fn writes_become_binary_frames_and_big_ones_are_split() {
        let (mut client, mut ws) = pair();
        ws.write_all(b"hello").await.unwrap();
        let mut head = [0u8; 7];
        client.read_exact(&mut head).await.unwrap();
        assert_eq!(&head, &[0x82, 5, b'h', b'e', b'l', b'l', b'o']);

        let big = vec![7u8; 40_000];
        ws.write_all(&big).await.unwrap();
        let mut total = 0;
        while total < big.len() {
            let mut h = [0u8; 4];
            client.read_exact(&mut h[..2]).await.unwrap();
            assert_eq!(h[0], 0x82);
            let len = if h[1] == 126 {
                client.read_exact(&mut h[2..4]).await.unwrap();
                usize::from(u16::from_be_bytes([h[2], h[3]]))
            } else {
                usize::from(h[1])
            };
            let mut payload = vec![0u8; len];
            client.read_exact(&mut payload).await.unwrap();
            total += len;
        }
        assert_eq!(total, big.len());
    }

    #[tokio::test]
    async fn a_close_frame_is_echoed_and_ends_the_stream() {
        let (mut client, mut ws) = pair();
        client
            .write_all(&masked(8, true, &[0x03, 0xe9]))
            .await
            .unwrap();
        let mut rest = Vec::new();
        assert_eq!(ws.read_to_end(&mut rest).await.unwrap(), 0);
        let mut close = [0u8; 4];
        client.read_exact(&mut close).await.unwrap();
        assert_eq!(&close, &[0x88, 2, 0x03, 0xe9]);
    }

    #[tokio::test]
    async fn a_bad_frame_closes_with_a_code_and_fails_the_read() {
        let (mut client, mut ws) = pair();
        client.write_all(&masked(1, true, b"text")).await.unwrap();
        let mut got = [0u8; 1];
        assert!(ws.read_exact(&mut got).await.is_err());
        let mut close = [0u8; 4];
        client.read_exact(&mut close).await.unwrap();
        assert_eq!(&close, &[0x88, 2, 0x03, 0xeb]);
    }

    #[tokio::test]
    async fn shutdown_sends_a_normal_close() {
        let (mut client, mut ws) = pair();
        ws.write_all(b"x").await.unwrap();
        ws.shutdown().await.unwrap();
        let mut all = Vec::new();
        client.read_to_end(&mut all).await.unwrap();
        assert_eq!(all, [0x82, 1, b'x', 0x88, 2, 0x03, 0xe8]);
    }

    #[tokio::test]
    async fn the_peer_closing_the_socket_is_end_of_stream() {
        let (client, mut ws) = pair();
        drop(client);
        let mut got = [0u8; 1];
        assert_eq!(ws.read(&mut got).await.unwrap(), 0);
    }
}
