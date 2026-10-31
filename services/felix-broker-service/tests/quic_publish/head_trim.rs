//! `trim_before` over QUIC: a trim hides the records below its floor, a
//! stale expected offset writes nothing, only a manage or trim grant may
//! trim, and every refusal has a code of its own.

use super::*;

/// One authenticated control stream, answering each request with the next
/// message the broker sends.
struct Session {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    scratch: felix_broker_service::serving::quic::FrameScratch,
    _connection: felix_transport::QuicConnection,
    _client: QuicClient,
}

impl Session {
    /// Authenticate with `perms`, offering error codes and, when `head_trim`,
    /// `FEATURE_HEAD_TRIM`.
    async fn open(
        addr: std::net::SocketAddr,
        cert: CertificateDer<'static>,
        perms: &[&str],
        head_trim: bool,
    ) -> Result<(Self, Option<u32>)> {
        let client = QuicClient::bind(
            "0.0.0.0:0".parse()?,
            build_quinn_client_config(cert)?,
            TransportConfig::default(),
        )?;
        let connection = client.connect(addr, "localhost").await?;
        let (send, recv) = connection.open_bi().await?;
        let mut session = Self {
            send,
            recv,
            scratch: felix_broker_service::serving::quic::FrameScratch::new(),
            _connection: connection,
            _client: client,
        };
        let token = auth_fixture("t1", perms.iter().map(|perm| perm.to_string()).collect()).token;
        let hi = if head_trim {
            felix_wire::FEATURE_HEAD_TRIM
        } else {
            0
        };
        let (features, features_hi) =
            felix_wire::offer_features(felix_wire::FEATURE_ERROR_CODES, hi);
        let answer = session
            .exchange(Message::Auth {
                tenant_id: "t1".to_string(),
                token,
                client_flags: Some(felix_wire::ORIGINAL_V1_FLAGS),
                client_features: Some(features),
                client_features_hi: features_hi,
            })
            .await?;
        match answer {
            Message::AuthOk {
                server_features_hi, ..
            } => Ok((session, server_features_hi)),
            other => anyhow::bail!("expected auth_ok, got {other:?}"),
        }
    }

    async fn exchange(&mut self, message: Message) -> Result<Message> {
        felix_broker_service::serving::quic::write_message(&mut self.send, message).await?;
        timeout(
            Duration::from_secs(10),
            felix_broker_service::serving::quic::read_message_limited(
                &mut self.recv,
                1 << 20,
                &mut self.scratch,
            ),
        )
        .await??
        .context("the broker closed the stream")
    }

    async fn publish(&mut self, count: usize) -> Result<()> {
        let answer = self
            .exchange(Message::PublishBatch {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "orders".to_string(),
                payloads: (0..count).map(|n| format!("r{n}").into_bytes()).collect(),
                key: None,
                shard: None,
                request_id: Some(1),
                ack: Some(AckMode::PerBatch),
            })
            .await?;
        anyhow::ensure!(
            matches!(answer, Message::PublishOk { .. }),
            "publish: {answer:?}"
        );
        Ok(())
    }

    async fn trim(&mut self, before: u64, expected_offset: u64) -> Result<Message> {
        self.exchange(Message::TrimBefore {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
            shard: 0,
            before,
            expected_offset,
            headers: Some(bytes::Bytes::from_static(b"holder-2")),
            request_id: 7,
        })
        .await
    }

    async fn read_from(&mut self, from: u64) -> Result<Message> {
        self.exchange(Message::StreamRead {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
            shard: 0,
            from,
            end: None,
            max_records: 0,
            max_bytes: 0,
            request_id: 8,
        })
        .await
    }
}

const MANAGER: &[&str] = &[
    "stream.manage:stream:t1/*/*",
    "stream.publish:stream:t1/*/*",
    "stream.subscribe:stream:t1/*/*",
];

fn code_of(message: &Message) -> Option<(felix_wire::ErrorCode, Option<String>)> {
    match message {
        Message::Error { code, detail, .. } | Message::PublishError { code, detail, .. } => Some((
            code.clone()?,
            detail.as_ref().and_then(|d| d.reason.clone()),
        )),
        _ => None,
    }
}

/// **A trim hides what is below its floor, and a stale one writes nothing.**
/// A read below the floor is too old, names the floor and carries the trim's
/// headers to a client that asked for them, and a retry with the old tail is
/// refused with the new one.
#[tokio::test]
#[serial]
async fn a_trim_hides_history_and_a_stale_one_is_refused() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (broker, addr, cert, _auth, _config, server_task) =
        serve_durable_orders(dir.path()).await?;
    broker.allow_head_trims_when(|| true);
    let (mut session, server_hi) = Session::open(addr, cert.clone(), MANAGER, true).await?;
    assert!(
        server_hi.is_some_and(|hi| felix_wire::supports_feature(hi, felix_wire::FEATURE_HEAD_TRIM)),
        "the broker advertises head_trim: {server_hi:?}"
    );
    session.publish(10).await?;

    match session.trim(5, 10).await? {
        Message::TrimOk {
            request_id: 7,
            offset: Some(10),
            oldest: 5,
        } => {}
        other => anyhow::bail!("expected trim_ok at 10, got {other:?}"),
    }
    match session.read_from(0).await? {
        Message::SubscribeCursorError {
            reason: felix_wire::CursorErrorReason::TooOld,
            requested: 0,
            available: 5,
            trim_headers: Some(headers),
        } => assert_eq!(&headers[..], b"holder-2"),
        other => anyhow::bail!("expected too_old at 5 with the headers, got {other:?}"),
    }
    match session.read_from(5).await? {
        Message::StreamRecords { records, .. } => {
            let offsets: Vec<u64> = records.iter().map(|record| record.offset).collect();
            assert_eq!(offsets, [5, 6, 7, 8, 9], "the trim record is not a record");
        }
        other => anyhow::bail!("expected records from 5, got {other:?}"),
    }

    // The same request again: it landed, so the tail moved past it.
    match session.trim(5, 10).await? {
        Message::PublishRefused {
            request_id: 7,
            reason: felix_wire::PublishRefusalReason::OffsetMismatch { tail: 11 },
            ..
        } => {}
        other => anyhow::bail!("expected offset_mismatch with tail 11, got {other:?}"),
    }
    let (mut late, _) = Session::open(addr, cert, MANAGER, true).await?;
    match late.trim(8, 10).await? {
        Message::PublishRefused {
            reason: felix_wire::PublishRefusalReason::OffsetMismatch { tail: 11 },
            ..
        } => {}
        other => anyhow::bail!("expected offset_mismatch, got {other:?}"),
    }
    match late.read_from(0).await? {
        Message::SubscribeCursorError { available: 5, .. } => {}
        other => anyhow::bail!("a refused trim moved the floor: {other:?}"),
    }
    // Each refused trim wrote nothing.
    match session.trim(5, 11).await? {
        Message::TrimOk {
            offset: None,
            oldest: 5,
            ..
        } => {}
        other => anyhow::bail!("expected a no-op at the tail 11, got {other:?}"),
    }
    server_task.abort();
    Ok(())
}

/// **Only a grant that allows trims allows one.** A viewer and a publisher
/// are refused with `forbidden`, write nothing, and keep their stream.
#[tokio::test]
#[serial]
async fn a_viewer_or_publisher_cannot_trim() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (broker, addr, cert, _auth, _config, server_task) =
        serve_durable_orders(dir.path()).await?;
    broker.allow_head_trims_when(|| true);
    let (mut manager, _) = Session::open(addr, cert.clone(), MANAGER, true).await?;
    manager.publish(4).await?;

    for perms in [
        &["stream.subscribe:stream:t1/*/*"][..],
        &["stream.publish:stream:t1/*/*"][..],
    ] {
        let (mut session, _) = Session::open(addr, cert.clone(), perms, true).await?;
        let answer = session.trim(2, 4).await?;
        assert_eq!(
            code_of(&answer).map(|(code, _)| code),
            Some(felix_wire::ErrorCode::Forbidden),
            "{perms:?}: {answer:?}"
        );
        // Still open, and nothing was trimmed.
        let answer = session.trim(2, 4).await?;
        assert!(code_of(&answer).is_some(), "{perms:?}: {answer:?}");
    }
    match manager.read_from(0).await? {
        Message::StreamRecords { records, .. } => assert_eq!(records.len(), 4),
        other => anyhow::bail!("expected every record, got {other:?}"),
    }

    // `stream.trim` alone is enough.
    let (mut trimmer, _) =
        Session::open(addr, cert, &["stream.trim:stream:t1/default/orders"], true).await?;
    match trimmer.trim(2, 4).await? {
        Message::TrimOk { oldest: 2, .. } => {}
        other => anyhow::bail!("expected trim_ok, got {other:?}"),
    }
    server_task.abort();
    Ok(())
}

/// **Each refusal has its own code.** Before the node allows trims the
/// answer is `feature_not_enabled` naming `head_trim`; a floor past the
/// trim's own record is `invalid_request` with `trim_past_tail`. And a
/// client that did not offer `FEATURE_HEAD_TRIM` gets the cursor error it
/// always got, without the headers.
#[tokio::test]
#[serial]
async fn refusals_are_coded_and_old_clients_see_old_frames() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (broker, addr, cert, _auth, _config, server_task) =
        serve_durable_orders(dir.path()).await?;
    let enabled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gate = Arc::clone(&enabled);
    broker.allow_head_trims_when(move || gate.load(std::sync::atomic::Ordering::Acquire));
    let (mut session, _) = Session::open(addr, cert.clone(), MANAGER, true).await?;
    session.publish(4).await?;

    let answer = session.trim(2, 4).await?;
    assert!(
        matches!(answer, Message::PublishError { request_id: 7, .. }),
        "a refusal answers the request: {answer:?}"
    );
    assert_eq!(
        code_of(&answer),
        Some((
            felix_wire::ErrorCode::FeatureNotEnabled,
            Some(felix_wire::trim_reason::HEAD_TRIM.to_string())
        )),
        "{answer:?}"
    );
    enabled.store(true, std::sync::atomic::Ordering::Release);
    let answer = session.trim(5, 4).await?;
    assert_eq!(
        code_of(&answer),
        Some((
            felix_wire::ErrorCode::InvalidRequest,
            Some(felix_wire::trim_reason::PAST_TAIL.to_string())
        )),
        "{answer:?}"
    );
    assert!(matches!(session.trim(2, 4).await?, Message::TrimOk { .. }));

    let (mut old, server_hi) = Session::open(addr, cert, MANAGER, false).await?;
    assert_eq!(server_hi, None, "no extended word for a client without one");
    let frame_json = {
        felix_broker_service::serving::quic::write_message(
            &mut old.send,
            Message::StreamRead {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "orders".to_string(),
                shard: 0,
                from: 0,
                end: None,
                max_records: 0,
                max_bytes: 0,
                request_id: 9,
            },
        )
        .await?;
        let frame = timeout(
            Duration::from_secs(10),
            felix_broker_service::serving::quic::read_frame_limited_into(
                &mut old.recv,
                1 << 20,
                &mut old.scratch,
            ),
        )
        .await??
        .context("closed")?;
        String::from_utf8(frame.payload.to_vec())?
    };
    assert!(
        frame_json.contains(r#""type":"subscribe_cursor_error""#)
            && !frame_json.contains("trim_headers"),
        "{frame_json}"
    );
    server_task.abort();
    Ok(())
}
