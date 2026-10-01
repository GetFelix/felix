//! Atomic commits: by hand-built frames and through `felix-client`.
//!
//! The scenarios are the `commit.*` entries in `scenarios.toml`.

use anyhow::{Result, anyhow, bail};
use bytes::Bytes;
use felix_broker_service::serving::quic;
use felix_client::{Client, CommitError, CommitOp};
use felix_wire::{Message, StateChange};
use rustls::pki_types::CertificateDer;

use super::MAX_TEST_FRAME_BYTES;
use super::fixture::{AuthFixture, build_client_config};
use super::frames::read_frame;

/// A durable stream the suite registers for these checks: a commit needs a log.
pub(crate) const COMMIT_STREAM: &str = "conformance-commits";

pub(crate) async fn run_commit(
    connection: &felix_transport::QuicConnection,
    auth: &AuthFixture,
) -> Result<()> {
    println!("Running atomic commit checks...");
    old_peer_sees_a_plain_ok(connection, auth).await?;

    // commit.negotiated: the bit is advertised to a peer that offers
    // capabilities.
    let (mut send, mut recv) = connection.open_bi().await?;
    let mut scratch = quic::FrameScratch::new();
    quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
            client_flags: Some(felix_wire::KNOWN_FLAGS),
            client_features: Some(felix_wire::FEATURE_REDIRECT),
        },
    )
    .await?;
    match quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut scratch).await? {
        Some(Message::AuthOk {
            server_features: Some(features),
            ..
        }) if felix_wire::supports_feature(features, felix_wire::FEATURE_ATOMIC_COMMIT) => {}
        other => bail!("auth_ok did not advertise FEATURE_ATOMIC_COMMIT: {other:?}"),
    }

    // commit.returns_the_offset
    quic::write_message(
        &mut send,
        Message::Commit {
            tenant_id: auth.tenant_id.clone(),
            namespace: "default".to_string(),
            stream: COMMIT_STREAM.to_string(),
            entity_key: Bytes::from_static(b"order-1"),
            event: Bytes::from_static(b"placed"),
            changes: vec![StateChange::Put {
                key: "order-1".to_string(),
                value: Bytes::from_static(b"placed"),
            }],
            request_id: 1,
        },
    )
    .await?;
    let offset =
        match quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut scratch).await? {
            Some(Message::CommitOk {
                request_id: 1,
                offset,
            }) => offset,
            other => bail!("commit was not answered with commit_ok: {other:?}"),
        };

    // commit.state_get_returns_the_version
    quic::write_message(
        &mut send,
        Message::StateGet {
            tenant_id: auth.tenant_id.clone(),
            namespace: "default".to_string(),
            stream: COMMIT_STREAM.to_string(),
            entity_key: Bytes::from_static(b"order-1"),
            key: "order-1".to_string(),
            request_id: 2,
        },
    )
    .await?;
    match quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut scratch).await? {
        Some(Message::StateValue {
            value: Some(value),
            version: Some(version),
            as_of: Some(as_of),
            request_id: 2,
        }) if value == "placed" && version == offset && as_of >= offset => {}
        other => bail!("state_get did not return the commit's value at offset {offset}: {other:?}"),
    }
    send.finish()?;
    Ok(())
}

/// commit.negotiated: a peer that offers nothing gets the plain `ok` it
/// always did, byte for byte, with no feature it cannot decode.
async fn old_peer_sees_a_plain_ok(
    connection: &felix_transport::QuicConnection,
    auth: &AuthFixture,
) -> Result<()> {
    let (mut send, mut recv) = connection.open_bi().await?;
    quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
            client_flags: None,
            client_features: None,
        },
    )
    .await?;
    let frame = read_frame(&mut recv)
        .await?
        .ok_or_else(|| anyhow!("no answer to a legacy auth"))?;
    let expected = Message::Ok.encode()?;
    if frame.header.flags != expected.header.flags || frame.payload != expected.payload {
        bail!(
            "a legacy auth was not answered with the original ok frame: {:?}",
            frame.payload
        );
    }
    send.finish()?;
    Ok(())
}

pub(crate) async fn run_client_commit(
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    auth: &AuthFixture,
) -> Result<()> {
    println!("Running client atomic commit checks...");
    let client = Client::connect(addr, "localhost", build_client_config(cert, auth)?).await?;
    let tenant = auth.tenant_id.as_str();

    let receipt = client
        .commit(
            tenant,
            "default",
            b"order-2",
            vec![
                CommitOp::enqueue(COMMIT_STREAM, "shipped"),
                CommitOp::put(COMMIT_STREAM, "order-2", "shipped"),
            ],
        )
        .await?;
    let state = client
        .state_get(tenant, "default", COMMIT_STREAM, b"order-2", "order-2")
        .await?;
    if state.value.as_deref() != Some(&b"shipped"[..]) || state.version != Some(receipt.offset) {
        bail!("client state_get disagrees with the commit: {state:?} vs {receipt:?}");
    }
    let absent = client
        .state_get(
            tenant,
            "default",
            COMMIT_STREAM,
            b"order-2",
            "never-written",
        )
        .await?;
    if absent.value.is_some() {
        bail!("a never-written key read as present: {absent:?}");
    }

    // commit.another_stream_is_refused
    let split = client
        .commit(
            tenant,
            "default",
            b"order-3",
            vec![
                CommitOp::publish(COMMIT_STREAM, "placed"),
                CommitOp::put("conformance", "order-3", "placed"),
            ],
        )
        .await;
    match split
        .as_ref()
        .map_err(|err| err.downcast_ref::<CommitError>())
    {
        Err(Some(CommitError::NotOnOwningShard { index: 1, .. })) => {}
        other => bail!("a commit across two streams was not refused: {other:?}"),
    }

    // commit.exactly_one_event
    for ops in [
        vec![CommitOp::put(COMMIT_STREAM, "k", "v")],
        vec![
            CommitOp::publish(COMMIT_STREAM, "a"),
            CommitOp::enqueue(COMMIT_STREAM, "b"),
        ],
    ] {
        let refused = client.commit(tenant, "default", b"order-4", ops).await;
        match refused
            .as_ref()
            .map_err(|err| err.downcast_ref::<CommitError>())
        {
            Err(Some(CommitError::EventCount(_))) => {}
            other => bail!("a commit without exactly one event was not refused: {other:?}"),
        }
    }
    Ok(())
}
