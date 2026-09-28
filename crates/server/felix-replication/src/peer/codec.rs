//! Reading and writing internal frames on a QUIC stream.
//!
//! Every read is bounded by the length the header declares, which is itself
//! bounded by [`felix_wire::internal::MAX_BODY_BYTES`] before a buffer is sized
//! from it. A peer is authenticated, not assumed correct.
use anyhow::{Context, Result, bail};
use bytes::{Bytes, BytesMut};
use felix_wire::internal::{
    FrameEnvelope, InternalHeader, InternalMessage, MAX_BODY_BYTES, correlation_id_in,
};
use quinn::{RecvStream, SendStream};

/// Write one complete frame.
///
/// A single `write_all` per frame is what lets several requests share a stream:
/// the caller holds the only handle to this `SendStream`, so frames cannot
/// interleave.
pub async fn write_frame(send: &mut SendStream, message: &InternalMessage) -> Result<()> {
    let frame = message.encode().context("encode internal frame")?;
    send.write_all(&frame)
        .await
        .context("write internal frame")?;
    Ok(())
}

/// What arrived on the stream.
pub enum Incoming {
    Message(InternalMessage),
    /// A frame whose kind this build does not know, stepped over rather than
    /// fatal. The caller answers it and the stream carries on.
    ///
    /// This is how the protocol stays extensible: adding a kind is the
    /// sanctioned additive change, and it is only additive if an older peer can
    /// refuse one frame instead of dropping a lane every in-flight request is
    /// sharing.
    UnknownKind {
        kind: u16,
        correlation_id: u64,
    },
    /// The stream ended between frames.
    Eof,
}

/// Read one complete frame.
///
/// [`Incoming::Eof`] is only returned when the stream ended *between* frames. A
/// stream that ends part-way through one is an error: the peer stopped
/// mid-message, and treating that as a clean close would silently drop a
/// request.
pub async fn read_frame(recv: &mut RecvStream) -> Result<Incoming> {
    let mut head = [0u8; InternalHeader::LEN];
    match recv.read_exact(&mut head).await {
        Ok(()) => {}
        Err(quinn::ReadExactError::FinishedEarly(0)) => return Ok(Incoming::Eof),
        Err(err) => return Err(err).context("read internal header"),
    }

    // The envelope, not the typed header: the magic and version decide whether
    // the framing is ours at all, and only then is the declared length worth
    // trusting. A frame that is not ours is still fatal — there is nothing to
    // step over, because the bytes are not laid out the way this assumes.
    let head = Bytes::copy_from_slice(&head);
    let envelope = FrameEnvelope::decode(&head).context("decode internal header")?;
    if envelope.length > MAX_BODY_BYTES {
        bail!("internal frame declares {} bytes", envelope.length);
    }

    let mut frame = BytesMut::with_capacity(InternalHeader::LEN + envelope.length as usize);
    frame.extend_from_slice(&head);
    frame.resize(InternalHeader::LEN + envelope.length as usize, 0);
    recv.read_exact(&mut frame[InternalHeader::LEN..])
        .await
        .context("read internal body")?;
    let frame = frame.freeze();

    if felix_wire::internal::Kind::from_u16(envelope.kind).is_err() {
        // The body has been read, so the stream is still aligned on a frame
        // boundary. Every body begins with its correlation id, which is what
        // lets the refusal be matched to the request that caused it.
        let correlation_id = correlation_id_in(&frame[InternalHeader::LEN..]).unwrap_or_default();
        return Ok(Incoming::UnknownKind {
            kind: envelope.kind,
            correlation_id,
        });
    }

    Ok(Incoming::Message(
        InternalMessage::decode(frame).context("decode internal frame")?,
    ))
}
