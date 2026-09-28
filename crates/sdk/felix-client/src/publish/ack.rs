//! Reading the broker's answers to acked publishes.
//!
//! An acked publish (`AckMode` other than `None`) always carries a
//! `request_id`. A broker that granted a publish window answers a stream's
//! publishes in the order it carried them; any other broker answers each as it
//! completes, so a `Quorum` or forwarded publish can be answered after one
//! written behind it. Either way the id, not the position, says which request
//! an answer is for.

use anyhow::{Context, Result};
use bytes::BytesMut;
#[cfg(test)]
use felix_wire::AckMode;
use felix_wire::Message;
use quinn::RecvStream;

use super::AckOutcome;
use crate::frame_io::read_frame_into_with_limit;
#[cfg(feature = "telemetry")]
use crate::telemetry::frame_counters;
#[cfg(feature = "telemetry")]
use crate::timings;

/// How long a publisher will wait for an ack before giving up.
///
/// Deliberately well above the broker's own commit-ack budget
/// (`FELIX_ACK_WAIT_TIMEOUT_MS`, 2s by default) so this never fires ahead of the
/// broker's own timeout — it is a backstop for a broker that answers nothing at
/// all, not a competing deadline. A broker that dies does not get this far: the
/// read fails as soon as the transport's idle timeout closes the connection.
const ACK_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// What a publish's ack says, beyond success or failure.
///
/// `forwarded_to` is the shard's owner when this broker was not it and passed
/// the batch on. A client that routes to the owner next time stops paying the
/// decrypt/re-encrypt/decrypt a forward costs -- roughly half the throughput
/// per core (#536).
#[derive(Debug)]
pub(crate) struct AckRead {
    pub(crate) message: Message,
    pub(crate) forwarded_to: Option<felix_wire::binary::PublishOwner>,
}

/// Wait for the broker's ack to the publish sent as `request_id`, when it is
/// the only one outstanding on the stream.
///
/// The outer error means the stream can no longer be trusted: it broke, timed
/// out, or answered some other request. The inner result is the broker's
/// answer to this one request.
#[cfg(test)]
pub(crate) async fn wait_for_ack(
    recv: &mut RecvStream,
    request_id: u64,
    frame_scratch: &mut BytesMut,
    max_frame_bytes: usize,
) -> Result<AckOutcome> {
    let (answered, outcome) = read_ack(recv, frame_scratch, max_frame_bytes).await?;
    if answered != request_id {
        anyhow::bail!("publish failed: ack for request {answered}, expected {request_id}");
    }
    Ok(outcome)
}

/// Read the next ack on a publish stream: the request it answers and the
/// broker's answer.
///
/// The outer error means the stream can no longer be trusted: it broke, timed
/// out, or sent something that is not an ack. The inner result is the
/// broker's answer to that request; a refusal there leaves the stream serving
/// the others.
pub(crate) async fn read_ack(
    recv: &mut RecvStream,
    frame_scratch: &mut BytesMut,
    max_frame_bytes: usize,
) -> Result<(u64, AckOutcome)> {
    // Bound the wait. The ack reader blocks here, so an ack that never
    // arrives wedges that stream's publishes indefinitely rather than failing.
    // This is a real possibility whenever the broker cannot answer — it is
    // overloaded, the stream broke, or (before capability negotiation) it
    // misparsed the frame and produced no reply at all. A timeout turns that hang
    // into an error the caller can see, retry, or fail on.
    let response = match tokio::time::timeout(
        ACK_WAIT_TIMEOUT,
        read_ack_message_with_timing(recv, frame_scratch, max_frame_bytes),
    )
    .await
    {
        Ok(response) => response?,
        Err(_) => {
            return Err(anyhow::anyhow!(
                "timed out after {ACK_WAIT_TIMEOUT:?} waiting for a publish ack"
            ));
        }
    };
    let (message, forwarded_to) = match response {
        Some(read) => (Some(read.message), read.forwarded_to),
        None => (None, None),
    };
    match message {
        Some(Message::PublishOk { request_id }) => {
            #[cfg(feature = "telemetry")]
            {
                let counters = frame_counters();
                counters
                    .ack_frames_in_ok
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                counters
                    .ack_items_in_ok
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Ok((request_id, Ok(forwarded_to)))
        }
        Some(Message::PublishError {
            request_id,
            message,
            code,
            retry,
            detail,
        }) => {
            #[cfg(feature = "telemetry")]
            {
                let counters = frame_counters();
                counters
                    .ack_frames_in_ok
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                counters
                    .ack_items_in_ok
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Ok((
                request_id,
                Err(crate::error::refused(
                    "publish failed",
                    message,
                    code,
                    retry,
                    detail,
                )),
            ))
        }
        Some(Message::PublishRefused {
            request_id,
            reason,
            message,
        }) => Ok((
            request_id,
            Err(crate::PublishRefused { reason, message }.into()),
        )),
        other => Err(anyhow::anyhow!("publish failed: {other:?}")),
    }
}

pub(crate) async fn read_ack_message_with_timing(
    recv: &mut RecvStream,
    frame_scratch: &mut BytesMut,
    max_frame_bytes: usize,
) -> Result<Option<AckRead>> {
    #[cfg(feature = "telemetry")]
    let sample = crate::telemetry::t_should_sample();
    #[cfg(feature = "telemetry")]
    let read_start = crate::telemetry::t_now_if(sample);
    let frame =
        match read_frame_into_with_limit(recv, frame_scratch, false, max_frame_bytes).await? {
            Some(frame) => frame,
            None => return Ok(None),
        };
    #[cfg(feature = "telemetry")]
    if let Some(start) = read_start {
        let read_ns = start.elapsed().as_nanos() as u64;
        timings::record_ack_read_wait_ns(read_ns);
        t_histogram!("client_ack_read_wait_ns").record(read_ns as f64);
    }
    #[cfg(feature = "telemetry")]
    let decode_start = crate::telemetry::t_now_if(sample);
    // A binary ack carries exactly the information `PublishOk`/`PublishError` do,
    // so normalise it into those variants here. Everything downstream — the
    // request_id correlation, the error mapping, the counters — then has a single
    // path regardless of which encoding the publish went out in.
    let mut forwarded_to = None;
    let message = if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_ACK != 0 {
        let ack = felix_wire::binary::decode_publish_ack(&frame).context("decode publish ack")?;
        if let Some(owner) = &ack.forwarded_to {
            // The batch was written by another broker, and this one paid to
            // decrypt and re-encrypt it on the way -- roughly half the
            // throughput per core (#536). Counted *and* returned: `ClusterClient`
            // routes the next publish for this shard straight to the owner, and
            // the counter is what says whether that is working -- a number a
            // client can answer about itself without a metrics recorder.
            //
            // Labelled by owner because the cardinality is the cluster's size,
            // and *which* broker the traffic should have gone to is the part
            // that says whether the connections are spread or all on one.
            t_counter!(
                "felix_client_publish_forwarded_total",
                "owner" => owner.node_id.clone()
            )
            .increment(1);
            // And on the always-on counter, which is what a build without the
            // telemetry feature -- and a test, and the perf harness -- can read
            // without installing a metrics recorder.
            crate::telemetry::record_publish_forwarded();
            tracing::debug!(
                owner = %owner.node_id,
                addr = owner.addr.as_deref().unwrap_or("<unpublished>"),
                generation = owner.generation,
                "publish was forwarded to the shard's owner",
            );
        }
        forwarded_to = ack.forwarded_to;
        match ack.error {
            None => Message::PublishOk {
                request_id: ack.request_id,
            },
            Some(message) => {
                let (code, retry) = ack.code.unzip();
                Message::PublishError {
                    request_id: ack.request_id,
                    message,
                    code,
                    retry,
                    detail: ack.detail,
                }
            }
        }
    } else {
        Message::decode(frame).context("decode message")?
    };
    #[cfg(feature = "telemetry")]
    if let Some(start) = decode_start {
        let decode_ns = start.elapsed().as_nanos() as u64;
        timings::record_ack_decode_ns(decode_ns);
        t_histogram!("client_ack_decode_ns").record(decode_ns as f64);
    }
    Ok(Some(AckRead {
        message,
        forwarded_to,
    }))
}

#[cfg(test)]
pub(crate) async fn maybe_wait_for_ack(
    recv: &mut RecvStream,
    ack: AckMode,
    request_id: Option<u64>,
    frame_scratch: &mut BytesMut,
) -> Result<()> {
    maybe_wait_for_ack_with_limit(
        recv,
        ack,
        request_id,
        frame_scratch,
        crate::config::DEFAULT_MAX_FRAME_BYTES,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn maybe_wait_for_ack_with_limit(
    recv: &mut RecvStream,
    ack: AckMode,
    request_id: Option<u64>,
    frame_scratch: &mut BytesMut,
    max_frame_bytes: usize,
) -> Result<()> {
    // AckMode::None is fire-and-forget; otherwise wait for PublishOk/PublishError.
    if ack == AckMode::None {
        return Ok(());
    }
    let request_id =
        request_id.ok_or_else(|| anyhow::anyhow!("missing request_id for acked publish"))?;
    wait_for_ack(recv, request_id, frame_scratch, max_frame_bytes)
        .await?
        .map(|_| ())
}
