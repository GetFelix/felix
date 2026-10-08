//! Reading a bounded page of a stream shard on the control stream.

use anyhow::Result;
use felix_authz::Action;
use felix_wire::{Message, StreamRecord};

use super::authz::authorize_stream_simple;
use super::record_time::{ShardTarget, not_served_here};
use super::{Ctx, Session, Step};
use crate::serving::quic::handlers::cache_watch::WatchResponder;
use crate::serving::quic::handlers::subscribe::subscribe_error_message;

/// Payload bytes one page carries at most. Base64 and JSON make the frame
/// about a third larger, which keeps it well under the default 16 MiB frame
/// limit on both sides.
const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;

/// What a `stream_read` asks for besides the shard.
pub(super) struct ReadBounds {
    pub(super) from: u64,
    pub(super) end: Option<u64>,
    pub(super) max_records: u32,
    pub(super) max_bytes: u64,
}

/// Answer `stream_read` with `stream_records`. Needs what a subscribe needs,
/// since it reads the same records.
pub(super) async fn stream_read(
    cx: &Ctx<'_>,
    session: &mut Session,
    target: ShardTarget,
    bounds: ReadBounds,
    request_id: u64,
) -> Result<Step> {
    if !authorize_stream_simple(
        session.auth_ctx.as_ref(),
        &target.tenant_id,
        Action::StreamSubscribe,
        &target.namespace,
        &target.stream,
        cx.authz_ctx,
    )
    .await?
    {
        // An authenticated stream is kept open so the refusal reaches the
        // client: closing here can drop it, leaving only a closed stream.
        return Ok(if session.auth_ctx.is_some() {
            Step::Next
        } else {
            Step::Close(false)
        });
    }
    let answer = match read(cx, session.peer_features, &target, &bounds).await {
        Ok(page) => Message::StreamRecords {
            records: page
                .records
                .into_iter()
                .map(|record| StreamRecord {
                    offset: record.offset,
                    payload: record.payload,
                    publisher: record
                        .publisher
                        .map(|publisher| String::from_utf8_lossy(&publisher).into_owned()),
                    timestamp_micros: record.timestamp_micros,
                })
                .collect(),
            next_offset: page.next_offset,
            request_id,
        },
        Err(answer) => answer,
    };
    WatchResponder {
        out_ack_tx: cx.out_ack_tx,
        out_ack_depth: cx.out_ack_depth,
        ack_throttle_tx: cx.ack_throttle_tx,
        ack_timeout_state: cx.ack_timeout_state,
        cancel_tx: cx.cancel_tx,
    }
    .send(answer)
    .await?;
    Ok(Step::Next)
}

async fn read(
    cx: &Ctx<'_>,
    peer_features: u32,
    target: &ShardTarget,
    bounds: &ReadBounds,
) -> Result<felix_broker::ReadPage, Message> {
    if let Some(answer) = not_served_here(cx, peer_features, target) {
        return Err(answer);
    }
    // `0` asks for the broker's cap; the log caps the record count further.
    let max_records = match bounds.max_records {
        0 => usize::MAX,
        n => n as usize,
    };
    let max_bytes = match bounds.max_bytes {
        0 => MAX_PAGE_BYTES,
        n => usize::try_from(n).unwrap_or(usize::MAX).min(MAX_PAGE_BYTES),
    };
    cx.broker
        .read_range(
            &target.tenant_id,
            &target.namespace,
            &target.stream,
            target.shard,
            bounds.from,
            bounds.end,
            max_records,
            max_bytes,
        )
        .await
        .map_err(subscribe_error_message)
}
