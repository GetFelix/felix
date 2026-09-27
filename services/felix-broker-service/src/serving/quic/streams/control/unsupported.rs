//! Requests this broker does not implement: a `type` it does not know, or any
//! request in the `extension` area.

use anyhow::Result;
use felix_wire::{Frame, Message};

use super::{Ctx, Session, Step};
use crate::serving::quic::handlers::publish::{
    Outgoing, handle_ack_enqueue_result, send_outgoing_critical,
};

/// Answer `unsupported` to a client that can read it, and keep serving the
/// stream. Any other client gets what it always got: the stream ends, since
/// it has no way to decode an answer and would take a closed stream for one.
pub(super) async fn unsupported(
    cx: &Ctx<'_>,
    session: &Session,
    frame: &Frame,
    message: &Message,
) -> Result<Step> {
    let (request_type, extension, request_id) = match message {
        Message::Extension {
            name, request_id, ..
        } => ("extension".to_string(), Some(name.clone()), *request_id),
        _ => match Message::unknown_request(frame) {
            Some(request) => (request.request_type, None, request.request_id),
            None => ("unknown".to_string(), None, None),
        },
    };
    if session.auth_ctx.is_none()
        || !felix_wire::supports_feature(session.peer_features, felix_wire::FEATURE_UNSUPPORTED)
    {
        tracing::debug!(
            request_type,
            "closing a control stream on an unknown request"
        );
        anyhow::bail!("unknown message type {request_type:?}");
    }
    tracing::debug!(
        request_type,
        extension = extension.as_deref().unwrap_or(""),
        "answering a request this broker does not implement"
    );
    handle_ack_enqueue_result(
        send_outgoing_critical(
            cx.out_ack_tx,
            cx.out_ack_depth,
            "felix_broker_out_ack_depth",
            cx.ack_throttle_tx,
            Outgoing::Message(Message::Unsupported {
                request_type,
                extension,
                request_id,
            }),
        )
        .await,
        cx.ack_timeout_state,
        cx.ack_throttle_tx,
        cx.cancel_tx,
    )
    .await?;
    Ok(Step::Next)
}
