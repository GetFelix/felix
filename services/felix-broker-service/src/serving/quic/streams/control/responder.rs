//! Answering on the control stream.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::Result;
use tokio::sync::{mpsc, watch};

use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::publish::{
    AckTimeoutState, Outgoing, handle_ack_enqueue_result, send_outgoing_critical,
};

/// Everything needed to answer on the control stream: the outbound queue, its
/// depth and throttle, and the state that decides when a full queue ends the
/// stream.
pub(super) struct Responder<'a> {
    pub(super) out_ack_tx: &'a mpsc::Sender<Outgoing>,
    pub(super) out_ack_depth: &'a Arc<AtomicUsize>,
    pub(super) ack_throttle_tx: &'a watch::Sender<bool>,
    pub(super) ack_timeout_state: &'a Arc<parking_lot::Mutex<AckTimeoutState>>,
    pub(super) cancel_tx: &'a watch::Sender<bool>,
}

pub(super) async fn send_control_error(
    out_ack_tx: &mpsc::Sender<Outgoing>,
    out_ack_depth: &Arc<AtomicUsize>,
    ack_throttle_tx: &watch::Sender<bool>,
    ack_timeout_state: &Arc<parking_lot::Mutex<AckTimeoutState>>,
    cancel_tx: &watch::Sender<bool>,
    error: ClientError,
) -> Result<()> {
    handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::Message(error.into_message()),
        )
        .await,
        ack_timeout_state,
        ack_throttle_tx,
        cancel_tx,
    )
    .await
}
