//! Stream lifecycle orchestration for the broker's QUIC streams.
//!
//! The bi-directional control stream multiplexes control-plane requests,
//! cache request/response, and publish acknowledgements; uni streams are
//! ingress-only publish. The pieces composed here: a `writer` task that owns
//! every write to the `SendStream` (Quinn does not support concurrent
//! writes), the `control` loop, an `ack_waiter` for delayed publish acks, and
//! the `uni` loop. Queues are bounded, cancellation is cooperative, and
//! shutdown drains against a deadline.
//!
//! Each loop (read, write, ack waiter) has its own module so it can be read
//! and tested on its own.

mod ack_waiter;
mod control;
mod frame_source;
mod hooks;
mod uni;
mod writer;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::BytesMut;
use felix_broker::Broker;
use quinn::{RecvStream, SendStream};
use tokio::sync::{Mutex, Semaphore, mpsc, watch};

use crate::config::BrokerConfig;
use crate::serving::auth::BrokerAuth;
use crate::serving::quic::client_error::ErrorCodeSupport;
use crate::serving::quic::handlers::publish::{
    AckTimeoutState, AckWaiterMessage, Outgoing, PublishContext, reset_local_depth_only,
};
use crate::serving::quic::telemetry::t_counter;
#[cfg(feature = "telemetry")]
use crate::serving::quic::telemetry::t_histogram;
use crate::serving::quic::{ACK_QUEUE_DEPTH, ACK_WAITERS_MAX, GLOBAL_ACK_DEPTH};
use ack_waiter::run_ack_waiter_loop;
use control::run_control_loop;
#[cfg(test)]
use hooks::test_hooks;
use uni::{UniLoopArgs, run_uni_loop};
use writer::run_writer_loop;

/// Handle one bi-directional control stream.
///
/// Spawns the writer task (sole owner of the `SendStream`) and the ack
/// waiter, runs the control loop in place, then winds everything down: on an
/// abnormal close the tasks are cancelled, and either way they get a bounded
/// drain before being aborted. "Graceful" means the peer finished the stream
/// cleanly (EOF).
pub(crate) async fn handle_stream(
    broker: Arc<Broker>,
    connection: felix_transport::QuicConnection,
    config: BrokerConfig,
    auth: Arc<BrokerAuth>,
    publish_ctx: PublishContext,
    send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    // All outbound acks/responses go through this queue to the writer task —
    // nothing else may touch the SendStream.
    let (out_ack_tx, out_ack_rx) = mpsc::channel::<Outgoing>(ACK_QUEUE_DEPTH);

    // Local outbound-queue depth, reconciled with the global ack gauge.
    let out_ack_depth = Arc::new(AtomicUsize::new(0));

    // Per-connection stream-handle cache for the publish hot path.
    let stream_cache = HashMap::new();
    let stream_cache_key = String::new();

    let (cancel_tx, cancel_rx) = watch::channel(false);

    // Bounds in-flight ack-on-commit waiters so a stalled commit path cannot
    // accumulate unbounded state.
    let ack_waiters = Arc::new(Semaphore::new(ACK_WAITERS_MAX));

    // Writer-driven backpressure: flips when the outbound queue crosses its
    // watermarks, telling the control loop to throttle.
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);

    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(Instant::now())));

    // Reused across frames to keep the hot path allocation-free.
    let mut frame_scratch = BytesMut::with_capacity(config.max_frame_bytes.min(64 * 1024));

    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel::<AckWaiterMessage>(ACK_WAITERS_MAX);

    // Floored so it outlasts the quorum wait a publish to a `Quorum` stream
    // may be sitting on.
    let ack_wait_timeout = config.ack_wait_timeout();

    let out_ack_depth_worker = Arc::clone(&out_ack_depth);
    let cancel_tx_writer = cancel_tx.clone();
    let cancel_rx_writer = cancel_rx.clone();
    let ack_throttle_tx_writer = ack_throttle_tx.clone();

    let out_ack_tx_waiter = out_ack_tx.clone();
    let out_ack_depth_waiter = Arc::clone(&out_ack_depth);
    let ack_throttle_tx_waiter = ack_throttle_tx.clone();
    let ack_timeout_state_waiter = Arc::clone(&ack_timeout_state);
    let cancel_tx_waiter = cancel_tx.clone();
    let cancel_rx_waiter = cancel_rx.clone();

    // If the writer exits early, Outgoing messages still queued are dropped;
    // teardown reconciles the depth gauges via `reset_local_depth_only`.
    // What the client said it can read, set at `Auth` and applied by the writer.
    let error_codes = Arc::new(ErrorCodeSupport::default());

    let writer_handle = tokio::spawn(run_writer_loop(
        send,
        out_ack_rx,
        Arc::clone(&error_codes),
        out_ack_depth_worker,
        ack_throttle_tx_writer,
        cancel_tx_writer,
        cancel_rx_writer,
    ));

    // Decouples commit completion from the read loop so acks can be emitted
    // out of order.
    let ack_waiter_handle = tokio::spawn(run_ack_waiter_loop(
        ack_waiter_rx,
        out_ack_tx_waiter,
        out_ack_depth_waiter,
        ack_throttle_tx_waiter,
        ack_timeout_state_waiter,
        cancel_tx_waiter,
        cancel_rx_waiter,
        ack_wait_timeout,
    ));

    // Ok(true) = graceful EOF; Ok(false) = protocol violation or
    // handler-initiated close; Err = fatal decode/protocol error.
    let result = run_control_loop(
        &mut recv,
        Arc::clone(&broker),
        connection.clone(),
        config.clone(),
        Arc::clone(&auth),
        publish_ctx,
        stream_cache,
        stream_cache_key,
        out_ack_tx.clone(),
        Arc::clone(&out_ack_depth),
        ack_throttle_rx.clone(),
        ack_throttle_tx.clone(),
        Arc::clone(&ack_timeout_state),
        cancel_tx.clone(),
        cancel_rx.clone(),
        Arc::clone(&ack_waiters),
        ack_waiter_tx.clone(),
        ack_wait_timeout,
        &mut frame_scratch,
        error_codes,
    )
    .await;

    let graceful_close = matches!(result, Ok(true));
    if !graceful_close {
        let _ = cancel_tx.send(true);
    }

    // Dropping the senders is what lets the background tasks see EOF and exit.
    drop(out_ack_tx);
    drop(ack_waiter_tx);

    #[cfg(feature = "telemetry")]
    let drain_start = Instant::now();

    let drain_timeout = Duration::from_millis(config.control_stream_drain_timeout_ms);

    let mut writer_handle = writer_handle;
    let mut ack_waiter_handle = ack_waiter_handle;
    let mut timed_out = false;

    tokio::select! {
        _ = tokio::time::sleep(drain_timeout) => {
            timed_out = true;
        }
        _ = &mut writer_handle => {}
        _ = &mut ack_waiter_handle => {}
    }

    #[cfg(feature = "telemetry")]
    {
        t_histogram!("felix_broker_control_stream_drain_ms")
            .record(drain_start.elapsed().as_secs_f64() * 1000.0);
    }

    #[cfg(test)]
    if test_hooks::force_drain_timeout() {
        timed_out = true;
    }

    if timed_out {
        t_counter!("felix_broker_control_stream_drain_timeout_total").increment(1);
        writer_handle.abort();
        ack_waiter_handle.abort();
    }

    // Even on abort, or the depth gauges leak.
    reset_local_depth_only(
        &out_ack_depth,
        &GLOBAL_ACK_DEPTH,
        "felix_broker_out_ack_depth",
    );

    result.map(|_| ())
}

/// Handle one ingress-only uni publish stream: reads until EOF or a protocol
/// violation. There is no response path on a uni stream.
pub(crate) async fn handle_uni_stream(
    broker: Arc<Broker>,
    config: BrokerConfig,
    auth: Arc<BrokerAuth>,
    publish_ctx: PublishContext,
    mut recv: RecvStream,
    peer_certs: Option<Vec<rustls::pki_types::CertificateDer<'static>>>,
) -> Result<()> {
    let mut frame_scratch = BytesMut::with_capacity(config.max_frame_bytes.min(64 * 1024));

    run_uni_loop(
        &mut recv,
        broker,
        UniLoopArgs {
            config,
            auth,
            publish_ctx,
            stream_cache: HashMap::new(),
            stream_cache_key: String::new(),
            peer_certs,
        },
        &mut frame_scratch,
    )
    .await
}

#[cfg(test)]
mod tests;
