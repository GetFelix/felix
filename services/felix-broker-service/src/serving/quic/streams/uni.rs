//! The uni-directional publish stream loop. A client opens a uni stream to
//! send publish-only traffic — binary or JSON, single or batched — and nothing
//! is ever sent back on it. Anything other than auth or a publish is a
//! protocol violation and closes the stream.

use std::sync::Arc;
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;

use crate::serving::quic::codec::FrameScratch;
use anyhow::{Context, Result};
use felix_authz::{Action, Namespace, StreamName, TenantId, stream_resource};
use felix_broker::Broker;
use felix_wire::Message;

use super::frame_source::FrameSource;
use crate::config::BrokerConfig;
use crate::serving::auth::{AuthContext, BrokerAuth};
use crate::serving::quic::handlers::publish::{
    PublishContext, StreamHandleCache, handle_binary_publish_batch_uni,
    handle_publish_batch_message_uni, handle_publish_message_uni,
};
use crate::serving::quic::telemetry::t_counter;

pub(super) struct UniLoopArgs {
    pub config: BrokerConfig,
    pub auth: Arc<BrokerAuth>,
    pub publish_ctx: PublishContext,
    pub stream_cache: StreamHandleCache,
    pub stream_cache_key: String,
    /// The certificate chain the client presented, for subject binding.
    pub peer_certs: Option<Vec<rustls::pki_types::CertificateDer<'static>>>,
}

/// Run one uni publish stream until EOF, a decode error, or a protocol
/// violation. `frame_scratch` carries bytes read past one frame into the
/// next; `stream_cache`/`stream_cache_key` carry resolved stream state
/// between frames.
pub(super) async fn run_uni_loop<S: FrameSource + ?Sized>(
    source: &mut S,
    broker: Arc<Broker>,
    args: UniLoopArgs,
    frame_scratch: &mut FrameScratch,
) -> Result<()> {
    let UniLoopArgs {
        config,
        auth,
        publish_ctx,
        mut stream_cache,
        mut stream_cache_key,
        peer_certs,
    } = args;
    let mut auth_ctx: Option<AuthContext> = None;
    // Held until this stream authenticates; see `preauth`.
    let mut preauth_permit = Some(publish_ctx.preauth.admit_stream().await);
    loop {
        let frame_cap = publish_ctx
            .preauth
            .frame_cap(auth_ctx.is_some(), config.max_frame_bytes);
        let frame = match source.next_frame(frame_cap, frame_scratch).await? {
            Some(frame) => frame,
            None => break,
        };
        // Flags select the body's layout, and a uni stream has no way to
        // answer, so a bit this version does not know ends the stream.
        if felix_wire::has_unknown_flags(frame.header.flags) {
            anyhow::bail!("unsupported frame flags {:#06x}", frame.header.flags);
        }

        // Binary batches skip the JSON decode entirely.
        if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_BATCH != 0 {
            let handled = handle_binary_publish_batch_uni(
                &broker,
                &mut stream_cache,
                &mut stream_cache_key,
                &publish_ctx,
                &frame,
                auth_ctx.as_ref(),
            )
            .await?;
            if !handled {
                break;
            }
            continue;
        }

        let message = match Message::decode(frame.clone()).context("decode message") {
            Ok(message) => message,
            Err(err) => {
                #[cfg(feature = "telemetry")]
                {
                    let counters = crate::serving::quic::telemetry::frame_counters();
                    counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                    counters.pub_frames_in_err.fetch_add(1, Ordering::Relaxed);
                    counters.pub_batches_in_err.fetch_add(1, Ordering::Relaxed);
                }
                crate::serving::quic::telemetry::log_decode_error("uni_message", &err, &frame);
                return Err(err);
            }
        };

        match message {
            Message::Auth {
                tenant_id,
                token,
                // One-directional: there is no reply channel to advertise on,
                // and nothing here would ever send an optional message back.
                client_flags: _,
                client_features: _,
            } => {
                if auth_ctx.is_some() {
                    tracing::debug!("closing uni stream after duplicate auth");
                    break;
                }
                match auth
                    .authenticate_peer(&tenant_id, &token, peer_certs.as_deref())
                    .await
                {
                    Ok(ctx) => {
                        auth_ctx = Some(ctx);
                        preauth_permit.take();
                        publish_ctx.preauth.mark_authenticated();
                    }
                    Err(err) => {
                        tracing::debug!(error = %err, "closing uni stream after auth failure");
                        break;
                    }
                }
            }
            Message::Publish {
                tenant_id,
                namespace,
                stream,
                payload,
                ..
            } => {
                let Some(auth_ctx) = auth_ctx.as_ref() else {
                    tracing::debug!("closing uni stream after unauthenticated publish");
                    break;
                };
                if auth_ctx.tenant_id != tenant_id {
                    tracing::debug!("closing uni stream after tenant mismatch");
                    break;
                }
                let resource = stream_resource(
                    &TenantId::new(tenant_id.as_str()),
                    &Namespace::new(namespace.as_str()),
                    &StreamName::new(stream.as_str()),
                );
                if !auth_ctx.matcher.allows(Action::StreamPublish, &resource) {
                    tracing::debug!("closing uni stream after unauthorized publish");
                    break;
                }
                let handled = handle_publish_message_uni(
                    &broker,
                    &mut stream_cache,
                    &mut stream_cache_key,
                    &publish_ctx,
                    tenant_id,
                    namespace,
                    stream,
                    payload,
                    auth_ctx.publishing_as(),
                )
                .await?;
                if !handled {
                    break;
                }
            }
            Message::PublishBatch {
                tenant_id,
                namespace,
                stream,
                payloads,
                ..
            } => {
                let Some(auth_ctx) = auth_ctx.as_ref() else {
                    tracing::debug!("closing uni stream after unauthenticated publish batch");
                    break;
                };
                if auth_ctx.tenant_id != tenant_id {
                    tracing::debug!("closing uni stream after tenant mismatch");
                    break;
                }
                let resource = stream_resource(
                    &TenantId::new(tenant_id.as_str()),
                    &Namespace::new(namespace.as_str()),
                    &StreamName::new(stream.as_str()),
                );
                if !auth_ctx.matcher.allows(Action::StreamPublish, &resource) {
                    tracing::debug!("closing uni stream after unauthorized publish batch");
                    break;
                }
                let handled = handle_publish_batch_message_uni(
                    &broker,
                    &mut stream_cache,
                    &mut stream_cache_key,
                    &publish_ctx,
                    tenant_id,
                    namespace,
                    stream,
                    payloads,
                    auth_ctx.publishing_as(),
                )
                .await?;
                if !handled {
                    break;
                }
            }
            _ => {
                t_counter!(
                    "felix_quic_protocol_violation_total",
                    "stream" => "uni"
                )
                .increment(1);
                tracing::debug!("closing uni stream after non-publish message");
                break;
            }
        }
    }
    Ok(())
}
