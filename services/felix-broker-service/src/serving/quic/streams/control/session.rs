//! Authenticating the control stream, and agreeing what the client understands.

use anyhow::Result;
use felix_wire::Message;

use super::responder::send_control_error;
use super::{Ctx, Session, Step};
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::publish::{
    Outgoing, handle_ack_enqueue_result, send_outgoing_critical,
};

pub(super) async fn authenticate(
    cx: &Ctx<'_>,
    session: &mut Session,
    tenant_id: String,
    token: String,
    client_flags: Option<u16>,
    client_features: Option<u32>,
) -> Result<Step> {
    let Ctx {
        broker,
        config,
        auth,
        publish_ctx,
        out_ack_tx,
        out_ack_depth,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        ..
    } = *cx;
    // Before anything is answered, so even a refused `Auth` gets a code when
    // it asked for one. A second `Auth` changes nothing: it is refused below.
    if session.auth_ctx.is_none() {
        session.error_codes.negotiate(
            client_features.unwrap_or(0),
            client_flags.unwrap_or(felix_wire::ORIGINAL_V1_FLAGS),
        );
    }
    if session.auth_ctx.is_some() {
        send_control_error(
            out_ack_tx,
            out_ack_depth,
            ack_throttle_tx,
            ack_timeout_state,
            cancel_tx,
            ClientError::invalid("auth already established"),
        )
        .await?;
        return Ok(Step::Close(false));
    }
    let peer_certs = cx.connection.peer_certificates();
    match auth
        .authenticate_peer(&tenant_id, &token, peer_certs.as_deref())
        .await
    {
        Ok(ctx) => {
            session.auth_ctx = Some(ctx);
            // Remembered, not just answered: delivery paths need to
            // know which optional frame shapes this client can read.
            // Absent means a pre-negotiation client, and the only
            // safe reading of that silence is the original bits.
            session.peer_flags = client_flags.unwrap_or(felix_wire::ORIGINAL_V1_FLAGS);
            // Which optional messages this client can decode.
            // Absent means none: a broker that guessed would send a
            // frame the client cannot parse, and an undecodable
            // frame costs the connection.
            session.peer_features = client_features.unwrap_or(0);
            // Per connection, on top of the broker-wide setting.
            session.commit_ack = config.ack_on_commit
                || felix_wire::supports_feature(
                    session.peer_features,
                    felix_wire::FEATURE_ACK_ON_COMMIT,
                );
            // A window only for a client that asked and can read the answer:
            // any other client keeps completion-order acks and its old frame.
            let publish_window = (client_flags.is_some()
                && felix_wire::supports_feature(
                    session.peer_features,
                    felix_wire::FEATURE_PUBLISH_PIPELINE,
                )
                && publish_ctx.publish_window > 0)
                .then_some(publish_ctx.publish_window);
            if let Some(window) = publish_window {
                session.ack_order.enable();
                session.publish_window = Some(std::sync::Arc::new(tokio::sync::Semaphore::new(
                    window as usize,
                )));
            }
            // Advertise our flag set only to a client that offered its
            // own. A client that sent no `client_flags` predates
            // negotiation and would not understand `AuthOk`, so it must
            // keep receiving the plain `Ok` it expects.
            let response = match client_flags {
                Some(_) => Message::AuthOk {
                    server_flags: felix_wire::KNOWN_FLAGS,
                    // Only what this broker can actually answer.
                    //
                    // The cluster-shaped features are gated on there
                    // being a cluster: a broker with no topology to
                    // report would have to refuse the question it
                    // had invited. Cache delete is not one of those
                    // -- it works the same on a single node -- so
                    // gating it too would leave every standalone
                    // broker unable to offer a request it can serve.
                    server_features: Some(
                        felix_wire::FEATURE_CACHE_DELETE
                            | felix_wire::FEATURE_ACK_ON_COMMIT
                            // Only when the cache store can observe
                            // its writes. A watch's contract is
                            // built on log offsets, so a broker
                            // whose cache has no log has nothing to
                            // anchor a resume to and must not
                            // invite one. Retained delivery rides
                            // the same machinery — the snapshot is
                            // the index the log already maintains —
                            // so the two bits travel together here.
                            | match broker.cache_watches() {
                                Some(_) => {
                                    felix_wire::FEATURE_CACHE_WATCH
                                        | felix_wire::FEATURE_CACHE_WATCH_RETAINED
                                }
                                None => 0,
                            }
                            | felix_wire::FEATURE_ATOMIC_COMMIT
                            | match publish_ctx.client_endpoints {
                                Some(_) => {
                                    felix_wire::FEATURE_TOPOLOGY
                                        | felix_wire::FEATURE_REDIRECT
                                }
                                None => 0,
                            }
                            // Only when there is somewhere to keep a
                            // group's position. Without durable
                            // storage a group would restart from the
                            // beginning on every reconnect, so
                            // offering the feature would invite work
                            // this broker cannot do.
                            | match broker.group_reader() {
                                Some(_) => {
                                    felix_wire::FEATURE_CONSUMER_GROUP
                                        | felix_wire::FEATURE_GROUP_CONSUMER
                                        | felix_wire::FEATURE_GROUP_DEAD_LETTERS
                                        | felix_wire::FEATURE_GROUP_SKIPPED
                                }
                                None => 0,
                            }
                            // Only when there is somewhere to write
                            // the counter log. A sum any restart
                            // resets is worse than refusing to
                            // count at all.
                            | match broker.counters() {
                                Some(_) => felix_wire::FEATURE_COUNTERS,
                                None => 0,
                            }
                            // Advertised unconditionally. A broker
                            // with no routing snapshot answers 1,
                            // which is the truth for a single-node
                            // deployment rather than a guess.
                            | felix_wire::FEATURE_STREAM_SHARDS
                            | felix_wire::FEATURE_CACHE_SHARDS
                            // Likewise: a single node owns every shard.
                            | felix_wire::FEATURE_SHARD_OWNERS
                            // Advertised unconditionally: the
                            // sequences live with the shard's
                            // leader, which every broker is for
                            // the shards it leads.
                            | felix_wire::FEATURE_IDEMPOTENT_PRODUCER
                            // Codes are sent only to a client that offered
                            // the bit; advertising it tells that client an
                            // error without one is not a gap in this broker.
                            | felix_wire::FEATURE_ERROR_CODES
                            // Sent only to a client that offered it, when a
                            // shard it reads moves away.
                            | felix_wire::FEATURE_SHARD_MOVED
                            // Likewise, when a subscriber's queue drops.
                            | felix_wire::FEATURE_SUBSCRIPTION_LAGGED
                            // An unknown request is answered, not fatal, for a
                            // client that offered the bit.
                            | felix_wire::FEATURE_UNSUPPORTED
                            // A reused sequence is refused, not answered as a
                            // duplicate, for a client that offered the bit.
                            | felix_wire::FEATURE_SEQUENCE_REUSED
                            // Each stream gets a window of its own.
                            | match publish_ctx.publish_window {
                                0 => 0,
                                _ => {
                                    felix_wire::FEATURE_PUBLISH_PIPELINE
                                        | felix_wire::FEATURE_STREAM_PUBLISH_WINDOW
                                }
                            },
                    ),
                    // Only when there is more than one. A single
                    // listener is the default, and saying so
                    // explicitly would change the bytes every
                    // existing deployment puts on the wire to say
                    // nothing a client does not already know.
                    listener_ports: (config.quic_listeners > 1)
                        .then(|| config.quic_binds().iter().map(|a| a.port()).collect()),
                    publish_window,
                },
                None => Message::Ok,
            };
            handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(response),
                )
                .await,
                ack_timeout_state,
                ack_throttle_tx,
                cancel_tx,
            )
            .await?;
        }
        Err(err) => {
            tracing::warn!(error = %err, "auth failed");
            send_control_error(
                out_ack_tx,
                out_ack_depth,
                ack_throttle_tx,
                ack_timeout_state,
                cancel_tx,
                ClientError::unauthenticated("auth failed"),
            )
            .await?;
            return Ok(Step::Close(false));
        }
    }
    Ok(Step::Next)
}
