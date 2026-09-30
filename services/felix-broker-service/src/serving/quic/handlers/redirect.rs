//! Redirecting a request for a shard this broker does not own.

use felix_wire::Message;

use crate::serving::quic::client_error::ClientError;

/// What to answer a subscribe with, when this broker should not serve it.
///
/// `None` means serve it here: either this broker owns the shard, or it has no
/// cluster to resolve against and everything is local.
///
/// A shard this broker owns but whose lease has lapsed is refused like a
/// write: another broker may be leading it, and a reader here would follow a
/// log that one has moved past. The refusal is retryable. A shard whose
/// readers need no lease ([`ShardFence::sessions_lease_free`]) is served
/// anyway: they only ever see its committed mark.
///
/// [`ShardFence::sessions_lease_free`]: crate::shards::lifecycle::fence::ShardFence::sessions_lease_free
///
/// A redirect needs the owner's *client-facing* address, which is a different
/// listener from the one brokers forward to each other on and is known only
/// from the control plane's catalog. When the cluster has not been told one,
/// the redirect still names the owner and omits the address: "not here, and
/// here is who has it" is more use than "not here", and a client that already
/// knows that broker from discovery can act on the name alone.
// An ownership question is seven fields plus the peer's capabilities; bundling
// them would move the argument list rather than shorten it, as the publish
// handlers' allows already note.
#[allow(clippy::too_many_arguments)]
pub(crate) fn redirect_for(
    ingress: Option<&crate::shards::routing::IngressRouter>,
    client_endpoints: Option<&crate::cluster::client_endpoints::ClientEndpoints>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
    // The kind travels with every ownership question. A cache and a stream may
    // share a name, and answering for the wrong one redirects a watch to a
    // broker that does not own the key.
    kind: crate::shards::ShardKind,
    peer_features: u32,
) -> Option<Message> {
    use crate::shards::routing::dispatch;

    let key = crate::shards::ShardKey {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
        stream: stream.to_string(),
        shard,
        kind,
    };

    let dispatched = dispatch(ingress, &key);
    if let crate::shards::routing::Dispatch::Local { generation } = dispatched
        && ingress.is_some_and(|ingress| {
            !ingress.fence().lease_valid() && !ingress.fence().sessions_lease_free(&key, generation)
        })
    {
        use crate::cluster::lease::metrics;
        metrics::record_refusal(metrics::BOUNDARY_READ);
        return Some(
            ClientError::from(crate::shards::lifecycle::fence::Fenced::LeaseLapsed).into_message(),
        );
    }
    redirect_from(dispatched, client_endpoints, stream, peer_features)
}

/// [`redirect_for`], for a request already dispatched.
pub(crate) fn redirect_from(
    dispatched: crate::shards::routing::Dispatch,
    client_endpoints: Option<&crate::cluster::client_endpoints::ClientEndpoints>,
    stream: &str,
    peer_features: u32,
) -> Option<Message> {
    use crate::shards::routing::Dispatch;

    match dispatched {
        Dispatch::Local { .. } => None,
        Dispatch::Forward {
            node_id,
            generation,
            ..
        } => {
            if !felix_wire::supports_feature(peer_features, felix_wire::FEATURE_REDIRECT) {
                // A client that cannot decode `NotLeader` would lose the
                // connection to a message meant to help it. An error says the
                // same thing in a shape every client has always understood.
                return Some(
                    ClientError::new(
                        felix_wire::ErrorCode::NotLeader,
                        format!(
                            "stream {stream} is served by {node_id}; this broker does not own it"
                        ),
                    )
                    .into_message(),
                );
            }
            let addr = client_endpoints.and_then(|endpoints| endpoints.redirect_addr(&node_id));
            Some(Message::NotLeader {
                node_id,
                addr,
                generation,
            })
        }
        Dispatch::Unavailable(reason) => Some(
            ClientError::unavailable(
                &reason,
                format!("stream {stream} cannot be subscribed to right now: {reason}"),
            )
            .into_message(),
        ),
    }
}
