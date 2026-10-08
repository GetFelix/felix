//! What a broker tells an operator about itself: its view of one shard, and
//! the subscriptions it serves. Both need `node.view:cluster:*` and are asked
//! only of a broker that advertised [`felix_wire::FEATURE_INSPECT`].

use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_wire::{InspectedSubscription, Message, SubscriptionCursor, SubscriptionFilter};

use super::Client;
use crate::connection::OpenedStream;
use crate::frame_io::{read_message_with_limit, write_message};

/// One page of [`Client::list_subscriptions`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionsPage {
    /// The broker that answered. Empty on a broker that is not in a cluster.
    pub node_id: String,
    pub subscriptions: Vec<InspectedSubscription>,
    /// Pass this back for the next page. `None` on the last one.
    pub next_cursor: Option<SubscriptionCursor>,
}

impl Client {
    /// Whether this broker answers [`Client::inspect_shard`] and
    /// [`Client::list_subscriptions`].
    pub fn supports_inspect(&self) -> bool {
        felix_wire::supports_feature(self.server_features_hi, felix_wire::FEATURE_INSPECT)
    }

    /// This broker's own view of one shard: its phase and generation, the
    /// fence, its lease and tail, and, where it leads, every replica's
    /// position. Not forwarded: ask each broker for its own.
    ///
    /// Needs `node.view:cluster:*`, and may name any tenant. Errors when the
    /// broker did not advertise [`felix_wire::FEATURE_INSPECT`]; check
    /// [`Client::supports_inspect`] first to tell that apart.
    pub async fn inspect_shard(
        &self,
        kind: crate::ShardKind,
        tenant_id: &str,
        namespace: &str,
        name: &str,
        shard: u32,
    ) -> Result<felix_wire::ShardInspection> {
        if !self.supports_inspect() {
            anyhow::bail!("broker does not support inspect");
        }
        let request_id = self
            .cache_request_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let answer = self
            .ask_once(
                Message::ShardInspect {
                    tenant_id: tenant_id.to_string(),
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                    kind,
                    shard,
                    request_id,
                },
                "shard inspect",
            )
            .await?;
        match answer {
            Message::ShardInspectInfo { view, .. } => Ok(*view),
            other => Err(not_answered(
                "shard inspect",
                "shard inspect refused",
                other,
            )),
        }
    }

    /// One page of the subscriptions this broker serves: each one's queue
    /// depth and capacity, overflow policy, records dropped, position against
    /// the shard's tail, connection and principal. Not forwarded: ask each
    /// broker for its own.
    ///
    /// Needs `node.view:cluster:*`. The broker caps a page at 1000 and answers
    /// 100 when `limit` is `None`.
    pub async fn list_subscriptions(
        &self,
        filter: SubscriptionFilter,
        limit: Option<u32>,
        cursor: Option<SubscriptionCursor>,
    ) -> Result<SubscriptionsPage> {
        if !self.supports_inspect() {
            anyhow::bail!("broker does not support inspect");
        }
        let request_id = self
            .cache_request_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let answer = self
            .ask_once(
                Message::SubscriptionsList {
                    filter: Box::new(filter),
                    limit,
                    cursor,
                    request_id,
                },
                "subscriptions list",
            )
            .await?;
        match answer {
            Message::SubscriptionsListInfo {
                node_id,
                subscriptions,
                next_cursor,
                ..
            } => Ok(SubscriptionsPage {
                node_id,
                subscriptions,
                next_cursor,
            }),
            other => Err(not_answered(
                "subscriptions list",
                "subscriptions list refused",
                other,
            )),
        }
    }

    /// Send one request on a fresh stream and read its one answer.
    async fn ask_once(&self, request: Message, what: &'static str) -> Result<Message> {
        let OpenedStream {
            mut send,
            mut recv,
            lease: _lease,
            ..
        } = self.open_event_stream().await?;
        write_message(&mut send, request)
            .await
            .with_context(|| format!("send {what} request"))?;
        let mut scratch = BytesMut::with_capacity(4 * 1024);
        let answer =
            read_message_with_limit(&mut recv, &mut scratch, self.runtime_config.max_frame_bytes)
                .await?;
        let _ = send.finish();
        answer.ok_or_else(|| anyhow::anyhow!("{what} response missing"))
    }
}

/// The error for an answer that is not the one asked for: a refusal, a
/// broker that does not know the request, or something unexpected.
fn not_answered(what: &'static str, refusal: &'static str, answer: Message) -> anyhow::Error {
    match answer {
        Message::Error {
            message,
            code,
            retry,
            detail,
        } => crate::error::refused(refusal, message, code, retry, detail),
        Message::Unsupported { request_type, .. } => {
            anyhow::anyhow!("broker does not support {request_type}")
        }
        other => anyhow::anyhow!("unexpected {what} response: {other:?}"),
    }
}
