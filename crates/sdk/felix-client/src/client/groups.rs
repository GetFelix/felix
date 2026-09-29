//! Consumer groups and their dead letters, through a [`Client`].
//!
//! Every request here is a single exchange on a stream of its own; see
//! [`Client::group_round_trip`].
//!
//! Only the shard's leader serves its groups. Any other broker, including the
//! old leader once a shard move cuts over, answers with [`NotLeaderError`],
//! which these calls return rather than follow: a `Client` is one broker's
//! connections. The same calls on [`crate::ClusterClient`] follow it.

use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_wire::Message;

use super::Client;
use crate::NotLeaderError;
use crate::connection::OpenedStream;
use crate::frame_io::{read_message_with_limit, write_message};

impl Client {
    /// Take up to `max_records` for a consumer group on one shard.
    ///
    /// An empty answer means nothing was available, not an error. Each record
    /// carries the offset to pass back to [`Client::group_ack`] or
    /// [`Client::group_nack`]; a record neither finished nor handed back is
    /// redelivered once the broker's visibility timeout lapses.
    ///
    /// Only the broker that leads the shard can serve its groups, because the
    /// claim and the acknowledgement have to reach the same place. Polling any
    /// other broker is refused rather than answered emptily.
    pub async fn group_poll(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        max_records: u32,
    ) -> Result<Vec<felix_wire::GroupRecord>> {
        self.group_poll_wait(
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            max_records,
            std::time::Duration::ZERO,
        )
        .await
    }

    /// [`Client::group_poll`], but the broker may hold the request open for up
    /// to `wait` waiting for work.
    ///
    /// An empty answer still means nothing was available — the wait bounds how
    /// long the broker looks, not whether it answers. The broker caps the wait,
    /// so asking for an hour does not get one.
    ///
    /// This is how a consumer idles without spinning: one request that waits
    /// costs one round trip, where repeated immediate polls cost one each.
    #[allow(clippy::too_many_arguments)]
    pub async fn group_poll_wait(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        max_records: u32,
        wait: std::time::Duration,
    ) -> Result<Vec<felix_wire::GroupRecord>> {
        self.require_groups()?;
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::GroupPoll {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            shard,
            group: group.to_string(),
            max_records,
            wait_ms: wait.as_millis() as u64,
            request_id,
        };
        match self.group_round_trip(message, request_id).await? {
            Message::GroupRecords { records, .. } => Ok(records),
            other => Err(anyhow::anyhow!(
                "unexpected answer to a group poll: {other:?}"
            )),
        }
    }

    /// Finish one record. Everything below the group's cursor stays finished.
    pub async fn group_ack(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> Result<()> {
        self.settle_group(tenant_id, namespace, stream, shard, group, offset, true)
            .await
    }

    /// Hand one record back without finishing it. It is redelivered at once
    /// rather than after the visibility timeout.
    pub async fn group_nack(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> Result<()> {
        self.settle_group(tenant_id, namespace, stream, shard, group, offset, false)
            .await
    }

    /// Offsets this group gave up on, lowest first.
    ///
    /// The records are still in the stream's log at these offsets, readable by
    /// an ordinary replay — this is a list of what to look at, not a copy of it.
    pub async fn group_dead_letters(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
    ) -> Result<Vec<u64>> {
        self.require_dead_letters()?;
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::GroupDeadLetters {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            shard,
            group: group.to_string(),
            request_id,
        };
        match self.group_round_trip(message, request_id).await? {
            Message::GroupDeadLetterList { offsets, .. } => Ok(offsets),
            other => Err(anyhow::anyhow!(
                "unexpected answer to a dead-letter list: {other:?}"
            )),
        }
    }

    /// Stop tracking one dead letter, having decided the record is not worth
    /// reprocessing. The record itself is untouched.
    pub async fn group_discard(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> Result<()> {
        self.manage_dead_letter(tenant_id, namespace, stream, shard, group, offset, false)
            .await
    }

    /// Put one dead letter back in the queue, its attempt count reset.
    ///
    /// For when the reason it failed has been fixed. The group's cursor does
    /// not move backwards: everything it finished stays finished, and only this
    /// record is delivered again.
    pub async fn group_redrive(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> Result<()> {
        self.manage_dead_letter(tenant_id, namespace, stream, shard, group, offset, true)
            .await
    }

    /// One group request on a stream of its own.
    ///
    /// Not the cache workers' streams: those are pipelined against a response
    /// shape that is always `CacheValue` or `CacheOk`, and a `GroupRecords`
    /// arriving there would be matched against the wrong request. Same reason
    /// `topology` opens its own.
    pub(super) async fn group_round_trip(
        &self,
        message: Message,
        request_id: u64,
    ) -> Result<Message> {
        let OpenedStream {
            mut send,
            mut recv,
            lease: _lease,
            ..
        } = self.open_event_stream().await?;
        write_message(&mut send, message)
            .await
            .context("send group request")?;
        let mut scratch = BytesMut::with_capacity(64 * 1024);
        let answer =
            read_message_with_limit(&mut recv, &mut scratch, self.runtime_config.max_frame_bytes)
                .await?;
        let _ = send.finish();
        match answer {
            Some(Message::Error {
                message,
                code,
                retry,
                detail,
            }) => Err(crate::error::refused(
                "group request refused",
                message,
                code,
                retry,
                detail,
            )),
            // Typed, so a caller can follow it: only the shard's leader holds
            // its groups, and this names which broker that is.
            Some(Message::NotLeader {
                node_id,
                addr,
                generation,
            }) => Err(NotLeaderError {
                node_id,
                addr,
                generation,
            }
            .into()),
            Some(other) => {
                // The exchange is one request on one stream, so an answer
                // carrying a different id belongs to nothing this sent.
                if let Some(id) = group_response_id(&other)
                    && id != request_id
                {
                    return Err(anyhow::anyhow!(
                        "group answer carried request id {id}, expected {request_id}",
                    ));
                }
                Ok(other)
            }
            None => Err(anyhow::anyhow!("the broker closed the group stream")),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn settle_group(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
        finish: bool,
    ) -> Result<()> {
        self.require_groups()?;
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let build = |tenant_id: String, namespace: String, stream: String, group: String| {
            if finish {
                Message::GroupAck {
                    tenant_id,
                    namespace,
                    stream,
                    shard,
                    group,
                    offset,
                    request_id,
                }
            } else {
                Message::GroupNack {
                    tenant_id,
                    namespace,
                    stream,
                    shard,
                    group,
                    offset,
                    request_id,
                }
            }
        };
        let message = build(
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            group.to_string(),
        );
        match self.group_round_trip(message, request_id).await? {
            Message::CacheOk { .. } => Ok(()),
            other => Err(anyhow::anyhow!(
                "unexpected answer to a group settle: {other:?}"
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn manage_dead_letter(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
        redrive: bool,
    ) -> Result<()> {
        self.require_dead_letters()?;
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = if redrive {
            Message::GroupRedrive {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
                shard,
                group: group.to_string(),
                offset,
                request_id,
            }
        } else {
            Message::GroupDiscard {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
                shard,
                group: group.to_string(),
                offset,
                request_id,
            }
        };
        match self.group_round_trip(message, request_id).await? {
            Message::CacheOk { .. } => Ok(()),
            other => Err(anyhow::anyhow!(
                "unexpected answer to a dead-letter change: {other:?}"
            )),
        }
    }

    fn require_groups(&self) -> Result<()> {
        if felix_wire::supports_feature(self.server_features, felix_wire::FEATURE_CONSUMER_GROUP) {
            return Ok(());
        }
        // Refused here rather than sent. An unrecognised message type ends the
        // broker's control loop, so probing one that predates this costs the
        // connection instead of returning an error.
        Err(anyhow::anyhow!(
            "this broker does not serve consumer groups",
        ))
    }

    fn require_dead_letters(&self) -> Result<()> {
        if felix_wire::supports_feature(
            self.server_features,
            felix_wire::FEATURE_GROUP_DEAD_LETTERS,
        ) {
            return Ok(());
        }
        Err(anyhow::anyhow!("this broker does not serve dead letters",))
    }
}

/// The request id a group answer echoes, when it carries one.
fn group_response_id(message: &Message) -> Option<u64> {
    match message {
        Message::GroupRecords { request_id, .. }
        | Message::GroupDeadLetterList { request_id, .. }
        | Message::ProducerInitOk { request_id, .. }
        | Message::CommitOk { request_id, .. }
        | Message::StateValue { request_id, .. }
        | Message::CacheOk { request_id } => Some(*request_id),
        _ => None,
    }
}
