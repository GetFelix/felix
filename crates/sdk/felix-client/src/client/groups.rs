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
use felix_wire::{Message, StartPosition};

use super::Client;
use crate::NotLeaderError;
use crate::connection::OpenedStream;
use crate::frame_io::{read_message_with_limit, write_message};

/// A member of a consumer group that names itself when it polls. See
/// [`Client::group_poll_as`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMember {
    /// Stable across restarts of the same member, and unique within the group
    /// for the principal. 1 to 128 bytes.
    pub consumer: String,
    /// Take back the records this member holds from older connections, left
    /// by a process that restarted, before anything else. The broker honours
    /// it on the connection's first such poll and ignores it after, so it is
    /// safe to leave set.
    pub reclaim: bool,
}

/// How a poll takes records. See [`Client::group_poll_with`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupPollOptions {
    /// The member polling, or `None` for one that does not name itself. See
    /// [`Client::group_poll_as`].
    pub member: Option<GroupMember>,
    /// How long the broker may wait for work. It caps the wait.
    pub wait: std::time::Duration,
    /// How long this poll's claims stand before the records are owed to the
    /// group again. `None` is the broker's visibility timeout. The broker caps
    /// it (`FELIX_GROUP_MAX_VISIBILITY_MS`). Refused by a broker that cannot
    /// honour it, which would claim for its own timeout instead.
    pub visibility: Option<std::time::Duration>,
}

/// Where [`Client::group_seek`] or [`Client::group_create`] left a group on
/// one shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupPosition {
    /// The offset the group resumes from.
    pub offset: u64,
    /// False when [`Client::group_create`] found the group already there and
    /// left it alone.
    pub moved: bool,
}

/// Where a group stands on one shard. See [`Client::group_describe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupInfo {
    /// Everything below this is finished. `None` for a group with no cursor
    /// on the shard, which starts at the beginning of the log when polled.
    pub committed: Option<u64>,
    /// The shard's committed tail.
    pub tail: u64,
    /// Records handed out and not yet settled. Held by the shard's leader in
    /// memory, so it starts again from zero when the leader changes.
    pub in_flight: u64,
    /// Records owed again after a nack or a lapsed claim. Leader memory, like
    /// `in_flight`.
    pub owed: u64,
    /// Records the group gave up on.
    pub dead_letters: u64,
}

impl GroupInfo {
    /// Records published to the shard that the group has not finished.
    pub fn lag(&self) -> u64 {
        self.tail.saturating_sub(self.committed.unwrap_or(0))
    }
}

impl Client {
    /// Take up to `max_records` for a consumer group on one shard.
    ///
    /// An empty answer means nothing was available, not an error. Each record
    /// carries the offset to pass back to [`Client::group_ack`] or
    /// [`Client::group_nack`]; a record neither finished nor handed back is
    /// redelivered once the broker's visibility timeout lapses.
    ///
    /// A record can be polled once it is written. A publish acknowledged on
    /// enqueue (a `Leader` stream with `ack_on_commit` off) may not be yet, so
    /// a poll right after that ack can miss it; the next one gets it. Publish
    /// with [`crate::ClientConfig::ack_on_commit`] when a poll must see every
    /// acknowledged record.
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
        self.group_poll_as(
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            None,
            max_records,
            wait,
        )
        .await
    }

    /// [`Client::group_poll_wait`] as a named member of the group, or as
    /// none when `member` is `None`.
    ///
    /// The broker records the records it hands out as `member`'s, scoped to
    /// the principal this client authenticated as. A process that restarts
    /// under the same name with [`GroupMember::reclaim`] gets back the records
    /// its predecessor held, before anything else and over as many polls as it
    /// takes, instead of waiting out the visibility timeout. A member is
    /// refused by a broker that predates it, which would silently ignore the
    /// name.
    #[allow(clippy::too_many_arguments)]
    pub async fn group_poll_as(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        member: Option<&GroupMember>,
        max_records: u32,
        wait: std::time::Duration,
    ) -> Result<Vec<felix_wire::GroupRecord>> {
        let options = GroupPollOptions {
            member: member.cloned(),
            wait,
            visibility: None,
        };
        self.group_poll_with(
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            max_records,
            &options,
        )
        .await
    }

    /// Take up to `max_records` for a consumer group on one shard, as
    /// `options` says: as a named member, waiting for work, or with claims
    /// that stand longer or shorter than the broker's visibility timeout.
    #[allow(clippy::too_many_arguments)]
    pub async fn group_poll_with(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        max_records: u32,
        options: &GroupPollOptions,
    ) -> Result<Vec<felix_wire::GroupRecord>> {
        self.require_groups()?;
        let member = options.member.as_ref();
        require_member_support(self.server_features, member)?;
        if options.visibility.is_some() {
            self.require_claim_control()?;
        }
        let wait = options.wait;
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
            consumer: member.map(|member| member.consumer.clone()),
            reclaim: member.is_some_and(|member| member.reclaim),
            // At least a millisecond: zero on the wire is the broker's own.
            visibility_ms: options
                .visibility
                .map_or(0, |visibility| (visibility.as_millis() as u64).max(1)),
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
        self.settle_group(
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            offset,
            Settle::Ack,
        )
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
        self.settle_group(
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            offset,
            Settle::Nack(std::time::Duration::ZERO),
        )
        .await
    }

    /// Hand one record back, to be redelivered once `delay` has passed rather
    /// than at once. For a consumer that backs off before retrying.
    ///
    /// Until then the record holds a place under the group's in-flight cap.
    /// The broker caps the delay (`FELIX_GROUP_MAX_VISIBILITY_MS`), and the
    /// delay is the leader's memory: a failover redelivers the record sooner.
    /// A zero delay is [`Client::group_nack`].
    #[allow(clippy::too_many_arguments)]
    pub async fn group_nack_after(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
        delay: std::time::Duration,
    ) -> Result<()> {
        if !delay.is_zero() {
            self.require_claim_control()?;
        }
        self.settle_group(
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            offset,
            Settle::Nack(delay),
        )
        .await
    }

    /// Give up on one record: list it as a dead letter of the group and
    /// finish it, as the group does once a record runs out of attempts.
    /// [`Client::group_redrive`] puts it back.
    ///
    /// Refused for a record already finished, which is not listed.
    pub async fn group_dead_letter(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> Result<()> {
        self.require_claim_control()?;
        self.settle_group(
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            offset,
            Settle::DeadLetter,
        )
        .await
    }

    /// Keep the claim on `record` standing for `extend` from now, for a
    /// consumer still working on it. Returns how long it now stands, which is
    /// less than asked when the broker capped it
    /// (`FELIX_GROUP_MAX_VISIBILITY_MS`).
    ///
    /// Refused with `stale_claim` once the claim has lapsed or the record has
    /// been handed out again, even to this consumer: the record is then the
    /// group's, and finishing it is no longer this delivery's to do. An
    /// extension is the leader's memory, so a failover hands the record out
    /// again sooner.
    #[allow(clippy::too_many_arguments)]
    pub async fn group_extend(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        record: &felix_wire::GroupRecord,
        extend: std::time::Duration,
    ) -> Result<std::time::Duration> {
        self.require_claim_control()?;
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::GroupExtend {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            shard,
            group: group.to_string(),
            offset: record.offset,
            attempts: record.attempts,
            extend_ms: (extend.as_millis() as u64).max(1),
            request_id,
        };
        match self.group_round_trip(message, request_id).await? {
            Message::GroupExtended { visible_ms, .. } => {
                Ok(std::time::Duration::from_millis(visible_ms))
            }
            other => Err(anyhow::anyhow!(
                "unexpected answer to a group extend: {other:?}"
            )),
        }
    }

    /// Whether the broker serves [`Client::group_extend`],
    /// [`Client::group_nack_after`], [`Client::group_dead_letter`] and
    /// [`GroupPollOptions::visibility`].
    pub fn supports_group_claim_control(&self) -> bool {
        felix_wire::supports_feature(
            self.server_features,
            felix_wire::FEATURE_GROUP_CLAIM_CONTROL,
        )
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

    /// Move a group's cursor on one shard to `start`, backwards or forwards.
    ///
    /// `Earliest` is the oldest record the shard still holds and `Latest` its
    /// committed tail when the seek lands; an `Offset` outside those is
    /// refused. Records the group had handed out are void: an ack for one
    /// that the group now owes is refused as stale, and the record is
    /// delivered again from the new position. Dead letters are kept.
    /// Needs the `group.manage` permission.
    pub async fn group_seek(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        start: StartPosition,
    ) -> Result<GroupPosition> {
        self.seek_group(tenant_id, namespace, stream, shard, group, start, false)
            .await
    }

    /// Create a group on one shard at `start`, so its first poll begins
    /// there rather than at the beginning of the log.
    ///
    /// A group that already exists here, with a cursor or with records handed
    /// out, is left where it is, and [`GroupPosition::moved`] is false. Safe to
    /// call on every start of a consumer. Needs the `group.manage` permission.
    pub async fn group_create(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        start: StartPosition,
    ) -> Result<GroupPosition> {
        self.seek_group(tenant_id, namespace, stream, shard, group, start, true)
            .await
    }

    /// Where a group stands on one shard.
    pub async fn group_describe(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
    ) -> Result<GroupInfo> {
        self.require_group_admin()?;
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::GroupDescribe {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            shard,
            group: group.to_string(),
            request_id,
        };
        match self.group_round_trip(message, request_id).await? {
            Message::GroupInfo {
                committed,
                tail,
                in_flight,
                owed,
                dead_letters,
                ..
            } => Ok(GroupInfo {
                committed,
                tail,
                in_flight,
                owed,
                dead_letters,
            }),
            other => Err(anyhow::anyhow!(
                "unexpected answer to a group describe: {other:?}"
            )),
        }
    }

    /// Delete a group on one shard: its cursor, its dead letters, and the
    /// records it has handed out. Returns whether there was anything to
    /// delete. A consumer that polls the group again starts it afresh.
    /// Needs the `group.manage` permission.
    pub async fn group_delete(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
    ) -> Result<bool> {
        self.require_group_admin()?;
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::GroupDelete {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            shard,
            group: group.to_string(),
            request_id,
        };
        match self.group_round_trip(message, request_id).await? {
            Message::GroupDeleted { existed, .. } => Ok(existed),
            other => Err(anyhow::anyhow!(
                "unexpected answer to a group delete: {other:?}"
            )),
        }
    }

    /// Whether the broker serves [`Client::group_seek`],
    /// [`Client::group_create`], [`Client::group_describe`] and
    /// [`Client::group_delete`].
    pub fn supports_group_admin(&self) -> bool {
        felix_wire::supports_feature(self.server_features, felix_wire::FEATURE_GROUP_ADMIN)
    }

    #[allow(clippy::too_many_arguments)]
    async fn seek_group(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        start: StartPosition,
        if_new: bool,
    ) -> Result<GroupPosition> {
        self.require_group_admin()?;
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::GroupSeek {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            shard,
            group: group.to_string(),
            start,
            if_new,
            request_id,
        };
        match self.group_round_trip(message, request_id).await? {
            Message::GroupPosition { offset, moved, .. } => Ok(GroupPosition { offset, moved }),
            other => Err(anyhow::anyhow!(
                "unexpected answer to a group seek: {other:?}"
            )),
        }
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
        action: Settle,
    ) -> Result<()> {
        self.require_groups()?;
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let build =
            |tenant_id: String, namespace: String, stream: String, group: String| match action {
                Settle::Ack => Message::GroupAck {
                    tenant_id,
                    namespace,
                    stream,
                    shard,
                    group,
                    offset,
                    request_id,
                },
                Settle::Nack(delay) => Message::GroupNack {
                    tenant_id,
                    namespace,
                    stream,
                    shard,
                    group,
                    offset,
                    request_id,
                    // At least a millisecond: zero on the wire is at once.
                    delay_ms: if delay.is_zero() {
                        0
                    } else {
                        (delay.as_millis() as u64).max(1)
                    },
                },
                Settle::DeadLetter => Message::GroupDeadLetter {
                    tenant_id,
                    namespace,
                    stream,
                    shard,
                    group,
                    offset,
                    request_id,
                },
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

    fn require_claim_control(&self) -> Result<()> {
        require_claim_control(self.server_features)
    }

    fn require_group_admin(&self) -> Result<()> {
        if self.supports_group_admin() {
            return Ok(());
        }
        Err(anyhow::anyhow!(
            "this broker cannot create, move, describe or delete a consumer group",
        ))
    }
}

/// What a consumer does with a record it was handed.
#[derive(Debug, Clone, Copy)]
enum Settle {
    Ack,
    Nack(std::time::Duration),
    DeadLetter,
}

/// The request id a group answer echoes, when it carries one.
fn group_response_id(message: &Message) -> Option<u64> {
    match message {
        Message::GroupRecords { request_id, .. }
        | Message::GroupDeadLetterList { request_id, .. }
        | Message::GroupPosition { request_id, .. }
        | Message::GroupInfo { request_id, .. }
        | Message::GroupDeleted { request_id, .. }
        | Message::GroupExtended { request_id, .. }
        | Message::ProducerInitOk { request_id, .. }
        | Message::CommitOk { request_id, .. }
        | Message::StateValue { request_id, .. }
        | Message::OffsetValue { request_id, .. }
        | Message::CacheOk { request_id } => Some(*request_id),
        _ => None,
    }
}

/// Refuse a named poll to a broker without `FEATURE_GROUP_CONSUMER`. It would
/// ignore the name, so a reclaim would silently do nothing.
fn require_member_support(server_features: u32, member: Option<&GroupMember>) -> Result<()> {
    if member.is_some()
        && !felix_wire::supports_feature(server_features, felix_wire::FEATURE_GROUP_CONSUMER)
    {
        anyhow::bail!("this broker does not record which member holds a group's records");
    }
    Ok(())
}

/// Refuse claim control to a broker without `FEATURE_GROUP_CLAIM_CONTROL`.
/// Checked for the new fields too, not only the new requests: an older broker
/// ignores `delay_ms` and `visibility_ms` and says nothing.
fn require_claim_control(server_features: u32) -> Result<()> {
    if felix_wire::supports_feature(server_features, felix_wire::FEATURE_GROUP_CLAIM_CONTROL) {
        return Ok(());
    }
    Err(anyhow::anyhow!(
        "this broker cannot extend a claim, delay a nack, dead-letter a record, or claim for a chosen time",
    ))
}

#[cfg(test)]
mod tests;
