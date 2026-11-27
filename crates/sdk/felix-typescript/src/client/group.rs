//! `Client` methods for consumer groups: polling, acknowledgement, dead letters and group administration.

use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::args::{duration_ms, parse_start, u64_of};
use crate::errors::classify;
use crate::group_admin::{GroupInfo, GroupPosition, delivery};
use crate::sharded_group::ShardedGroupHandle;
use crate::types::{GroupDelivery, GroupRecord};

use super::Client;

#[napi]
impl Client {
    /// Claim up to `maxRecords` from a consumer group, waiting up to `waitMs`
    /// for one to appear.
    ///
    /// Each record stays claimed until acked or the visibility timeout lapses,
    /// at which point it is handed to someone else — which is why `attempts`
    /// is worth reading. An empty array is an answer, not a failure: the group
    /// is owed nothing right now.
    ///
    /// The wait is a long poll rather than a spin, because the caller would
    /// otherwise write the spin: a busy loop measures the loop, and a bare
    /// `setTimeout` between polls adds latency to a record that has arrived.
    ///
    /// `visibilityMs` sets how long this poll's claims stand, instead of the
    /// broker's visibility timeout. The broker caps it, and a broker that
    /// cannot honour it refuses the poll.
    #[napi]
    pub async fn group_poll(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        max_records: Option<u32>,
        wait_ms: Option<u32>,
        consumer: Option<String>,
        reclaim: Option<bool>,
        visibility_ms: Option<f64>,
    ) -> Result<Vec<GroupRecord>> {
        let max_records = max_records.unwrap_or(32);
        let wait_ms = wait_ms.unwrap_or(0);
        let options = felix_client::GroupPollOptions {
            member: consumer.map(|consumer| felix_client::GroupMember {
                consumer,
                reclaim: reclaim.unwrap_or(false),
            }),
            wait: std::time::Duration::from_millis(u64::from(wait_ms)),
            visibility: visibility_ms
                .map(|ms| duration_ms(ms, 1.0, "visibilityMs"))
                .transpose()?,
        };
        let records = self
            .cluster()?
            .group_poll_with(
                &tenant_id,
                &namespace,
                &stream,
                shard,
                &group,
                max_records,
                &options,
            )
            .await
            .map_err(classify)?;
        Ok(records.into_iter().map(GroupRecord::from).collect())
    }

    /// Finish a record: it will not be handed out again.
    #[napi]
    pub async fn group_ack(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        offset: BigInt,
    ) -> Result<()> {
        let offset = u64_of(offset)?;
        self.cluster()?
            .group_ack(&tenant_id, &namespace, &stream, shard, &group, offset)
            .await
            .map_err(classify)
    }

    /// Return a record for redelivery without waiting out its timeout.
    /// Refused once its claim has lapsed or the record has gone out again.
    #[napi]
    pub async fn group_nack(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        record: GroupDelivery,
    ) -> Result<()> {
        let record = delivery(record)?;
        self.cluster()?
            .group_nack(&tenant_id, &namespace, &stream, shard, &group, &record)
            .await
            .map_err(classify)
    }

    /// Offsets this group gave up on after exhausting their attempts.
    #[napi]
    pub async fn group_dead_letters(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
    ) -> Result<Vec<BigInt>> {
        let offsets = self
            .cluster()?
            .group_dead_letters(&tenant_id, &namespace, &stream, shard, &group)
            .await
            .map_err(classify)?;
        Ok(offsets.into_iter().map(BigInt::from).collect())
    }

    /// Drop a dead-lettered record permanently.
    #[napi]
    pub async fn group_discard(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        offset: BigInt,
    ) -> Result<()> {
        let offset = u64_of(offset)?;
        self.cluster()?
            .group_discard(&tenant_id, &namespace, &stream, shard, &group, offset)
            .await
            .map_err(classify)
    }

    /// Put a dead-lettered record back into the group for another attempt.
    #[napi]
    pub async fn group_redrive(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        offset: BigInt,
    ) -> Result<()> {
        let offset = u64_of(offset)?;
        self.cluster()?
            .group_redrive(&tenant_id, &namespace, &stream, shard, &group, offset)
            .await
            .map_err(classify)
    }

    /// Keep the claim on `record` standing for `extendMs` from now. Resolves
    /// with how many milliseconds it now stands, which is less than asked when
    /// the broker capped it. Pass the record `groupPoll` returned: it names
    /// that delivery, and is refused once the claim has lapsed or the record
    /// has gone out again at another attempt.
    #[napi]
    pub async fn group_extend(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        record: GroupDelivery,
        extend_ms: f64,
    ) -> Result<f64> {
        let extend = duration_ms(extend_ms, 1.0, "extendMs")?;
        let record = delivery(record)?;
        let stands = self
            .cluster()?
            .group_extend(
                &tenant_id, &namespace, &stream, shard, &group, &record, extend,
            )
            .await
            .map_err(classify)?;
        Ok(stands.as_millis() as f64)
    }

    /// Move a group's cursor on one shard to `start`: `"latest"`,
    /// `"earliest"` or an offset. Records the group had handed out are void
    /// and delivered again from the new position; dead letters are kept.
    #[napi]
    pub async fn group_seek(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        start: Either<String, BigInt>,
    ) -> Result<GroupPosition> {
        let start = parse_start(Some(start))?;
        self.cluster()?
            .group_seek(&tenant_id, &namespace, &stream, shard, &group, start)
            .await
            .map(GroupPosition::from)
            .map_err(classify)
    }

    /// Create a group on one shard at `start`, so its first poll begins
    /// there. A group that already exists is left where it is and `moved` is
    /// false, so this is safe to call on every start.
    #[napi]
    pub async fn group_create(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        start: Either<String, BigInt>,
    ) -> Result<GroupPosition> {
        let start = parse_start(Some(start))?;
        self.cluster()?
            .group_create(&tenant_id, &namespace, &stream, shard, &group, start)
            .await
            .map(GroupPosition::from)
            .map_err(classify)
    }

    /// Where a group stands on one shard.
    #[napi]
    pub async fn group_describe(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
    ) -> Result<GroupInfo> {
        self.cluster()?
            .group_describe(&tenant_id, &namespace, &stream, shard, &group)
            .await
            .map(GroupInfo::from)
            .map_err(classify)
    }

    /// Delete a group on one shard: its cursor, its dead letters and the
    /// records it has handed out. Resolves with whether there was anything to
    /// delete.
    #[napi]
    pub async fn group_delete(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
    ) -> Result<bool> {
        self.cluster()?
            .group_delete(&tenant_id, &namespace, &stream, shard, &group)
            .await
            .map_err(classify)
    }

    /// Hand `record` back to be redelivered once `delayMs` has passed rather
    /// than at once, for a consumer that backs off before retrying. A delay
    /// of `0` is `groupNack`. The broker caps the delay.
    #[napi]
    pub async fn group_nack_after(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        record: GroupDelivery,
        delay_ms: f64,
    ) -> Result<()> {
        let delay = duration_ms(delay_ms, 0.0, "delayMs")?;
        let record = delivery(record)?;
        self.cluster()?
            .group_nack_after(
                &tenant_id, &namespace, &stream, shard, &group, &record, delay,
            )
            .await
            .map_err(classify)
    }

    /// Give up on one record: list it as a dead letter of the group and finish
    /// it, as the group does once a record runs out of attempts.
    /// `groupRedrive` puts it back.
    #[napi]
    pub async fn group_dead_letter(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        offset: BigInt,
    ) -> Result<()> {
        let offset = u64_of(offset)?;
        self.cluster()?
            .group_dead_letter(&tenant_id, &namespace, &stream, shard, &group, offset)
            .await
            .map_err(classify)
    }

    /// `groupSeek` on every shard of the stream, in shard order. Each entry is
    /// that shard's position.
    #[napi]
    pub async fn group_seek_stream(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        group: String,
        start: Either<String, BigInt>,
    ) -> Result<Vec<GroupPosition>> {
        let start = parse_start(Some(start))?;
        let positions = self
            .cluster()?
            .group_seek_stream(&tenant_id, &namespace, &stream, &group, start)
            .await
            .map_err(classify)?;
        Ok(positions.into_iter().map(GroupPosition::from).collect())
    }

    /// `groupCreate` on every shard of the stream. A shard where the group
    /// already exists is left alone.
    #[napi]
    pub async fn group_create_stream(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        group: String,
        start: Either<String, BigInt>,
    ) -> Result<Vec<GroupPosition>> {
        let start = parse_start(Some(start))?;
        let positions = self
            .cluster()?
            .group_create_stream(&tenant_id, &namespace, &stream, &group, start)
            .await
            .map_err(classify)?;
        Ok(positions.into_iter().map(GroupPosition::from).collect())
    }

    /// `groupDescribe` for every shard of the stream, indexed by shard.
    #[napi]
    pub async fn group_describe_stream(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        group: String,
    ) -> Result<Vec<GroupInfo>> {
        let infos = self
            .cluster()?
            .group_describe_stream(&tenant_id, &namespace, &stream, &group)
            .await
            .map_err(classify)?;
        Ok(infos.into_iter().map(GroupInfo::from).collect())
    }

    /// `groupDelete` on every shard of the stream. Resolves with whether any
    /// shard had anything to delete.
    #[napi]
    pub async fn group_delete_stream(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        group: String,
    ) -> Result<bool> {
        self.cluster()?
            .group_delete_stream(&tenant_id, &namespace, &stream, &group)
            .await
            .map_err(classify)
    }

    /// A consumer group across every shard of a stream, for a consumer that
    /// wants the whole stream through one handle.
    #[napi]
    pub async fn group_sharded(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        group: String,
    ) -> Result<ShardedGroupHandle> {
        self.cluster()?
            .group_sharded(&tenant_id, &namespace, &stream, &group)
            .await
            .map(ShardedGroupHandle::new)
            .map_err(classify)
    }
}
