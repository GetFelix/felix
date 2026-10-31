//! Conditional head trims through a [`Connection`].

use std::sync::atomic::Ordering;

use anyhow::Result;
use bytes::Bytes;
use felix_wire::{Message, PublishRefusalReason};

use super::Connection;
use crate::trim::{ConditionalTrim, TrimError};

impl Connection {
    /// Hide the records of `shard` below `before`, only if the shard's next
    /// offset is exactly `expected_offset`. Afterwards a read below the floor
    /// is refused with a [`crate::SubscribeCursorError`] (`TooOld`) naming the
    /// floor, and `headers`, if given, come back on it as `trim_headers`.
    ///
    /// The trim is a record of its own and lands at `expected_offset`, so
    /// `before` can be at most that. A `before` at or below the current floor
    /// writes nothing and answers with the floor, but only when the tail
    /// matches. Needs the `stream.trim` permission, and a durable stream
    /// without atomic commits.
    ///
    /// A stale tail is an answer, not an error: [`ConditionalTrim::Refused`]
    /// carries the tail, and nothing was written. Every other refusal is a
    /// [`crate::BrokerError`]. `feature_not_enabled` means the fleet has not
    /// finalized head trims, and is not worth retrying until it has.
    /// `unacknowledged` (`trim_timed_out`) is ambiguous: the trim may have
    /// been written, and the same call again is then answered `Refused` with
    /// the tail one past it. Read the shard's earliest offset to tell the two
    /// apart.
    ///
    /// Bound to this broker: a shard led elsewhere is refused with a
    /// [`crate::NotLeaderError`], which a [`crate::Client`] follows. Fails
    /// with [`TrimError::Unsupported`], sending nothing, when the broker did
    /// not advertise [`felix_wire::FEATURE_HEAD_TRIM`].
    #[allow(clippy::too_many_arguments)]
    pub async fn trim_before(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        before: u64,
        expected_offset: u64,
        headers: Option<&[u8]>,
    ) -> Result<ConditionalTrim> {
        self.require_head_trim()?;
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::TrimBefore {
            tenant_id: tenant_id.to_owned(),
            namespace: namespace.to_owned(),
            stream: stream.to_owned(),
            shard,
            before,
            expected_offset,
            headers: headers.map(Bytes::copy_from_slice),
            request_id,
        };
        match self
            .group_round_trip(message, request_id, "trim_before refused")
            .await?
        {
            Message::TrimOk { offset, oldest, .. } => {
                Ok(ConditionalTrim::Trimmed { offset, oldest })
            }
            Message::PublishRefused {
                reason: PublishRefusalReason::OffsetMismatch { tail },
                ..
            } => Ok(ConditionalTrim::Refused { tail }),
            Message::PublishRefused {
                reason, message, ..
            } => Err(crate::PublishRefused { reason, message }.into()),
            Message::PublishError {
                message,
                code,
                retry,
                detail,
                ..
            } => Err(crate::error::refused(
                "trim_before refused",
                message,
                code,
                retry,
                detail,
            )),
            other => Err(anyhow::anyhow!(
                "unexpected answer to trim_before: {:?}",
                std::mem::discriminant(&other)
            )),
        }
    }

    fn require_head_trim(&self) -> Result<(), TrimError> {
        if felix_wire::supports_feature(self.server_features_hi, felix_wire::FEATURE_HEAD_TRIM) {
            Ok(())
        } else {
            Err(TrimError::Unsupported)
        }
    }
}
