//! `InitProducerId`: a producer id for an idempotent producer.
//!
//! The sequences an id numbers live in each shard's log (see `felix-broker`'s
//! `publish/per_record.rs`), so no broker has to remember having issued it:
//! any leader of a shard, now or after a failover, checks a batch against what
//! its log holds. The answer's epoch is 0.
//!
//! A Kafka producer is a producer id and an epoch. A Java producer bumps the
//! epoch itself (KIP-360) and starts its sequences over at 0 under the same
//! id, so the log keys a Kafka producer by both: [`felix_producer`] packs them
//! into one Felix producer id. A new epoch is then a producer the log has not
//! seen, and the sequences of the old one stay where they were. See
//! `docs/kafka-compatibility.md`, "Producer epochs".
//!
//! A transactional id is refused; see `transactions`.

use anyhow::Result;
use bytes::Bytes;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::{InitProducerIdRequest, InitProducerIdResponse, ProducerId};

use crate::cluster::Principal;
use crate::service::Shared;

pub(super) fn answer(
    shared: &Shared,
    principal: Option<&Principal>,
    request: InitProducerIdRequest,
    version: i16,
) -> Result<(Bytes, i16)> {
    let refused = |error: ResponseError| {
        let response = InitProducerIdResponse::default()
            .with_error_code(error.code())
            .with_producer_id(ProducerId(-1))
            .with_producer_epoch(-1);
        super::encode(&response, version, error.code())
    };
    if request
        .transactional_id
        .as_ref()
        .is_some_and(|id| !id.is_empty())
    {
        tracing::info!("refused a kafka transactional producer: Felix has no transactions");
        return refused(super::transactions::REFUSED);
    }
    if principal.is_none() {
        return refused(ResponseError::ClusterAuthorizationFailed);
    }
    // Random, as Felix ids are, cut to the bits a packed id keeps.
    let id = (shared.broker.new_producer_id() & KAFKA_ID_MASK).max(1) as i64;
    let response = InitProducerIdResponse::default()
        .with_producer_id(ProducerId(id))
        .with_producer_epoch(0);
    super::encode(&response, version, 0)
}

/// The bits of a Kafka producer id that [`felix_producer`] keeps. The ids
/// `InitProducerId` hands out fit; another id is folded into them.
pub(crate) const KAFKA_ID_MASK: u64 = (1 << 47) - 1;

/// Marks a Felix producer id as packed from a Kafka id and epoch, and keeps
/// it from being zero.
const KAFKA_PRODUCER: u64 = 1 << 63;

/// One more than the highest epoch: the slot [`producer_family`] takes.
const EPOCH_SLOTS: u64 = 1 << 15;

/// The Felix producer id that Kafka producer `id` at `epoch` writes as.
/// `epoch` is not negative.
pub(crate) fn felix_producer(id: i64, epoch: i16) -> u64 {
    debug_assert!(epoch >= 0);
    KAFKA_PRODUCER | ((id as u64 & KAFKA_ID_MASK) << 16) | epoch as u16 as u64
}

/// Every epoch of Kafka producer `id`, as Felix producer ids.
pub(crate) fn producer_epochs(id: i64) -> std::ops::RangeInclusive<u64> {
    felix_producer(id, 0)..=felix_producer(id, i16::MAX)
}

/// A key no epoch of `id` writes under, whose turn serialises all of them.
pub(crate) fn producer_family(id: i64) -> u64 {
    felix_producer(id, 0) | EPOCH_SLOTS
}
