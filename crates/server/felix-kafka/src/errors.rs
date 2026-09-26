//! Felix errors as Kafka error codes.
//!
//! Chosen for what a Kafka client does next, since that is all a code
//! controls: `NOT_LEADER_OR_FOLLOWER` makes it refresh metadata and go to the
//! new leader, `OFFSET_OUT_OF_RANGE` makes it apply its reset policy, and
//! `UNKNOWN_TOPIC_OR_PARTITION` makes it report the topic as missing, and the
//! sequence errors steer an idempotent producer.

use felix_broker::BrokerError;
use kafka_protocol::ResponseError;

/// The Kafka error for a broker error met while reading or writing a
/// partition.
pub(crate) fn from_broker(err: &BrokerError) -> ResponseError {
    match err {
        BrokerError::CursorTooOld { .. } | BrokerError::CursorInFuture { .. } => {
            ResponseError::OffsetOutOfRange
        }
        BrokerError::StreamNotFound { .. }
        | BrokerError::TenantNotFound(_)
        | BrokerError::NamespaceNotFound { .. }
        | BrokerError::StreamNotDurable { .. } => ResponseError::UnknownTopicOrPartition,
        // The shard closed under the read: it is moving, or this broker
        // stopped leading it. A metadata refresh finds where it went.
        BrokerError::StreamHandleInactive(_) => ResponseError::NotLeaderOrFollower,
        BrokerError::Storage(_) | BrokerError::DurableStorageNotConfigured { .. } => {
            ResponseError::KafkaStorageError
        }
        // An idempotent producer's refusals, as librdkafka reads them. A
        // gap is fatal to the producer, since something it believes written
        // is not. An unknown producer makes it take a new id and start its
        // sequences over. A duplicate older than the log remembers was
        // written, so librdkafka counts it delivered.
        BrokerError::SequenceGap { .. } => ResponseError::OutOfOrderSequenceNumber,
        BrokerError::UnknownProducer { .. } => ResponseError::UnknownProducerId,
        BrokerError::SequenceExpired { .. } => ResponseError::DuplicateSequenceNumber,
        // None can come from reading or writing records; answered as a
        // server fault rather than guessed at.
        BrokerError::CapacityTooLarge
        | BrokerError::DurabilityChangeRequiresRecreate { .. }
        | BrokerError::GroupOffsetNotHandedOut { .. } => ResponseError::UnknownServerError,
    }
}

#[cfg(test)]
mod tests;
