//! The Kafka listener's metrics. Documented with the broker's others in
//! `docs-site/src/content/docs/features/observability.md`.
//!
//! Labels are API names and error codes, both small fixed sets. Topics and
//! tenants are deliberately not labels: that would be a series per stream.

use kafka_protocol::messages::ApiKey;

/// Kafka connections open right now.
pub(crate) fn connection_opened() {
    metrics::gauge!("felix_kafka_connections").increment(1.0);
    metrics::counter!("felix_kafka_connections_total").increment(1);
}

pub(crate) fn connection_closed() {
    metrics::gauge!("felix_kafka_connections").decrement(1.0);
}

/// One answered request. `error` is the Kafka error code the answer carried at
/// its top level or, for per-partition answers, the first non-zero one.
pub(crate) fn request(api: ApiKey, error: i16) {
    metrics::counter!(
        "felix_kafka_requests_total",
        "api" => api_name(api),
        "error" => error_name(error),
    )
    .increment(1);
}

/// A request the listener would not serve, and closed the connection over.
pub(crate) fn refused(reason: &'static str) {
    metrics::counter!("felix_kafka_refused_total", "reason" => reason).increment(1);
}

pub(crate) fn fetched(records: u64, bytes: u64) {
    metrics::counter!("felix_kafka_fetch_records_total").increment(records);
    metrics::counter!("felix_kafka_fetch_bytes_total").increment(bytes);
}

/// A fetch that had too little to return and waited. `outcome` is `data` when
/// an append woke it, `timeout` when `max_wait_ms` ran out first.
pub(crate) fn long_poll(outcome: &'static str, waited: std::time::Duration) {
    metrics::counter!("felix_kafka_fetch_waits_total", "outcome" => outcome).increment(1);
    metrics::histogram!("felix_kafka_fetch_wait_seconds").record(waited.as_secs_f64());
}

/// Records a produce wrote, and their payload bytes. A duplicate is not
/// counted here: nothing was written.
pub(crate) fn produced(records: u64, bytes: u64) {
    metrics::counter!("felix_kafka_produce_records_total").increment(records);
    metrics::counter!("felix_kafka_produce_bytes_total").increment(bytes);
}

/// Records of an idempotent batch that was already in the log, answered
/// without writing them again.
pub(crate) fn duplicate(records: u64) {
    metrics::counter!("felix_kafka_produce_duplicate_records_total").increment(records);
}

/// Record keys and headers dropped because the listener has not opted in to
/// storing them or the broker does not accept record headers.
pub(crate) fn dropped(keys: u64, headers: u64) {
    if keys > 0 {
        metrics::counter!("felix_kafka_produce_dropped_total", "field" => "key").increment(keys);
    }
    if headers > 0 {
        metrics::counter!("felix_kafka_produce_dropped_total", "field" => "headers")
            .increment(headers);
    }
}

/// A partition of a produce that was refused, by Kafka error. Counted per
/// partition: `felix_kafka_requests_total` keeps only a request's first error.
pub(crate) fn produce_refused(error: i16) {
    metrics::counter!("felix_kafka_produce_errors_total", "error" => error_name(error))
        .increment(1);
}

fn api_name(api: ApiKey) -> &'static str {
    match api {
        ApiKey::ApiVersions => "ApiVersions",
        ApiKey::SaslHandshake => "SaslHandshake",
        ApiKey::SaslAuthenticate => "SaslAuthenticate",
        ApiKey::Metadata => "Metadata",
        ApiKey::ListOffsets => "ListOffsets",
        ApiKey::Fetch => "Fetch",
        ApiKey::Produce => "Produce",
        ApiKey::FindCoordinator => "FindCoordinator",
        ApiKey::JoinGroup => "JoinGroup",
        ApiKey::SyncGroup => "SyncGroup",
        ApiKey::Heartbeat => "Heartbeat",
        ApiKey::LeaveGroup => "LeaveGroup",
        ApiKey::OffsetCommit => "OffsetCommit",
        ApiKey::OffsetFetch => "OffsetFetch",
        ApiKey::InitProducerId => "InitProducerId",
        ApiKey::AddPartitionsToTxn => "AddPartitionsToTxn",
        ApiKey::AddOffsetsToTxn => "AddOffsetsToTxn",
        ApiKey::EndTxn => "EndTxn",
        ApiKey::TxnOffsetCommit => "TxnOffsetCommit",
        _ => "other",
    }
}

fn error_name(code: i16) -> &'static str {
    use kafka_protocol::ResponseError as E;
    match E::try_from_code(code) {
        None => "none",
        Some(E::UnsupportedVersion) => "unsupported_version",
        Some(E::OffsetOutOfRange) => "offset_out_of_range",
        Some(E::UnknownTopicOrPartition) => "unknown_topic_or_partition",
        Some(E::NotLeaderOrFollower) => "not_leader_or_follower",
        Some(E::LeaderNotAvailable) => "leader_not_available",
        Some(E::TopicAuthorizationFailed) => "topic_authorization_failed",
        Some(E::GroupAuthorizationFailed) => "group_authorization_failed",
        Some(E::SaslAuthenticationFailed) => "sasl_authentication_failed",
        Some(E::UnsupportedSaslMechanism) => "unsupported_sasl_mechanism",
        Some(E::IllegalSaslState) => "illegal_sasl_state",
        Some(E::KafkaStorageError) => "kafka_storage_error",
        Some(E::UnknownTopicId) => "unknown_topic_id",
        Some(E::InvalidRequest) => "invalid_request",
        Some(E::PolicyViolation) => "policy_violation",
        Some(E::CorruptMessage) => "corrupt_message",
        Some(E::MessageTooLarge) => "message_too_large",
        Some(E::RequestTimedOut) => "request_timed_out",
        Some(E::InvalidRecord) => "invalid_record",
        Some(E::UnsupportedForMessageFormat) => "unsupported_for_message_format",
        Some(E::OutOfOrderSequenceNumber) => "out_of_order_sequence_number",
        Some(E::DuplicateSequenceNumber) => "duplicate_sequence_number",
        Some(E::UnknownProducerId) => "unknown_producer_id",
        Some(E::TransactionalIdAuthorizationFailed) => "transactional_id_authorization_failed",
        Some(E::ClusterAuthorizationFailed) => "cluster_authorization_failed",
        Some(E::UnknownServerError) => "unknown_server_error",
        Some(_) => "other",
    }
}
