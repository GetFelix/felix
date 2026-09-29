mod database;

use jsonwebtoken::Algorithm;

use super::auth::{algorithm_to_str, decode_key, parse_algorithm};
use super::codec::{
    DbStream, consistency_to_str, delivery_to_str, parse_consistency, parse_delivery,
    parse_stream_kind, stream_from_db, stream_kind_to_str,
};
use super::*;
use crate::model::StreamKind;

#[test]
fn unique_violation_detects_only_db_codes() {
    let err = sqlx::Error::RowNotFound;
    assert!(!is_unique_violation(&err));
}

#[test]
fn stream_kind_round_trip() {
    assert!(matches!(
        parse_stream_kind("Stream").unwrap(),
        StreamKind::Stream
    ));
    assert!(matches!(
        parse_stream_kind("Queue").unwrap(),
        StreamKind::Queue
    ));
    assert!(matches!(
        parse_stream_kind("Cache").unwrap(),
        StreamKind::Cache
    ));
    assert!(parse_stream_kind("Unknown").is_err());
    assert_eq!(stream_kind_to_str(&StreamKind::Stream), "Stream");
    assert_eq!(stream_kind_to_str(&StreamKind::Queue), "Queue");
    assert_eq!(stream_kind_to_str(&StreamKind::Cache), "Cache");
}

#[test]
fn consistency_round_trip() {
    assert!(matches!(
        parse_consistency("Leader").unwrap(),
        crate::model::ConsistencyLevel::Leader
    ));
    assert!(matches!(
        parse_consistency("Quorum").unwrap(),
        crate::model::ConsistencyLevel::Quorum
    ));
    assert!(parse_consistency("Unknown").is_err());
    assert_eq!(
        consistency_to_str(&crate::model::ConsistencyLevel::Leader),
        "Leader"
    );
    assert_eq!(
        consistency_to_str(&crate::model::ConsistencyLevel::Quorum),
        "Quorum"
    );
}

#[test]
fn delivery_round_trip() {
    assert!(matches!(
        parse_delivery("AtMostOnce").unwrap(),
        crate::model::DeliveryGuarantee::AtMostOnce
    ));
    assert!(matches!(
        parse_delivery("AtLeastOnce").unwrap(),
        crate::model::DeliveryGuarantee::AtLeastOnce
    ));
    assert!(parse_delivery("Unknown").is_err());
    assert_eq!(
        delivery_to_str(&crate::model::DeliveryGuarantee::AtMostOnce),
        "AtMostOnce"
    );
    assert_eq!(
        delivery_to_str(&crate::model::DeliveryGuarantee::AtLeastOnce),
        "AtLeastOnce"
    );
}

#[test]
fn stream_from_db_maps_fields() {
    let row = DbStream {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "s1".to_string(),
        kind: "Stream".to_string(),
        shards: 2,
        replication_factor: 3,
        retention_max_age_seconds: Some(3600),
        retention_max_size_bytes: Some(2048),
        consistency: "Leader".to_string(),
        delivery: "AtLeastOnce".to_string(),
        durable: true,
        region: None,
        routing: None,
    };
    let stream = stream_from_db(row).expect("stream");
    assert_eq!(stream.tenant_id, "t1");
    assert_eq!(stream.namespace, "ns");
    assert_eq!(stream.stream, "s1");
    assert!(matches!(stream.kind, StreamKind::Stream));
    assert_eq!(stream.shards, 2);
    assert_eq!(stream.replication_factor, 3);
    assert_eq!(stream.retention.max_age_seconds, Some(3600));
    assert_eq!(stream.retention.max_size_bytes, Some(2048));
    assert!(matches!(
        stream.consistency,
        crate::model::ConsistencyLevel::Leader
    ));
    assert!(matches!(
        stream.delivery,
        crate::model::DeliveryGuarantee::AtLeastOnce
    ));
    assert!(stream.durable);
}

#[test]
fn algorithm_round_trip_and_rejects_unknown() {
    assert!(matches!(
        parse_algorithm("EdDSA").unwrap(),
        Algorithm::EdDSA
    ));
    assert!(parse_algorithm("RS256").is_err());
    assert_eq!(algorithm_to_str(Algorithm::EdDSA), "EdDSA");
}

#[test]
fn algorithm_to_str_defaults_to_eddsa() {
    assert_eq!(algorithm_to_str(Algorithm::HS256), "EdDSA");
}

#[test]
fn decode_key_rejects_invalid_length() {
    let err = decode_key(&[1, 2, 3], "key").unwrap_err();
    assert!(err.to_string().contains("invalid key length"));
    let ok = decode_key(&[0u8; 32], "key").unwrap();
    assert_eq!(ok, [0u8; 32]);
}
