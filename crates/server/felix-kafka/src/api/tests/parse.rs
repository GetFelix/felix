//! Decoder robustness over the committed seeds (`fuzz/seeds/`): each seed decodes, and every truncation and single-byte
//! corruption of one is refused without a panic.

use bytes::Bytes;
use kafka_protocol::messages::ApiKey;

use crate::api::{Answer, Body, Parsed, parse};
use crate::records::decode::decode;

const REQUESTS: &[(&str, &[u8])] = &[
    (
        "api_versions",
        include_bytes!("../../../fuzz/seeds/kafka_request/api_versions"),
    ),
    (
        "fetch",
        include_bytes!("../../../fuzz/seeds/kafka_request/fetch"),
    ),
    (
        "find_coordinator",
        include_bytes!("../../../fuzz/seeds/kafka_request/find_coordinator"),
    ),
    (
        "init_producer_id",
        include_bytes!("../../../fuzz/seeds/kafka_request/init_producer_id"),
    ),
    (
        "metadata_all",
        include_bytes!("../../../fuzz/seeds/kafka_request/metadata_all"),
    ),
    (
        "metadata_one",
        include_bytes!("../../../fuzz/seeds/kafka_request/metadata_one"),
    ),
    (
        "produce",
        include_bytes!("../../../fuzz/seeds/kafka_request/produce"),
    ),
    (
        "produce_gzip",
        include_bytes!("../../../fuzz/seeds/kafka_request/produce_gzip"),
    ),
    (
        "produce_idempotent",
        include_bytes!("../../../fuzz/seeds/kafka_request/produce_idempotent"),
    ),
    (
        "sasl_handshake",
        include_bytes!("../../../fuzz/seeds/kafka_request/sasl_handshake"),
    ),
];

const RECORDS: &[(&str, &[u8])] = &[
    (
        "gzip",
        include_bytes!("../../../fuzz/seeds/kafka_records/gzip"),
    ),
    (
        "idempotent",
        include_bytes!("../../../fuzz/seeds/kafka_records/idempotent"),
    ),
    (
        "plain",
        include_bytes!("../../../fuzz/seeds/kafka_records/plain"),
    ),
    (
        "two_batches",
        include_bytes!("../../../fuzz/seeds/kafka_records/two_batches"),
    ),
];

#[test]
fn every_request_seed_decodes() {
    for (name, seed) in REQUESTS {
        let parsed =
            parse(Bytes::from_static(seed)).unwrap_or_else(|err| panic!("{name}: {err:#}"));
        let Parsed::Request(request) = parsed else {
            panic!("{name}: answered without decoding");
        };
        if let Body::Produce(produce) = request.body {
            assert_eq!(request.api, ApiKey::Produce);
            for topic in produce.topic_data {
                for partition in topic.partition_data {
                    let records = partition.records.expect("seed carries records");
                    decode(records).unwrap_or_else(|err| panic!("{name}: {err:?}"));
                }
            }
        }
    }
}

#[test]
fn every_record_seed_decodes() {
    for (name, seed) in RECORDS {
        let batches =
            decode(Bytes::from_static(seed)).unwrap_or_else(|err| panic!("{name}: {err:?}"));
        assert!(!batches.is_empty(), "{name}");
    }
}

#[test]
fn a_frame_shorter_than_its_header_is_an_error_not_a_panic() {
    for len in 0..8 {
        assert!(parse(Bytes::from(vec![0u8; len])).is_err(), "{len} bytes");
    }
}

#[test]
fn an_unknown_api_key_closes() {
    let frame = Bytes::from_static(&[0x7f, 0x7f, 0, 0, 0, 0, 0, 1]);
    assert!(matches!(parse(frame), Ok(Parsed::Answer(Answer::Close(_)))));
}

#[test]
fn truncated_and_corrupted_seeds_never_panic() {
    for (_, seed) in REQUESTS {
        for variant in mutations(seed) {
            let _ = parse(variant);
        }
    }
    for (_, seed) in RECORDS {
        for variant in mutations(seed) {
            let _ = decode(variant);
        }
    }
}

/// Every prefix of `seed`, and `seed` with each byte flipped.
fn mutations(seed: &[u8]) -> Vec<Bytes> {
    let mut out: Vec<Bytes> = (0..seed.len())
        .map(|len| Bytes::copy_from_slice(&seed[..len]))
        .collect();
    for at in 0..seed.len() {
        let mut flipped = seed.to_vec();
        flipped[at] ^= 0xff;
        out.push(Bytes::from(flipped));
    }
    out
}
