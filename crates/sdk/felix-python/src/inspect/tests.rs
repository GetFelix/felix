use felix_client::ShardKind;
use felix_wire::BrokerEndpoint;

use super::{Record, parse_kind};

#[test]
fn a_kind_is_a_stream_or_a_cache() {
    assert!(matches!(parse_kind("stream"), Ok(ShardKind::Stream)));
    assert!(matches!(parse_kind("cache"), Ok(ShardKind::Cache)));
    assert!(parse_kind("queue").is_err());
}

#[test]
fn a_wire_record_keeps_the_wires_field_names() {
    let record = Record::of(&BrokerEndpoint {
        node_id: "n1".into(),
        addr: "127.0.0.1:5000".into(),
    })
    .unwrap();
    assert_eq!(record.0["node_id"], "n1");
    assert_eq!(record.0["addr"], "127.0.0.1:5000");
}
