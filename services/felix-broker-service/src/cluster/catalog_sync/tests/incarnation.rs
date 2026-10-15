//! A stream's incarnation as the control plane sends it.

use super::super::wire::Stream;

fn metadata(json: &str) -> felix_broker::StreamMetadata {
    serde_json::from_str::<Stream>(json)
        .expect("stream")
        .metadata()
        .expect("metadata")
}

#[test]
fn a_streams_incarnation_reaches_its_metadata() {
    let metadata = metadata(
        r#"{"tenant_id":"t","namespace":"ns","stream":"s","shards":1,"durable":true,
            "incarnation":4503599627370495}"#,
    );
    assert_eq!(metadata.incarnation, 4_503_599_627_370_495);
}

/// A control plane that predates incarnations, or a stream created before the
/// fleet finalized them, sends none.
#[test]
fn an_absent_incarnation_is_zero() {
    let metadata =
        metadata(r#"{"tenant_id":"t","namespace":"ns","stream":"s","shards":1,"durable":true}"#);
    assert_eq!(metadata.incarnation, 0);
}
