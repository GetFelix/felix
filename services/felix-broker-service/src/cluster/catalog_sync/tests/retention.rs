//! A stream's retention as the control plane sends it.

use std::time::Duration;

use super::super::wire::Stream;

fn stream(json: &str) -> Stream {
    serde_json::from_str(json).expect("stream")
}

#[test]
fn a_streams_retention_reaches_its_metadata() {
    let metadata = stream(
        r#"{"tenant_id":"t","namespace":"ns","stream":"s","shards":1,"durable":true,
            "retention":{"max_age_seconds":3600,"max_size_bytes":1048576}}"#,
    )
    .metadata()
    .expect("metadata");
    assert_eq!(metadata.retention.bytes, Some(1_048_576));
    assert_eq!(metadata.retention.age, Some(Duration::from_secs(3600)));
}

/// A control plane that predates it sends none, and a zero stored before
/// the control plane refused it is no bound rather than a failed sync.
#[test]
fn absent_or_zero_retention_leaves_the_brokers_bounds() {
    for json in [
        r#"{"tenant_id":"t","namespace":"ns","stream":"s","shards":1,"durable":true}"#,
        r#"{"tenant_id":"t","namespace":"ns","stream":"s","shards":1,"durable":true,
            "retention":{"max_age_seconds":0,"max_size_bytes":null}}"#,
    ] {
        let metadata = stream(json).metadata().expect("metadata");
        assert_eq!(metadata.retention, felix_storage::log::Retention::default());
    }
}
