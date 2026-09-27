//! Per-connection subscriber counts.

use super::*;

#[test]
fn connection_subscriber_register_unregister_tracks_counts() {
    let connection_id = unique_test_connection_id();

    connection_subscriber_unregister(None);
    connection_subscriber_register(Some(connection_id));
    connection_subscriber_register(Some(connection_id));
    let map = ACTIVE_SUB_CONN_COUNTS
        .get()
        .expect("counts map should be initialized");
    let count = map
        .get(&connection_id)
        .expect("connection count should exist");
    assert_eq!(*count, 2);
    drop(count);

    connection_subscriber_unregister(Some(connection_id));
    let count = map
        .get(&connection_id)
        .expect("connection count should still exist");
    assert_eq!(*count, 1);
    drop(count);

    connection_subscriber_unregister(Some(connection_id));
    assert!(map.get(&connection_id).is_none());
}

#[test]
fn connection_subscriber_unregister_no_map_is_noop() {
    let connection_id = unique_test_connection_id();
    connection_subscriber_unregister(Some(connection_id));
}

/// No subscriber gauge is labelled by connection. A series per connection id
/// outlives the connection in the exporter, so churn grew memory and every
/// scrape without bound.
#[test]
fn subscriber_gauges_carry_no_connection_label() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let connection_id = unique_test_connection_id();
        connection_subscriber_register(Some(connection_id));
        connection_subscriber_register(Some(connection_id));
        connection_subscriber_unregister(Some(connection_id));
        connection_subscriber_unregister(Some(connection_id));
    });
    let rendered = handle.render();
    assert!(rendered.contains("felix_sub_connection_subscribers"));
    assert!(
        !rendered.contains("connection_id"),
        "a gauge is labelled per connection:\n{rendered}"
    );
}
