//! The MQTT listener's metrics. Documented with the broker's others in
//! `docs-site/src/content/docs/features/observability.md`.

/// MQTT connections open right now. Dropping the guard closes the count.
pub(crate) struct OpenConnection;

impl OpenConnection {
    pub(crate) fn new() -> Self {
        metrics::gauge!("felix_mqtt_connections").increment(1.0);
        metrics::counter!("felix_mqtt_connections_total").increment(1);
        Self
    }
}

impl Drop for OpenConnection {
    fn drop(&mut self) {
        metrics::gauge!("felix_mqtt_connections").decrement(1.0);
    }
}

/// A connection the listener closed, or a request it would not serve. Labels
/// are a small fixed set; clients and topics are never labels.
pub(crate) fn refused(reason: &'static str) {
    metrics::counter!("felix_mqtt_refused_total", "reason" => reason).increment(1);
}

/// A message written to its stream. `qos` is `"0"` or `"1"`; for QoS 1 the
/// count is taken when the `PUBACK` is owed, not before.
pub(crate) fn published(qos: &'static str, count: u64) {
    metrics::counter!("felix_mqtt_published_total", "qos" => qos).increment(count);
}
