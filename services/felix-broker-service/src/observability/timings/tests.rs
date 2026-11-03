use super::*;
use serial_test::serial;

// The collector is process-global, so every test that toggles it or records
// into it runs under `#[serial]`, the same lock the quic stream tests take.

#[test]
#[serial]
fn enable_collection_does_not_panic() {
    enable_collection(10);
}

#[test]
#[serial]
fn set_enabled_does_not_panic() {
    set_enabled(true);
    set_enabled(false);
}

#[test]
#[cfg(not(feature = "telemetry"))]
fn should_sample_returns_false() {
    assert!(!should_sample());
}

#[test]
#[serial]
fn record_functions_do_not_panic() {
    record_decode_ns(100);
    record_fanout_ns(200);
    record_ack_write_ns(300);
    record_quic_write_ns(400);
    record_sub_queue_wait_ns(500);
    record_sub_prefix_ns(550);
    record_sub_write_ns(600);
    record_sub_write_await_ns(650);
    record_sub_delivery_ns(700);
    record_cache_encode_ns(800);
    record_cache_write_ns(900);
    record_cache_read_ns(1000);
    record_cache_decode_ns(1100);
    record_cache_insert_ns(1200);
    record_cache_lookup_ns(1300);
}

#[test]
#[cfg(not(feature = "telemetry"))]
fn take_samples_returns_none() {
    assert!(take_samples().is_none());
}

#[test]
#[cfg(not(feature = "telemetry"))]
fn take_cache_samples_returns_none() {
    assert!(take_cache_samples().is_none());
}
