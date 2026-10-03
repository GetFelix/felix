use serde_json::json;

use super::*;

#[test]
fn a_pubsub_result_gives_publish_and_delivery_rows() {
    let result = json!({
        "scenario": "pubsub",
        "publish_throughput_msg_s": 1000.4,
        "delivered_throughput_msg_s": 9000.0,
        "ack_latency_us": {"p50": 120, "p99": 900, "p999": 1500, "max": 2000},
        "delivery_latency_us": {"p50": 200, "p99": 12_000},
    });
    let lines = summarize(&result);
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0].rate, Some(1000.4));
    assert_eq!((lines[0].p50_us, lines[0].p99_us), (Some(120), Some(900)));
    assert_eq!(lines[1].rate, Some(9000.0));
    assert_eq!(
        (lines[1].p50_us, lines[1].p99_us),
        (Some(200), Some(12_000))
    );
}

#[test]
fn a_pubsub_run_without_acks_has_no_ack_latency() {
    let result = json!({
        "scenario": "pubsub",
        "publish_throughput_msg_s": 10.0,
        "ack_latency_us": null,
        "delivery_latency_us": {"p50": 5, "p99": 6},
    });
    let lines = summarize(&result);
    assert_eq!(lines[0].p50_us, None);
}

#[test]
fn an_ingest_result_reports_acked_rate_only_when_acked() {
    let fire_and_forget = json!({
        "scenario": "ingest", "throughput_msg_s": 50_000.0,
        "acked_throughput_msg_s": null, "batch_ack_latency_us": null,
    });
    assert_eq!(summarize(&fire_and_forget).len(), 1);

    let acked = json!({
        "scenario": "ingest", "throughput_msg_s": 50_000.0,
        "acked_throughput_msg_s": 49_000.0,
        "batch_ack_latency_us": {"p50": 800, "p99": 4000},
    });
    let lines = summarize(&acked);
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[1].rate, Some(49_000.0));
    assert_eq!(lines[1].p99_us, Some(4000));
}

#[test]
fn a_cache_result_gives_put_and_get_rows() {
    let result = json!({
        "scenario": "cache",
        "put": {"n": 10, "throughput_op_s": 100.0, "latency_us": {"p50": 50, "p99": 90}},
        "get": {"n": 10, "throughput_op_s": 200.0, "latency_us": {"p50": 30, "p99": 60}},
    });
    let lines = summarize(&result);
    assert_eq!(lines[0].label, "put");
    assert_eq!(lines[1].label, "get");
    assert_eq!(lines[1].rate, Some(200.0));
    assert_eq!(lines[1].p50_us, Some(30));
}

#[test]
fn the_summary_renders_as_a_table() {
    let lines = vec![Line {
        label: "put",
        rate: Some(1234.6),
        unit: "op/s",
        p50_us: Some(850),
        p99_us: Some(12_345),
    }];
    assert_eq!(
        render("cache", &lines),
        "cache\n     RATE       P50     P99\nput  1235 op/s  850 us  12.3 ms"
    );
}

#[test]
fn micros_switch_to_milliseconds_at_ten() {
    assert_eq!(micros(9_999), "9999 us");
    assert_eq!(micros(10_000), "10.0 ms");
}
