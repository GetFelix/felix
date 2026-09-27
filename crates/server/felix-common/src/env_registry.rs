//! Every `FELIX_*` variable this workspace reads, and a check that a set one is
//! actually read.
//!
//! Configuration is a couple of hundred environment variables (`KNOWN_VARS`
//! below). They are individually parsed, so a name nobody reads does not fail —
//! it is simply absent, and the default takes effect. An operator who writes `FELIX_METRICS_BNID` gets a broker
//! listening somewhere they did not ask for, with no error and nothing in the
//! log to explain it.
//!
//! This is the same rule the wire protocol applies to unknown flag bits and the
//! config file now applies to unknown keys: a thing not understood is reported,
//! never ignored.
//!
//! The list is checked against the source by `scripts/check_env_registry.py`,
//! which CI runs — a new variable that is read but not listed fails there
//! rather than becoming a warning nobody can explain.

/// Every `FELIX_*` name any crate in this workspace reads.
///
/// Workspace-wide rather than per-binary on purpose. The failure worth catching
/// is a *typo*, and a name no code anywhere reads is a typo whichever process
/// it was set on. A control-plane variable set on a broker is a deployment
/// question, not a spelling one, and flagging it would train operators to
/// ignore the warning.
pub const KNOWN_VARS: &[&str] = &[
    "FELIX_ACK_ELICITING_THRESHOLD",
    "FELIX_ACK_FREQ_DISABLE",
    "FELIX_ACK_ON_COMMIT",
    "FELIX_ACK_WAIT_TIMEOUT_MS",
    "FELIX_AUTH_TENANT",
    "FELIX_AUTH_TIMEOUT_MS",
    "FELIX_AUTH_TOKEN",
    "FELIX_BENCH_EMBED_TS",
    "FELIX_BOOTSTRAP_BIND_ADDR",
    "FELIX_BOOTSTRAP_ENABLED",
    "FELIX_BOOTSTRAP_TLS_CERT",
    "FELIX_BOOTSTRAP_TLS_CLIENT_CA",
    "FELIX_BOOTSTRAP_TLS_KEY",
    "FELIX_BOOTSTRAP_TOKEN",
    "FELIX_BOOTSTRAP_TOKEN_PREVIOUS",
    "FELIX_BROKER_CONFIG",
    "FELIX_BROKER_METRICS_BIND",
    "FELIX_BROKER_PUBLISH_CONN_INFLIGHT_BYTES",
    "FELIX_BROKER_PUBLISH_INFLIGHT_BYTES",
    "FELIX_BROKER_PUB_FLUSH_CONCURRENCY",
    "FELIX_BROKER_PUB_QUEUE_DEPTH",
    "FELIX_BROKER_PUB_WORKERS_PER_CONN",
    "FELIX_CACHE_BENCH_CONCURRENCY",
    "FELIX_CACHE_BENCH_CONN_STATS",
    "FELIX_CACHE_BENCH_KEYS",
    "FELIX_CACHE_BENCH_OPS",
    "FELIX_CACHE_BENCH_PAYLOADS",
    "FELIX_CACHE_BENCH_SAMPLES",
    "FELIX_CACHE_BENCH_TIMINGS",
    "FELIX_CACHE_BENCH_TTL_MS",
    "FELIX_CACHE_BENCH_VALIDATE_EACH",
    "FELIX_CACHE_BENCH_WARMUP",
    "FELIX_CACHE_CONN_POOL",
    "FELIX_CACHE_CONN_RECV_WINDOW",
    "FELIX_CACHE_SEND_WINDOW",
    "FELIX_CACHE_STREAMS_PER_CONN",
    "FELIX_CACHE_STREAM_RECV_WINDOW",
    "FELIX_CLIENT_ADVERTISE_ADDR",
    "FELIX_CLIENT_CONFIG",
    "FELIX_CLIENT_SUB_QUEUE_CAPACITY",
    "FELIX_CLIENT_SUB_QUEUE_POLICY",
    "FELIX_CLUSTER_VERBOSE",
    "FELIX_CONN_STATS_MS",
    "FELIX_CONTROLPLANE_ACCEPT_BROKER_AUDIENCE",
    "FELIX_CONTROLPLANE_BIND",
    "FELIX_CONTROLPLANE_CA",
    "FELIX_CONTROLPLANE_CHANGES_LIMIT",
    "FELIX_CONTROLPLANE_CHANGE_RETENTION_MAX_ROWS",
    "FELIX_CONTROLPLANE_CONFIG",
    "FELIX_CONTROLPLANE_LEGACY_UNSCOPED_GROUPS",
    "FELIX_CONTROLPLANE_METRICS_BIND",
    "FELIX_CONTROLPLANE_OIDC_ALLOWED_ALGORITHMS",
    "FELIX_CONTROLPLANE_OIDC_ALLOW_INSECURE_HTTP",
    "FELIX_CONTROLPLANE_POSTGRES_ACQUIRE_TIMEOUT_MS",
    "FELIX_CONTROLPLANE_POSTGRES_CONNECT_TIMEOUT_MS",
    "FELIX_CONTROLPLANE_POSTGRES_MAX_CONNECTIONS",
    "FELIX_CONTROLPLANE_POSTGRES_URL",
    "FELIX_CONTROLPLANE_STORAGE_BACKEND",
    "FELIX_CONTROLPLANE_SYNC_INTERVAL_MS",
    "FELIX_CONTROLPLANE_TLS_CERT",
    "FELIX_CONTROLPLANE_TLS_KEY",
    "FELIX_CONTROLPLANE_URL",
    "FELIX_CONTROL_STREAM_DRAIN_TIMEOUT_MS",
    "FELIX_CORE_SHARDS",
    "FELIX_CP_SYNC_INTERVAL_MS",
    "FELIX_CP_URL",
    "FELIX_DEMO_LOG_CAPACITY",
    "FELIX_DISABLE_TIMINGS",
    "FELIX_DURABLE_FSYNC_INTERVAL_MS",
    "FELIX_DURABLE_FSYNC_MODE",
    "FELIX_DURABLE_INDEX_SPACING_BYTES",
    "FELIX_DURABLE_MAX_OVERSHOOT_PERCENT",
    "FELIX_DURABLE_MAX_RECORDS_PER_READ",
    "FELIX_DURABLE_PREALLOCATE",
    "FELIX_DURABLE_REPAIR_CHECKSUM_TAIL",
    "FELIX_DURABLE_RETENTION_BYTES",
    "FELIX_DURABLE_RETENTION_INTERVAL_SECONDS",
    "FELIX_DURABLE_RETENTION_SECONDS",
    "FELIX_DURABLE_ROLLOVER_THRESHOLD_PERCENT",
    "FELIX_DURABLE_SEGMENT_BYTES",
    "FELIX_DURABLE_STORAGE_DIR",
    "FELIX_DURABLE_VERIFY_ALL_ON_OPEN",
    "FELIX_EVENT_BATCH_MAX_BYTES",
    "FELIX_EVENT_BATCH_MAX_DELAY_US",
    "FELIX_EVENT_BATCH_MAX_EVENTS",
    "FELIX_EVENT_CONN_POOL",
    "FELIX_EVENT_CONN_RECV_WINDOW",
    "FELIX_EVENT_ROUTER_MAX_PENDING",
    "FELIX_EVENT_SEND_WINDOW",
    "FELIX_EVENT_STREAM_RECV_WINDOW",
    "FELIX_EXCHANGE_TOKEN_TTL_SECONDS",
    "FELIX_FANOUT_BATCH",
    "FELIX_GROUP_MAX_ATTEMPTS",
    "FELIX_GROUP_MAX_IN_FLIGHT",
    "FELIX_GROUP_MAX_WAIT_MS",
    "FELIX_GROUP_VISIBILITY_TIMEOUT_MS",
    "FELIX_INITIAL_CWND",
    "FELIX_INITIAL_MTU",
    "FELIX_INTERNAL_ALLOW_UNAUTHENTICATED",
    "FELIX_INTERNAL_BIND",
    "FELIX_INTERNAL_CONNS_PER_PEER",
    "FELIX_INTERNAL_HANDSHAKE_TIMEOUT_MS",
    "FELIX_INTERNAL_IDLE_TIMEOUT_MS",
    "FELIX_INTERNAL_MAX_INBOUND_CONNECTIONS",
    "FELIX_INTERNAL_MAX_INBOUND_PER_SOURCE",
    "FELIX_INTERNAL_MAX_INFLIGHT",
    "FELIX_INTERNAL_RECONNECT_BASE_MS",
    "FELIX_INTERNAL_RECONNECT_MAX_MS",
    "FELIX_INTERNAL_REQUEST_TIMEOUT_MS",
    "FELIX_INTERNAL_STREAMS_PER_CONN",
    "FELIX_INTERNAL_TLS_CA",
    "FELIX_INTERNAL_TLS_CERT",
    "FELIX_INTERNAL_TLS_KEY",
    "FELIX_IO_RUNTIME_THREADS",
    "FELIX_KAFKA_ADVERTISE_ADDR",
    "FELIX_KAFKA_ANONYMOUS_TENANT",
    "FELIX_KAFKA_AUTH_TIMEOUT_MS",
    "FELIX_KAFKA_DEFAULT_NAMESPACE",
    "FELIX_KAFKA_LISTEN",
    "FELIX_KAFKA_MAX_CONNECTIONS",
    "FELIX_KAFKA_MAX_CONNECTIONS_PER_IP",
    "FELIX_KAFKA_TLS",
    "FELIX_KEEPALIVE_MS",
    "FELIX_LATENCY_DEMO_FAST",
    "FELIX_MAX_CLIENT_CONNECTIONS",
    "FELIX_MAX_CONNECTIONS_PER_IP",
    "FELIX_MAX_FRAME_BYTES",
    "FELIX_MAX_IDLE_TIMEOUT_MS",
    "FELIX_MAX_SUBSCRIPTIONS_PER_CONN",
    "FELIX_MAX_SUB_WRITER_LANES",
    "FELIX_MAX_UDP_PAYLOAD",
    "FELIX_MTU_BLACK_HOLE_COOLDOWN_MS",
    "FELIX_MTU_UPPER_BOUND",
    "FELIX_NODE_ADVERTISE_ADDR",
    "FELIX_NODE_EXPIRY_SWEEP_INTERVAL_MS",
    "FELIX_NODE_EXPIRY_TIMEOUT_MS",
    "FELIX_NODE_HEARTBEAT_INTERVAL_MS",
    "FELIX_NODE_ID",
    "FELIX_NODE_REFRESH_TOKEN",
    "FELIX_NODE_REFRESH_TOKEN_FILE",
    "FELIX_NODE_REGRANT_MARGIN_MS",
    "FELIX_NODE_TOKEN",
    "FELIX_NODE_TOKEN_FILE",
    "FELIX_PEER_PARTITION_FILE",
    "FELIX_PREAUTH_MAX_FRAME_BYTES",
    "FELIX_PREAUTH_MAX_STREAMS_PER_CONN",
    "FELIX_PUBLISH_CHUNK_BYTES",
    "FELIX_PUBLISH_INFLIGHT_BYTES",
    "FELIX_PUBLISH_QUEUE_DEPTH",
    "FELIX_PUBLISH_QUEUE_WAIT_MS",
    "FELIX_PUBLISH_QUORUM_TIMEOUT_MS",
    "FELIX_PUBLISH_SHARDING",
    "FELIX_PUB_CONN_POOL",
    "FELIX_PUB_INGRESS_WAIT",
    "FELIX_PUB_SHARDING",
    "FELIX_PUB_STREAMS_PER_CONN",
    "FELIX_PUMP_COLOCATE",
    "FELIX_QUIC_BIND",
    "FELIX_QUIC_LISTENERS",
    "FELIX_RAFT_BIND_ADDR",
    "FELIX_RAFT_CLUSTER_ID",
    "FELIX_RAFT_DATA_DIR",
    "FELIX_RAFT_ELECTION_TIMEOUT_MAX_MS",
    "FELIX_RAFT_ELECTION_TIMEOUT_MIN_MS",
    "FELIX_RAFT_HEARTBEAT_MS",
    "FELIX_RAFT_INITIAL_CLUSTER_STATE",
    "FELIX_RAFT_INSECURE_PEERS",
    "FELIX_RAFT_LOGS_KEPT_BEHIND_SNAPSHOT",
    "FELIX_RAFT_NODE_ID",
    "FELIX_RAFT_PEERS",
    "FELIX_RAFT_PEER_TOKEN",
    "FELIX_RAFT_SNAPSHOT_LOGS_SINCE_LAST",
    "FELIX_RAFT_TLS_CA",
    "FELIX_RAFT_TLS_CERT",
    "FELIX_RAFT_TLS_KEY",
    "FELIX_RAFT_WRITE_TIMEOUT_MS",
    "FELIX_READINESS_CACHE_TTL_MS",
    "FELIX_READINESS_TIMEOUT_MS",
    "FELIX_REFRESH_TOKEN_TTL_SECONDS",
    "FELIX_REGION_BRIDGES",
    "FELIX_REGION_ID",
    "FELIX_REPLICATION_REBUILD_BYTES_PER_SEC",
    "FELIX_REPLICATION_REBUILD_MAX_CONCURRENT",
    "FELIX_SERVICE_INSTANCE_ID",
    "FELIX_SHARD_MOVES_MAX_CONCURRENT",
    "FELIX_SHARD_MOVES_MAX_PER_NODE",
    "FELIX_SHARD_MOVE_BYTES_PER_SEC",
    "FELIX_SHARD_MOVE_FENCE_MAX_LAG_RECORDS",
    "FELIX_SHARD_MOVE_HOLD_MAX",
    "FELIX_SHARD_MOVE_HOLD_MS",
    "FELIX_SHARD_MOVE_TIMEOUT_MS",
    "FELIX_SHARD_RECONCILE_INTERVAL_MS",
    "FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS",
    "FELIX_SHUTDOWN_HANDOFF_TIMEOUT_MS",
    "FELIX_SHUTDOWN_PREDRAIN_MS",
    "FELIX_STORAGE_IO_URING",
    "FELIX_SUBSCRIBER_QUEUE_CAPACITY",
    "FELIX_SUB_CONNS",
    "FELIX_SUB_DEDICATED_QUEUE_CAPACITY",
    "FELIX_SUB_DEDICATED_THREAD",
    "FELIX_SUB_DELIVERY_SHAPING",
    "FELIX_SUB_EGRESS_CONNS",
    "FELIX_SUB_EGRESS_LANES",
    "FELIX_SUB_FLUSH_MAX_DELAY_US",
    "FELIX_SUB_FLUSH_MAX_ITEMS",
    "FELIX_SUB_LANE_QUEUE_DEPTH",
    "FELIX_SUB_LANE_QUEUE_POLICY",
    "FELIX_SUB_LANE_SHARD",
    "FELIX_SUB_MAX_BYTES_PER_WRITE",
    "FELIX_SUB_QUEUE_BOUND",
    "FELIX_SUB_QUEUE_CAPACITY",
    "FELIX_SUB_QUEUE_MODE",
    "FELIX_SUB_QUEUE_POLICY",
    "FELIX_SUB_SINGLE_WRITER_PER_CONN",
    "FELIX_SUB_STREAMS_PER_CONN",
    "FELIX_SUB_STREAM_MODE",
    "FELIX_SUB_WRITER_LANES",
    "FELIX_TENANT_METRICS_MAX",
    "FELIX_TENANT_PUBLISH_BURST_MS",
    "FELIX_TENANT_PUBLISH_BYTES_PER_SEC",
    "FELIX_TENANT_PUBLISH_MSGS_PER_SEC",
    "FELIX_TENANT_PUBLISH_QUOTAS",
    "FELIX_TEST_BROKER_OUTPUT",
    "FELIX_TEST_DATABASE_URL",
    "FELIX_TEST_TIMEOUT_SCALE",
    "FELIX_TIMING_SAMPLE_EVERY",
    "FELIX_TLS_CERT",
    "FELIX_TLS_CERT_EXPORT",
    "FELIX_TLS_CLIENT_CA",
    "FELIX_TLS_KEY",
    "FELIX_TLS_REQUIRE_CERT",
    "FELIX_TOKEN",
    "FELIX_UDP_RECV_BUFFER",
    "FELIX_UDP_SEND_BUFFER",
    "FELIX_WORKER_THREADS",
];

/// Known names worth suggesting for `name`, best first.
///
/// Empty when nothing is close enough — suggesting the nearest arbitrary string
/// sends the reader after a setting that has no bearing on what they were doing.
///
/// Two ways of being close, because names get got wrong in two ways.
///
/// A **missing segment** is checked first: a name whose segments appear in
/// order within a known one. `FELIX_METRICS_BIND` reads like the obvious name
/// for the metrics listener and is not one — the broker's is
/// `FELIX_BROKER_METRICS_BIND` and the control plane's is
/// `FELIX_CONTROLPLANE_METRICS_BIND`. Both are returned, because the guess is
/// genuinely ambiguous and picking one would be inventing certainty.
///
/// A **misspelling** is caught by edit distance, budgeted at a third of the
/// name's length. It is checked second and only when the first finds nothing:
/// every segment matching exactly is the stronger signal, and trusting distance
/// first produced actively misleading answers — `FELIX_METRICS_BIND` is inside
/// the budget for `FELIX_QUIC_BIND`, a different listener entirely.
pub fn suggestions(name: &str) -> Vec<&'static str> {
    let wanted: Vec<&str> = name.split('_').collect();
    let by_segment: Vec<&'static str> = KNOWN_VARS
        .iter()
        .filter(|known| is_subsequence(&wanted, &known.split('_').collect::<Vec<_>>()))
        .copied()
        .collect();
    if !by_segment.is_empty() {
        return by_segment;
    }

    let budget = (name.len() / 3).max(1);
    KNOWN_VARS
        .iter()
        .map(|known| (distance(name, known), *known))
        .filter(|(d, _)| *d <= budget)
        .min_by_key(|(d, _)| *d)
        .map(|(_, known)| vec![known])
        .unwrap_or_default()
}

/// `FELIX_*` variables that are set in the environment and read by nothing,
/// each with a suggestion when there is a plausible one.
pub fn unrecognised() -> Vec<(String, Vec<&'static str>)> {
    let mut found: Vec<(String, Vec<&'static str>)> = std::env::vars()
        .map(|(name, _)| name)
        .filter(|name| name.starts_with("FELIX_"))
        .filter(|name| !KNOWN_VARS.contains(&name.as_str()))
        .map(|name| {
            let suggestions = suggestions(&name);
            (name, suggestions)
        })
        .collect();
    found.sort();
    found
}

/// A line per unrecognised variable, ready to log.
///
/// Returned rather than logged here: this crate stays free of a logging
/// dependency so a library consumer that never runs a process does not pull
/// one in, the same reason `lifecycle` is feature-gated. The binaries log.
pub fn unrecognised_warnings() -> Vec<String> {
    unrecognised()
        .into_iter()
        .map(|(name, suggestions)| match suggestions.as_slice() {
            [] => format!("{name} is set and nothing reads it"),
            [known] => format!(
                "{name} is set and nothing reads it — did you mean {known}? \
                 That setting is using its default"
            ),
            many => format!(
                "{name} is set and nothing reads it — did you mean one of {}? \
                 Those settings are using their defaults",
                many.join(", "),
            ),
        })
        .collect()
}

/// Levenshtein distance, bounded — only used to suggest a name for a typo.
fn distance(a: &str, b: &str) -> usize {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut current = vec![0usize; b.len() + 1];
    for (i, &ca) in a.iter().enumerate() {
        current[0] = i + 1;
        for (j, &cb) in b.iter().enumerate() {
            let cost = usize::from(ca != cb);
            current[j + 1] = (prev[j] + cost).min(prev[j + 1] + 1).min(current[j] + 1);
        }
        std::mem::swap(&mut prev, &mut current);
    }
    prev[b.len()]
}

/// Whether every element of `needle` appears in `haystack`, in order.
fn is_subsequence(needle: &[&str], haystack: &[&str]) -> bool {
    let mut it = haystack.iter();
    needle
        .iter()
        .all(|segment| it.any(|candidate| candidate == segment))
}

#[cfg(test)]
mod tests;
