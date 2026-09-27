use std::env;
use std::fs;

use serial_test::serial;
use tempfile::TempDir;

use super::*;

/// Names the pg-test harness uses to find its database.
///
/// `FELIX_CONTROLPLANE_POSTGRES_URL` and `DATABASE_URL` are also real
/// configuration, so they are cleared like anything else and restored on
/// drop. `FELIX_TEST_DATABASE_URL` is only ever harness plumbing, so
/// clearing it would be wrong even momentarily.
const HARNESS_ONLY_ENV: &str = "FELIX_TEST_DATABASE_URL";

/// Clears the config environment for the life of the returned guard.
///
/// Restoring matters beyond tidiness: these tests share a process with the
/// pg-test helpers, which read `FELIX_TEST_DATABASE_URL` and friends to find
/// their database. Clearing without restoring left every pg test that
/// happened to run afterwards silently falling back to spawning a Docker
/// container -- flaky, and it leaked the container.
#[must_use]
fn clear_felix_env() -> EnvRestore {
    let mut cleared = Vec::new();
    for (key, value) in env::vars() {
        if key == HARNESS_ONLY_ENV {
            continue;
        }
        if key.starts_with("FELIX_") || key == "DATABASE_URL" {
            unsafe {
                env::remove_var(&key);
            }
            cleared.push((key, value));
        }
    }
    EnvRestore { cleared }
}

/// Puts back everything [`clear_felix_env`] removed, and anything the test
/// set afterwards is removed for the same reason.
struct EnvRestore {
    cleared: Vec<(String, String)>,
}

impl Drop for EnvRestore {
    fn drop(&mut self) {
        for (key, _) in env::vars() {
            if key != HARNESS_ONLY_ENV && (key.starts_with("FELIX_") || key == "DATABASE_URL") {
                unsafe {
                    env::remove_var(&key);
                }
            }
        }
        for (key, value) in &self.cleared {
            unsafe {
                env::set_var(key, value);
            }
        }
    }
}

/// A timeout at or below the interval expires brokers that are heartbeating
/// exactly as configured, which takes the cluster down.
#[test]
fn a_liveness_timeout_must_outlast_the_heartbeat_interval() {
    let too_short = NodeLivenessConfig {
        heartbeat_interval_ms: 5_000,
        expiry_timeout_ms: 5_000,
        sweep_interval_ms: 1_000,
        shard_reconcile_interval_ms: 5_000,
    };
    let err = too_short.validate().expect_err("equal should be rejected");
    assert!(err.to_string().contains("must exceed"), "{err}");

    let inverted = NodeLivenessConfig {
        expiry_timeout_ms: 1_000,
        ..too_short.clone()
    };
    assert!(inverted.validate().is_err());

    let ok = NodeLivenessConfig {
        expiry_timeout_ms: 5_001,
        ..too_short
    };
    assert!(ok.validate().is_ok());
}

#[test]
fn zero_liveness_intervals_are_rejected() {
    for zeroed in [
        NodeLivenessConfig {
            heartbeat_interval_ms: 0,
            ..NodeLivenessConfig::default()
        },
        NodeLivenessConfig {
            sweep_interval_ms: 0,
            ..NodeLivenessConfig::default()
        },
        NodeLivenessConfig {
            shard_reconcile_interval_ms: 0,
            ..NodeLivenessConfig::default()
        },
    ] {
        assert!(zeroed.validate().is_err(), "{zeroed:?} should be rejected");
    }
}

#[test]
fn the_default_liveness_config_is_valid() {
    assert!(NodeLivenessConfig::default().validate().is_ok());
}

/// An unparsable or zero value falls back to the default rather than
/// disabling the timer.
#[serial]
#[test]
fn bad_liveness_env_values_fall_back_to_defaults() {
    let _env = clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_HEARTBEAT_INTERVAL_MS", "not-a-number");
        env::set_var("FELIX_NODE_EXPIRY_SWEEP_INTERVAL_MS", "0");
        env::set_var("FELIX_NODE_EXPIRY_TIMEOUT_MS", "30000");
    }

    let config = ControlPlaneConfig::from_env().expect("config");
    assert_eq!(
        config.node_liveness.heartbeat_interval_ms,
        DEFAULT_NODE_HEARTBEAT_INTERVAL_MS
    );
    assert_eq!(
        config.node_liveness.sweep_interval_ms,
        DEFAULT_NODE_EXPIRY_SWEEP_INTERVAL_MS
    );
    assert_eq!(config.node_liveness.expiry_timeout_ms, 30_000);
}

/// A config whose liveness timings cannot work must fail at startup, not
/// after it has taken the cluster down.
#[serial]
#[test]
fn an_unworkable_liveness_config_fails_startup() {
    let _env = clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_HEARTBEAT_INTERVAL_MS", "10000");
        env::set_var("FELIX_NODE_EXPIRY_TIMEOUT_MS", "5000");
    }

    let err = ControlPlaneConfig::from_env().expect_err("should fail");
    assert!(err.to_string().contains("must exceed"), "{err}");
}

#[serial]
#[test]
fn api_tls_is_both_paths_or_neither() {
    let _env = clear_felix_env();
    assert!(
        ControlPlaneConfig::from_env()
            .expect("none")
            .api_tls
            .is_none()
    );

    unsafe {
        env::set_var("FELIX_CONTROLPLANE_TLS_CERT", "/etc/felix/api/tls.crt");
    }
    let err = ControlPlaneConfig::from_env().expect_err("cert without key");
    assert!(
        err.to_string().contains("FELIX_CONTROLPLANE_TLS_KEY"),
        "{err}"
    );

    unsafe {
        env::set_var("FELIX_CONTROLPLANE_TLS_KEY", "/etc/felix/api/tls.key");
    }
    let tls = ControlPlaneConfig::from_env()
        .expect("both")
        .api_tls
        .expect("api tls");
    assert_eq!(tls.cert_path, "/etc/felix/api/tls.crt");
    assert_eq!(tls.key_path, "/etc/felix/api/tls.key");
}

#[serial]
#[test]
fn from_env_uses_defaults() {
    let _env = clear_felix_env();
    let config = ControlPlaneConfig::from_env().expect("from_env");
    assert_eq!(config.bind_addr.to_string(), "0.0.0.0:8443");
    assert_eq!(config.metrics_bind.to_string(), "0.0.0.0:8080");
    assert_eq!(config.region_id, "local");
    assert_eq!(config.changes_limit, DEFAULT_CHANGES_LIMIT);
    assert_eq!(config.oidc_allowed_algorithms, vec![Algorithm::ES256]);
    assert!(matches!(config.storage, StorageBackend::Memory));
    let _env = clear_felix_env();
}

#[serial]
#[test]
fn from_env_respects_env_vars() {
    let _env = clear_felix_env();
    unsafe {
        env::set_var("FELIX_CONTROLPLANE_BIND", "127.0.0.1:9443");
        env::set_var("FELIX_CONTROLPLANE_METRICS_BIND", "127.0.0.1:9090");
        env::set_var("FELIX_REGION_ID", "us-west-2");
        env::set_var("FELIX_CONTROLPLANE_CHANGES_LIMIT", "5000");
        env::set_var(
            "FELIX_CONTROLPLANE_OIDC_ALLOWED_ALGORITHMS",
            "ES256,RS256,PS256,RS256",
        );
    }

    let config = ControlPlaneConfig::from_env().expect("from_env");
    assert_eq!(config.bind_addr.to_string(), "127.0.0.1:9443");
    assert_eq!(config.metrics_bind.to_string(), "127.0.0.1:9090");
    assert_eq!(config.region_id, "us-west-2");
    assert_eq!(config.changes_limit, 5000);
    assert_eq!(
        config.oidc_allowed_algorithms,
        vec![Algorithm::ES256, Algorithm::RS256, Algorithm::PS256]
    );

    let _env = clear_felix_env();
}

/// The minimum a Raft member needs besides its peer credentials.
fn set_raft_basics() {
    unsafe {
        env::set_var("FELIX_RAFT_NODE_ID", "1");
        env::set_var("FELIX_RAFT_DATA_DIR", "/tmp/felix-raft-config-test");
        env::set_var("FELIX_RAFT_PEERS", "1=127.0.0.1:9444");
        env::set_var("FELIX_RAFT_BIND_ADDR", "127.0.0.1:9444");
        env::set_var("FELIX_RAFT_CLUSTER_ID", "test-cluster");
    }
}

const PEER_TOKEN: &str = "0123456789abcdef0123456789abcdef";

#[serial]
#[test]
fn raft_refuses_to_start_without_a_peer_token() {
    let _env = clear_felix_env();
    set_raft_basics();
    let err = ControlPlaneConfig::from_env().expect_err("no peer token");
    assert!(err.to_string().contains("FELIX_RAFT_PEER_TOKEN"), "{err}");

    unsafe { env::set_var("FELIX_RAFT_PEER_TOKEN", "short") };
    let err = ControlPlaneConfig::from_env().expect_err("short peer token");
    assert!(err.to_string().contains("at least"), "{err}");

    unsafe { env::set_var("FELIX_RAFT_PEER_TOKEN", PEER_TOKEN) };
    let config = ControlPlaneConfig::from_env().expect("token set");
    let raft = config.raft.expect("raft config");
    assert_eq!(raft.security.token.as_deref(), Some(PEER_TOKEN));
    assert_eq!(raft.peer_bind_addr.to_string(), "127.0.0.1:9444");
    assert_eq!(
        raft.initial_cluster_state,
        crate::raft::InitialClusterState::Existing,
        "an empty member must not form a group unless told to"
    );
    assert!(
        !format!("{:?}", raft.security).contains(PEER_TOKEN),
        "the peer token must not reach Debug output"
    );
}

#[serial]
#[test]
fn raft_runs_without_a_peer_token_only_when_told_to() {
    let _env = clear_felix_env();
    set_raft_basics();
    unsafe { env::set_var("FELIX_RAFT_INSECURE_PEERS", "true") };
    let config = ControlPlaneConfig::from_env().expect("explicit opt-out");
    assert!(config.raft.expect("raft config").security.token.is_none());
}

#[serial]
#[test]
fn raft_needs_a_cluster_id_and_its_own_listener() {
    let _env = clear_felix_env();
    set_raft_basics();
    unsafe {
        env::set_var("FELIX_RAFT_PEER_TOKEN", PEER_TOKEN);
        env::remove_var("FELIX_RAFT_CLUSTER_ID");
    }
    let err = ControlPlaneConfig::from_env().expect_err("no cluster id");
    assert!(err.to_string().contains("FELIX_RAFT_CLUSTER_ID"), "{err}");

    unsafe {
        env::set_var("FELIX_RAFT_CLUSTER_ID", "test-cluster");
        env::remove_var("FELIX_RAFT_BIND_ADDR");
    }
    let err = ControlPlaneConfig::from_env().expect_err("no peer listener");
    assert!(err.to_string().contains("FELIX_RAFT_BIND_ADDR"), "{err}");
}

#[serial]
#[test]
fn raft_initial_cluster_state_and_peer_tls_parse_strictly() {
    let _env = clear_felix_env();
    set_raft_basics();
    unsafe {
        env::set_var("FELIX_RAFT_PEER_TOKEN", PEER_TOKEN);
        env::set_var("FELIX_RAFT_INITIAL_CLUSTER_STATE", "new");
    }
    let config = ControlPlaneConfig::from_env().expect("new");
    assert_eq!(
        config.raft.expect("raft").initial_cluster_state,
        crate::raft::InitialClusterState::New
    );

    unsafe { env::set_var("FELIX_RAFT_INITIAL_CLUSTER_STATE", "bootstrap") };
    let err = ControlPlaneConfig::from_env().expect_err("unknown state");
    assert!(err.to_string().contains("'new' or 'existing'"), "{err}");

    unsafe {
        env::remove_var("FELIX_RAFT_INITIAL_CLUSTER_STATE");
        env::set_var("FELIX_RAFT_TLS_CERT", "/etc/felix/raft.crt");
    }
    let err = ControlPlaneConfig::from_env().expect_err("partial TLS");
    assert!(err.to_string().contains("FELIX_RAFT_TLS_CA"), "{err}");
}

/// Move pacing from the environment. Zero means "no limit" for the per-node
/// cap and "never" for the timeout, and a move limit of zero holds every
/// move.
#[serial]
#[test]
fn shard_move_pacing_comes_from_the_environment() {
    let _env = clear_felix_env();
    let defaults = ControlPlaneConfig::from_env()
        .expect("from_env")
        .shard_moves;
    assert_eq!(defaults, crate::cluster::placement::MovePolicy::default());

    unsafe {
        env::set_var("FELIX_SHARD_MOVES_MAX_CONCURRENT", "0");
        env::set_var("FELIX_SHARD_MOVES_MAX_PER_NODE", "2");
        env::set_var("FELIX_SHARD_MOVE_FENCE_MAX_LAG_RECORDS", "0");
        env::set_var("FELIX_SHARD_MOVE_TIMEOUT_MS", "0");
    }
    let config = ControlPlaneConfig::from_env()
        .expect("from_env")
        .shard_moves;
    assert_eq!(config.max_concurrent, 0);
    assert_eq!(config.max_per_node, Some(2));
    assert_eq!(config.fence_max_lag_records, 0);
    assert_eq!(config.timeout_millis, None);

    unsafe {
        env::set_var("FELIX_SHARD_MOVES_MAX_PER_NODE", "0");
    }
    let config = ControlPlaneConfig::from_env()
        .expect("from_env")
        .shard_moves;
    assert_eq!(config.max_per_node, None);
    let _env = clear_felix_env();
}

#[serial]
#[test]
fn from_env_rejects_invalid_socket_addr() {
    let _env = clear_felix_env();
    unsafe {
        env::set_var("FELIX_CONTROLPLANE_BIND", "not-a-valid-address");
    }
    let result = ControlPlaneConfig::from_env();
    assert!(result.is_err());
    let _env = clear_felix_env();
}

#[serial]
#[test]
fn from_env_activates_postgres_when_url_present() {
    let _env = clear_felix_env();
    unsafe {
        env::set_var(
            "FELIX_CONTROLPLANE_POSTGRES_URL",
            "postgres://localhost/test",
        );
    }
    let config = ControlPlaneConfig::from_env().expect("from_env");
    assert!(matches!(config.storage, StorageBackend::Postgres));
    assert!(config.postgres.is_some());
    let _env = clear_felix_env();
}

#[serial]
#[test]
fn from_env_or_yaml_no_file_uses_defaults() {
    let _env = clear_felix_env();
    let config = ControlPlaneConfig::from_env_or_yaml().expect("from_env_or_yaml");
    assert_eq!(config.bind_addr.to_string(), "0.0.0.0:8443");
    assert_eq!(config.region_id, "local");
    let _env = clear_felix_env();
}

#[serial]
#[test]
fn from_env_or_yaml_file_not_found_fails() {
    let _env = clear_felix_env();
    let tmpdir = TempDir::new().unwrap();
    let nonexistent = tmpdir.path().join("nonexistent.yml");
    unsafe {
        env::set_var("FELIX_CONTROLPLANE_CONFIG", nonexistent.to_str().unwrap());
    }
    let result = ControlPlaneConfig::from_env_or_yaml();
    assert!(result.is_err());
    let _env = clear_felix_env();
}

#[serial]
#[test]
fn from_env_or_yaml_overrides_with_valid_yaml() {
    let _env = clear_felix_env();
    let tmpdir = TempDir::new().unwrap();
    let config_path = tmpdir.path().join("config.yml");
    fs::write(
        &config_path,
        r#"
bind_addr: "127.0.0.1:7443"
metrics_bind: "127.0.0.1:7070"
region_id: "eu-central-1"
changes_limit: 2000
oidc_allowed_algorithms: ["ES256", "RS384", "PS512"]
storage:
  backend: "memory"
"#,
    )
    .unwrap();
    unsafe {
        env::set_var("FELIX_CONTROLPLANE_CONFIG", config_path.to_str().unwrap());
    }

    let config = ControlPlaneConfig::from_env_or_yaml().expect("from_env_or_yaml");
    assert_eq!(config.bind_addr.to_string(), "127.0.0.1:7443");
    assert_eq!(config.metrics_bind.to_string(), "127.0.0.1:7070");
    assert_eq!(config.region_id, "eu-central-1");
    assert_eq!(config.changes_limit, 2000);
    assert_eq!(
        config.oidc_allowed_algorithms,
        vec![Algorithm::ES256, Algorithm::RS384, Algorithm::PS512]
    );

    let _env = clear_felix_env();
}

#[serial]
#[test]
fn from_env_or_yaml_invalid_yaml_fails() {
    let _env = clear_felix_env();
    let tmpdir = TempDir::new().unwrap();
    let config_path = tmpdir.path().join("bad.yml");
    fs::write(&config_path, "this is not: valid: yaml:").unwrap();
    unsafe {
        env::set_var("FELIX_CONTROLPLANE_CONFIG", config_path.to_str().unwrap());
    }

    let result = ControlPlaneConfig::from_env_or_yaml();
    assert!(result.is_err());

    let _env = clear_felix_env();
}

#[serial]
#[test]
fn from_env_or_yaml_invalid_socket_in_yaml_fails() {
    let _env = clear_felix_env();
    let tmpdir = TempDir::new().unwrap();
    let config_path = tmpdir.path().join("config.yml");
    fs::write(&config_path, "bind_addr: \"not-a-socket\"").unwrap();
    unsafe {
        env::set_var("FELIX_CONTROLPLANE_CONFIG", config_path.to_str().unwrap());
    }

    let result = ControlPlaneConfig::from_env_or_yaml();
    assert!(result.is_err());

    let _env = clear_felix_env();
}

#[serial]
#[test]
fn from_env_reads_region_bridges() {
    let _env = clear_felix_env();
    unsafe {
        env::set_var("FELIX_REGION_BRIDGES", "eu>us");
    }
    let regions = ControlPlaneConfig::from_env()
        .expect("from_env")
        .shard_moves
        .regions;
    assert!(regions.can_route(&"eu".to_string(), &"us".to_string()));
    assert!(!regions.can_route(&"us".to_string(), &"eu".to_string()));

    unsafe {
        env::set_var("FELIX_REGION_BRIDGES", "eu-us");
    }
    assert!(ControlPlaneConfig::from_env().is_err());
    let _env = clear_felix_env();
}

#[serial]
#[test]
fn from_env_rejects_invalid_oidc_algorithm() {
    let _env = clear_felix_env();
    unsafe {
        env::set_var(
            "FELIX_CONTROLPLANE_OIDC_ALLOWED_ALGORITHMS",
            "ES256,INVALID",
        );
    }
    let result = ControlPlaneConfig::from_env();
    assert!(result.is_err());
    let _env = clear_felix_env();
}

/// The YAML config file, folded over what the environment already gave.
///
/// Driven through `apply` so the precedence table is one test rather than
/// one per key: an override that parses but is never assigned is invisible
/// until an operator sets it and nothing happens.
mod yaml_overrides {
    use super::*;

    fn parse(yaml: &str) -> ControlPlaneConfigOverride {
        serde_yaml_ng::from_str(yaml).expect("parse the override")
    }

    /// A base taken from an empty environment, which is what a config file
    /// is folded over in production.
    fn base() -> (ControlPlaneConfig, EnvRestore) {
        let restore = clear_felix_env();
        (ControlPlaneConfig::from_env().expect("config"), restore)
    }

    /// **Every key in the file has to reach the config**, nested ones
    /// included — the liveness and bootstrap blocks are the settings an
    /// operator is most likely to tune, and a dropped assignment there
    /// looks like a broker that never expires.
    #[serial]
    #[test]
    fn every_key_in_the_file_reaches_the_config() {
        let (mut config, _restore) = base();
        config
            .apply(parse(
                r#"
bind_addr: "127.0.0.1:9443"
tls:
  cert_path: "/etc/felix/api/tls.crt"
  key_path: "/etc/felix/api/tls.key"
metrics_bind: "127.0.0.1:9444"
region_id: "eu-west-1"
changes_limit: 4096
change_retention_max_rows: 100000
shutdown_drain_timeout_ms: 12000
max_concurrent_shard_moves: 3
max_shard_moves_per_node: 2
shard_move_fence_max_lag_records: 500
shard_move_timeout_ms: 60000
oidc_allowed_algorithms: ["ES256", "RS256"]
node_liveness:
  heartbeat_interval_ms: 1100
  expiry_timeout_ms: 4400
  sweep_interval_ms: 2200
  shard_reconcile_interval_ms: 3300
bootstrap:
  enabled: true
  bind_addr: "127.0.0.1:9445"
  token: "a-bootstrap-token"
"#,
            ))
            .expect("apply");

        assert_eq!(config.bind_addr, "127.0.0.1:9443".parse().unwrap());
        assert_eq!(
            config.api_tls.as_ref().map(|tls| tls.key_path.as_str()),
            Some("/etc/felix/api/tls.key")
        );
        assert_eq!(config.metrics_bind, "127.0.0.1:9444".parse().unwrap());
        assert_eq!(config.region_id, "eu-west-1");
        assert_eq!(config.changes_limit, 4096);
        assert_eq!(config.change_retention_max_rows, Some(100000));
        assert_eq!(config.shutdown_drain_timeout_ms, 12000);
        assert_eq!(config.oidc_allowed_algorithms.len(), 2);
        assert_eq!(config.node_liveness.heartbeat_interval_ms, 1100);
        assert_eq!(config.node_liveness.expiry_timeout_ms, 4400);
        assert_eq!(config.node_liveness.sweep_interval_ms, 2200);
        assert_eq!(config.node_liveness.shard_reconcile_interval_ms, 3300);
        assert!(config.bootstrap.enabled);
        assert_eq!(
            config.bootstrap.bind_addr,
            "127.0.0.1:9445".parse().unwrap()
        );
        assert_eq!(config.bootstrap.token.as_deref(), Some("a-bootstrap-token"));
        assert_eq!(
            config.shard_moves,
            crate::cluster::placement::MovePolicy {
                max_concurrent: 3,
                max_per_node: Some(2),
                fence_max_lag_records: 500,
                timeout_millis: Some(60_000),
                paused: false,
                ..Default::default()
            }
        );
    }

    /// A file that names no keys changes nothing. Absent is not zero: what
    /// the environment set has to survive a file that says nothing about it.
    #[serial]
    #[test]
    fn an_empty_file_leaves_the_config_alone() {
        let (mut config, _restore) = base();
        let before = config.region_id.clone();
        let changes_limit = config.changes_limit;

        config.apply(parse("{}")).expect("apply");

        assert_eq!(config.region_id, before);
        assert_eq!(config.changes_limit, changes_limit);
    }

    /// **Naming a postgres block selects postgres.** Writing a database URL
    /// and still running against memory would look like a control plane
    /// that had lost every registration on restart.
    #[serial]
    #[test]
    fn a_postgres_block_selects_the_postgres_backend() {
        let (mut config, _restore) = base();
        assert!(matches!(config.storage, StorageBackend::Memory));

        config
            .apply(parse(
                r#"
postgres:
  url: "postgres://localhost/felix"
  max_connections: 12
  connect_timeout_ms: 3000
  acquire_timeout_ms: 4000
"#,
            ))
            .expect("apply");

        assert!(matches!(config.storage, StorageBackend::Postgres));
        let pg = config.postgres.expect("postgres config");
        assert_eq!(pg.url, "postgres://localhost/felix");
        assert_eq!(pg.max_connections, 12);
        assert_eq!(pg.connect_timeout_ms, 3000);
        assert_eq!(pg.acquire_timeout_ms, 4000);
    }

    /// An explicit backend is not overridden by supplying connection
    /// details. Only an unset (memory) backend is inferred from them.
    #[serial]
    #[test]
    fn an_explicitly_chosen_backend_survives_a_postgres_block() {
        let (mut config, _restore) = base();
        config
            .apply(parse(
                r#"
storage:
  backend: "postgres"
postgres:
  url: "postgres://localhost/felix"
"#,
            ))
            .expect("apply");

        assert!(matches!(config.storage, StorageBackend::Postgres));
    }

    /// A zero drain budget means "cancel everything immediately", which is
    /// not a shorter graceful shutdown but the absence of one.
    #[serial]
    #[test]
    fn a_zero_drain_timeout_is_ignored() {
        let (mut config, _restore) = base();
        let before = config.shutdown_drain_timeout_ms;

        config
            .apply(parse("shutdown_drain_timeout_ms: 0"))
            .expect("apply");

        assert_eq!(config.shutdown_drain_timeout_ms, before);
    }

    /// An address that does not parse fails the load and names the key, so
    /// the control plane does not come up bound somewhere unasked for.
    #[serial]
    #[test]
    fn an_unparseable_address_names_the_key_it_came_from() {
        let (mut config, _restore) = base();
        let err = config
            .apply(parse("bind_addr: not-an-address"))
            .expect_err("an unparseable address should fail");
        assert!(err.to_string().contains("bind_addr"), "{err}");
    }

    /// An unknown signing algorithm is refused rather than dropped: a
    /// control plane silently accepting fewer algorithms than the operator
    /// listed would reject tokens it was configured to trust.
    #[serial]
    #[test]
    fn an_unknown_signing_algorithm_is_refused() {
        let (mut config, _restore) = base();
        assert!(
            config
                .apply(parse(r#"oidc_allowed_algorithms: ["ES256", "MD5"]"#))
                .is_err()
        );
    }
}

/// `Debug` on the config is what ends up in a startup log or a panic message,
/// so it must not carry the database password or the bootstrap tokens.
mod debug_redaction {
    use super::storage::redact_postgres_url;
    use super::*;

    #[serial]
    #[test]
    fn debug_hides_the_postgres_password_and_bootstrap_tokens() {
        let restore = clear_felix_env();
        let mut config = ControlPlaneConfig::from_env().expect("config");
        drop(restore);
        config.postgres = Some(PostgresConfig {
            url: "postgres://felix:pg-s3cret@db.internal:5432/felix?password=pg-other".into(),
            ..PostgresConfig::default()
        });
        config.bootstrap.token = Some("boot-current-token".into());
        config.bootstrap.previous_token = Some("boot-previous-token".into());

        let printed = format!("{config:?}");
        for (what, secret) in [
            ("the URL password", "pg-s3cret"),
            ("the password= parameter", "pg-other"),
            ("the bootstrap token", "boot-current-token"),
            ("the previous bootstrap token", "boot-previous-token"),
        ] {
            // Name the field, not the value: a failure message is a log too.
            assert!(!printed.contains(secret), "{what} leaked into Debug");
        }
        // Still useful for debugging: where it connects, and as whom.
        assert!(
            printed.contains("felix:<redacted>@db.internal:5432/felix"),
            "the redacted URL should keep the user, host and database"
        );
    }

    #[test]
    fn a_postgres_url_keeps_everything_but_the_password() {
        for (url, want) in [
            (
                "postgres://felix:secret@db:5432/felix",
                "postgres://felix:<redacted>@db:5432/felix",
            ),
            (
                "postgresql://felix@db/felix?sslmode=require",
                "postgresql://felix@db/felix?sslmode=require",
            ),
            (
                "postgres://db/felix?sslmode=require&PASSWORD=secret&user=felix",
                "postgres://db/felix?sslmode=require&PASSWORD=<redacted>&user=felix",
            ),
            ("postgres://db/felix", "postgres://db/felix"),
            ("", ""),
        ] {
            assert_eq!(redact_postgres_url(url), want, "{url}");
        }
    }

    /// Unencoded `@` and `/` in a password are invalid, but an operator can
    /// still write them, and the redaction must not stop short of the host.
    #[test]
    fn a_password_with_reserved_characters_is_still_hidden() {
        let redacted = redact_postgres_url("postgres://felix:se/cr@t@db:5432/felix");
        assert_eq!(redacted, "postgres://felix:<redacted>@db:5432/felix");
    }

    /// A key/value connection string can put the password anywhere, so it is
    /// withheld whole.
    #[test]
    fn a_non_url_connection_string_is_withheld() {
        assert_eq!(
            redact_postgres_url("host=db user=felix password=secret"),
            "<redacted>"
        );
    }
}
