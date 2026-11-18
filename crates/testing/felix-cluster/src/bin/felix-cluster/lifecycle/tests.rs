use super::{perf_broker_env, up_stream};

fn args(list: &[&str]) -> Vec<String> {
    list.iter().map(|arg| arg.to_string()).collect()
}

#[test]
fn up_defaults_to_one_unreplicated_shard() {
    let spec = up_stream(&args(&["up"])).unwrap();
    assert_eq!(spec.name, crate::STREAM);
    assert_eq!((spec.shards, spec.replication_factor), (1, 1));
    assert_eq!(spec.consistency, "Leader");
}

#[test]
fn a_replication_factor_makes_a_quorum_stream() {
    let spec = up_stream(&args(&[
        "up",
        "--stream",
        "perf",
        "--shards",
        "2",
        "--replication-factor",
        "3",
    ]))
    .unwrap();
    assert_eq!(spec.name, "perf");
    assert_eq!((spec.shards, spec.replication_factor), (2, 3));
    assert_eq!(spec.consistency, "Quorum");
}

#[test]
fn zero_is_refused() {
    assert!(up_stream(&args(&["up", "--replication-factor", "0"])).is_err());
}

#[test]
fn up_hands_the_engine_and_fsync_choice_to_the_brokers() {
    let env = perf_broker_env(|name| match name {
        "FELIX_SHARD_ENGINE" => Some("raft".to_string()),
        "FELIX_DURABLE_FSYNC_MODE" => Some("on_commit".to_string()),
        _ => Some("ignored".to_string()),
    });
    assert_eq!(
        env,
        vec![
            ("FELIX_SHARD_ENGINE".to_string(), "raft".to_string()),
            (
                "FELIX_DURABLE_FSYNC_MODE".to_string(),
                "on_commit".to_string()
            ),
        ]
    );
    assert!(perf_broker_env(|_| None).is_empty());
}
