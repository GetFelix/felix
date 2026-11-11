use super::up_stream;

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
