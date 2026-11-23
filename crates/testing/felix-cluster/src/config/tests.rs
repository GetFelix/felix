use super::engine_named;

#[test]
fn raft_is_the_default_test_engine() {
    assert_eq!(engine_named(None), "raft");
    assert_eq!(engine_named(Some("")), "raft");
    assert_eq!(engine_named(Some("raft")), "raft");
}

#[test]
fn hybrid_is_opt_in() {
    assert_eq!(engine_named(Some("hybrid")), "hybrid");
}
