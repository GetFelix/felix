//! Followers that shipping has stopped for.

use super::*;

/// **A halt says which replica, not just how many.**
///
/// The metric is a bare count and has to stay one — a label per shard is a
/// label per stream per tenant. So until now the only way to learn which
/// replica had stopped, on which shard, and why, was to grep the broker's logs
/// for the warning that accompanied the halt. A halt does not resolve on its
/// own, so that is the information an operator needs before they can act at
/// all (#424).
#[tokio::test]
async fn a_halted_follower_is_named_with_its_shard_and_reason() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let marks = QuorumMarks::new();

    let pass = replicate_once(
        &DivergingFollower,
        &broker,
        &router,
        &marks,
        None,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    assert_eq!(pass.halted.len(), 1);
    let halted = &pass.halted[0];
    assert_eq!(halted.node_id, "broker-b");
    assert_eq!(halted.stream, STREAM);
    assert_eq!(halted.tenant_id, TENANT);
    assert_eq!(halted.namespace, NAMESPACE);
    assert_eq!(halted.shard, 0);
    assert_eq!(halted.kind, "stream");
    assert_eq!(halted.generation, 4);
    assert_eq!(halted.reason, "diverged");
    assert!(
        halted.remedy.contains("discarded and rebuilt"),
        "the listing named a halt without saying what to do about it",
    );
}

/// A healthy pass lists nothing, so a non-empty listing is always news.
#[tokio::test]
async fn a_shipping_follower_is_not_listed_as_halted() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let marks = QuorumMarks::new();

    let pass = replicate_once(
        &AcceptingFollower::default(),
        &broker,
        &router,
        &marks,
        None,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    assert!(pass.halted.is_empty());
}
