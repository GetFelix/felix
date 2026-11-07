//! A failover's fence counts a replica's answer only from the copy the report
//! it was decided on named: `FenceCounts` in `docs/formal/FelixShard.tla`
//! (#1335).

use super::*;

/// The route the promoted leader sees, naming each follower under the
/// identity it started with, as the report the failover was decided on did.
fn route_naming_every_copy() -> Route {
    let mut route = route();
    route.fence_storage_ids = Some(Arc::new(
        ["broker-b", "broker-c"]
            .into_iter()
            .map(|id| (id.to_string(), storage_of(id).to_hex()))
            .collect(),
    ));
    route
}

/// broker-b came back under a new identity, as after a crash under
/// `Periodic` that cost it the tail it had answered for.
fn rotated() -> StorageId {
    StorageId::from_bytes([0xee; 16])
}

/// **A replica back under another identity than the report named is not
/// counted.** The old leader (broker-c here) acknowledged `w` on itself and
/// broker-b; broker-b crashed and lost it. Counting broker-b with this broker
/// would open a majority that never held `w`. The fence waits for broker-c,
/// whose answer carries it.
#[tokio::test]
async fn a_fence_answer_from_another_storage_id_does_not_count() {
    let mut replicas = Replicas::new();
    replicas
        .get("broker-b")
        .holds("broker-b", 4, 0, &["a"])
        .await;
    replicas
        .get("broker-c")
        .holds("broker-c", 4, 0, &["a", "w"])
        .await;
    replicas.get_mut("broker-b").storage = Some(rotated());
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    let route = route_naming_every_copy();

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route, false, None).await;

    let Outcome::Pending { took, why } = outcome else {
        panic!("opened on a copy the report did not name: {outcome:?}");
    };
    assert!(took.is_empty(), "{took:?}");
    assert!(why.contains("another copy"), "{why}");
    // It still took the promise, which refuses the old leader there.
    assert_eq!(accepted(&replicas.get("broker-b").broker).await, PROMOTED);

    replicas.get_mut("broker-c").reachable = true;
    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route, false, None).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: Some("broker-c".to_string())
        }
    );
    assert_eq!(held(&leader).await, vec!["a", "w"]);
}

/// **A handshake that gave no identity never matches** one the report named.
#[tokio::test]
async fn a_fence_answer_without_a_storage_id_does_not_count() {
    let mut replicas = Replicas::new();
    replicas.get_mut("broker-b").storage = None;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key(),
        &route_naming_every_copy(),
        false,
        None,
    )
    .await;

    assert!(matches!(outcome, Outcome::Pending { .. }), "{outcome:?}");
}

/// **Nor does a member the report could not vouch for**, which placement
/// names with an empty identity: its leader had not asked it yet when it
/// reported, so whatever it answered since is not vouched for.
#[tokio::test]
async fn a_replica_named_without_an_identity_does_not_count() {
    let mut replicas = Replicas::new();
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    let mut route = route();
    route.fence_storage_ids = Some(Arc::new(
        [
            ("broker-b".to_string(), String::new()),
            ("broker-c".to_string(), storage_of("broker-c").to_hex()),
        ]
        .into(),
    ));

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route, false, None).await;

    assert!(matches!(outcome, Outcome::Pending { .. }), "{outcome:?}");
}

/// **A replica the assignment does not name counts**: one outside the old
/// set gave the old leader nothing to count, as a failover that replaces the
/// dead leader with a new copy brings in.
#[tokio::test]
async fn a_replica_outside_the_old_set_counts() {
    let mut replicas = Replicas::new();
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    let mut route = route();
    route.fence_storage_ids = Some(Arc::new(
        [("broker-c".to_string(), storage_of("broker-c").to_hex())].into(),
    ));

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route, false, None).await;

    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
}

/// **An unnamed replica that gives no identity counts as it always has**, a
/// build from before identities: nothing says either way, and holding it out
/// would keep a promotion closed for good while it is rolled.
#[tokio::test]
async fn an_unnamed_replica_without_a_storage_id_counts() {
    let mut replicas = Replicas::new();
    replicas
        .get("broker-b")
        .holds("broker-b", 4, 0, &["a", "w"])
        .await;
    replicas.get_mut("broker-b").storage = None;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    let mut route = route();
    route.fence_storage_ids = Some(Arc::new(
        [("broker-c".to_string(), storage_of("broker-c").to_hex())].into(),
    ));

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route, false, None).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: Some("broker-b".to_string())
        }
    );
    assert_eq!(held(&leader).await, vec!["a", "w"]);
}

/// **The copy the report named counts, and its tail is taken.**
#[tokio::test]
async fn a_fence_answer_from_the_reported_storage_id_counts() {
    let mut replicas = Replicas::new();
    replicas
        .get("broker-b")
        .holds("broker-b", 4, 0, &["a", "w"])
        .await;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key(),
        &route_naming_every_copy(),
        false,
        None,
    )
    .await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: Some("broker-b".to_string())
        }
    );
    assert_eq!(held(&leader).await, vec!["a", "w"]);
}

/// **An assignment that names no identities counts every answer**, as from a
/// control plane that predates them, or a change that is not a failover.
#[tokio::test]
async fn a_fence_without_reported_ids_counts_every_answer() {
    let mut replicas = Replicas::new();
    replicas.get_mut("broker-b").storage = None;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route(), false, None).await;

    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
}

/// **A promise away is blamed when another copy the route pins answered.**
/// Of five, b promised the generation to another node, c answered from a
/// copy the report did not name, and d and e are down. Placement keeps the
/// pins across renames until a fenced open, so c counts no more one
/// generation up, and the promise is reported as with the rest silent
/// (#2046).
#[tokio::test]
async fn a_promise_away_beside_a_pinned_out_copy_is_refused() {
    let mut replicas = super::groups::group_replicas(5, REQUIRED);
    replicas.ballots = true;
    for id in ["broker-b", "broker-c"] {
        super::groups::holds(
            &replicas,
            5,
            id,
            felix_broker::LogKind::Stream,
            4,
            &[Bytes::from_static(b"a")],
        )
        .await;
    }
    promised_to(&replicas, "broker-b", "broker-x").await;
    replicas.get_mut("broker-c").storage = Some(rotated());
    replicas.get_mut("broker-d").reachable = false;
    replicas.get_mut("broker-e").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    let mut route = super::groups::route_of_size(5);
    route.fence_storage_ids = Some(Arc::new(
        ["broker-b", "broker-c", "broker-d", "broker-e"]
            .into_iter()
            .map(|id| (id.to_string(), storage_of(id).to_hex()))
            .collect(),
    ));

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route, true, None).await;

    let Outcome::Refused { promised_to, .. } = outcome else {
        panic!("{outcome:?}");
    };
    assert_eq!(promised_to, "broker-x");
}

/// **A promise away is not blamed when another replica refused for its own
/// reason.** b promised the generation to another node and c refused the
/// fence for something a rename does not cure, so the fence waits and
/// reports c's reason instead of moving the shard up.
#[tokio::test]
async fn a_promise_away_beside_another_refusal_stays_pending() {
    let mut replicas = Replicas::new();
    replicas.ballots = true;
    replicas
        .get("broker-b")
        .holds("broker-b", 4, 0, &["a"])
        .await;
    promised_to(&replicas, "broker-b", "broker-d").await;
    replicas.get_mut("broker-c").refuses = Some("copy is short of the tail");
    let (leader, _dir) = leader_holding(4, &["a"]).await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route(), true, None).await;

    let Outcome::Pending { why, .. } = outcome else {
        panic!("{outcome:?}");
    };
    assert!(why.contains("broker-c refused: copy is short"), "{why}");
}
