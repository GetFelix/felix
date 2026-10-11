use std::time::Duration;

use super::*;

fn token(
    revocations: &Arc<Revocations>,
    subject: &str,
    actor: Option<&str>,
    issued_at: i64,
) -> Revocable {
    Revocable::new(
        Arc::clone(revocations),
        "t1".to_string(),
        subject.to_string(),
        actor.map(str::to_string),
        issued_at,
    )
}

#[test]
fn a_token_issued_at_or_before_the_cutoff_is_revoked() {
    let revocations = Arc::new(Revocations::default());
    revocations.revoke("t1", "alice", 100);
    assert!(token(&revocations, "alice", None, 99).is_revoked());
    assert!(token(&revocations, "alice", None, 100).is_revoked());
    assert!(!token(&revocations, "alice", None, 101).is_revoked());
    assert!(!token(&revocations, "bob", None, 99).is_revoked());
    assert!(!revocations.revokes("t2", "alice", None, 99));
}

#[test]
fn an_earlier_cutoff_does_not_move_it_back() {
    let revocations = Arc::new(Revocations::default());
    revocations.revoke("t1", "alice", 100);
    revocations.revoke("t1", "alice", 50);
    assert!(token(&revocations, "alice", None, 80).is_revoked());
}

#[test]
fn the_actor_and_the_whole_tenant_count() {
    let revocations = Arc::new(Revocations::default());
    revocations.revoke("t1", "gateway", 100);
    assert!(token(&revocations, "alice", Some("gateway"), 90).is_revoked());
    assert!(!token(&revocations, "alice", None, 90).is_revoked());
    revocations.revoke("t1", ALL_PRINCIPALS, 200);
    assert!(token(&revocations, "bob", None, 150).is_revoked());
}

#[tokio::test]
async fn a_waiter_wakes_for_its_own_revocation_only() {
    let revocations = Arc::new(Revocations::default());
    let alice = token(&revocations, "alice", None, 10);
    let waiting = tokio::spawn(async move { alice.revoked().await });
    tokio::task::yield_now().await;
    revocations.revoke("t1", "bob", 100);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiting.is_finished(), "someone else's revocation");
    revocations.revoke("t1", "alice", 100);
    tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .expect("woken")
        .expect("joined");
}
