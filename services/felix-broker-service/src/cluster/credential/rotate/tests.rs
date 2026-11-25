//! Adopting — and declining — a credential written by something else.
use base64::Engine;

use super::*;

fn jwt(exp: i64) -> String {
    let encode = |value: &serde_json::Value| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value.to_string())
    };
    format!(
        "{}.{}.{}",
        encode(&serde_json::json!({"alg": "EdDSA", "typ": "JWT"})),
        encode(&serde_json::json!({"tid": "acme", "exp": exp, "sub": "node-1"})),
        "not-a-real-signature",
    )
}

/// The rotation this exists for: a sidecar rewrites the file, and the broker
/// is using the new token without a restart.
#[tokio::test]
async fn a_rewritten_token_file_is_adopted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("node.token");
    let original = jwt(now_secs() + 3_600);
    std::fs::write(&path, &original).expect("seed");

    let credential = NodeCredential::new(original.clone());
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run(
        path.clone(),
        credential.clone(),
        Duration::from_millis(10),
        shutdown.clone(),
    ));

    let rotated = jwt(now_secs() + 7_200);
    std::fs::write(&path, &rotated).expect("rotate");

    let adopted = tokio::time::timeout(Duration::from_secs(5), async {
        while credential.bearer().as_str() != rotated {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    shutdown.cancel();
    task.await.expect("task");

    assert!(
        adopted.is_ok(),
        "the rotated credential was never picked up"
    );
}

/// A rotation that writes a dead token must not be adopted.
///
/// Swapping a working credential for an expired one turns someone else's
/// rotation bug into an outage this broker caused — and the broker cannot
/// undo it, because the token it was running on is not written down anywhere
/// it can read back.
#[tokio::test]
async fn an_already_expired_replacement_is_declined() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("node.token");
    let good = jwt(now_secs() + 3_600);
    std::fs::write(&path, &good).expect("seed");

    let credential = NodeCredential::new(good.clone());
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run(
        path.clone(),
        credential.clone(),
        Duration::from_millis(10),
        shutdown.clone(),
    ));

    std::fs::write(&path, jwt(now_secs() - 1)).expect("rotate");
    tokio::time::sleep(Duration::from_millis(200)).await;
    shutdown.cancel();
    task.await.expect("task");

    assert_eq!(
        credential.bearer().as_str(),
        good,
        "an expired replacement was adopted over a credential that still works",
    );
}

/// A missing file is a rotation in progress, not a reason to stop.
#[tokio::test]
async fn a_momentarily_missing_file_keeps_the_current_credential() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("node.token");
    let good = jwt(now_secs() + 3_600);
    std::fs::write(&path, &good).expect("seed");

    let credential = NodeCredential::new(good.clone());
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run(
        path.clone(),
        credential.clone(),
        Duration::from_millis(10),
        shutdown.clone(),
    ));

    std::fs::remove_file(&path).expect("remove");
    tokio::time::sleep(Duration::from_millis(100)).await;

    // And the replacement that lands after it is picked up, so the gap did not
    // leave the watcher stuck.
    let rotated = jwt(now_secs() + 7_200);
    std::fs::write(&path, &rotated).expect("rotate");
    let adopted = tokio::time::timeout(Duration::from_secs(5), async {
        while credential.bearer().as_str() != rotated {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    shutdown.cancel();
    task.await.expect("task");

    assert!(
        adopted.is_ok(),
        "the watcher gave up after the file vanished"
    );
}

/// **A replacement in a format the broker cannot read is adopted anyway**,
/// with whitespace trimmed: the control plane, not the broker, decides what
/// a valid credential is. A blank rewrite is ignored.
#[tokio::test]
async fn an_opaque_replacement_is_adopted_and_a_blank_one_ignored() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("node.token");
    let original = jwt(now_secs() + 3_600);
    std::fs::write(&path, &original).expect("seed");

    let credential = NodeCredential::new(original.clone());
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run(
        path.clone(),
        credential.clone(),
        Duration::from_millis(10),
        shutdown.clone(),
    ));

    std::fs::write(&path, "  \n").expect("blank");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        credential.bearer().as_str(),
        original,
        "a blank file is not a credential"
    );

    std::fs::write(&path, "  opaque-vault-token\n").expect("rotate");
    let adopted = tokio::time::timeout(Duration::from_secs(5), async {
        while credential.bearer().as_str() != "opaque-vault-token" {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    shutdown.cancel();
    task.await.expect("task");
    assert!(adopted.is_ok(), "the opaque credential was never adopted");
}
