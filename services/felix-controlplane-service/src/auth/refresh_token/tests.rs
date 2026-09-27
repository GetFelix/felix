//! The token format and the hash, which every backend depends on agreeing.
use super::*;

#[test]
fn a_token_round_trips_through_its_wire_form() {
    let wire = join("abc123", "s3cret");
    assert_eq!(split(&wire), Some(("abc123", "s3cret")));
}

#[test]
fn anything_that_is_not_a_token_is_refused_rather_than_guessed() {
    // Each of these must take the same path a wrong token takes. Accepting a
    // half-formed one — an empty secret, say — would let a caller present a
    // token id alone and have it checked against a record.
    for malformed in ["", ".", "no-separator", "id.", ".secret"] {
        assert_eq!(split(malformed), None, "{malformed:?} parsed as a token");
    }
}

#[test]
fn a_secret_with_a_separator_in_it_keeps_its_tail() {
    // Split on the *first* dot: the secret is opaque and may contain anything,
    // so splitting on the last would silently truncate it and every comparison
    // against the stored hash would fail for reasons nobody could see.
    assert_eq!(split("id.a.b.c"), Some(("id", "a.b.c")));
}

#[test]
fn the_hash_is_stable_and_not_the_secret() {
    let hash = hash_secret("s3cret");
    assert_eq!(hash, hash_secret("s3cret"), "the hash must be stable");
    assert_ne!(hash, "s3cret");
    assert_ne!(hash, hash_secret("s3cret "));
    assert_eq!(hash.len(), 64, "sha-256, hex encoded");
}

#[test]
fn liveness_needs_all_three_conditions() {
    let base = RefreshToken {
        token_id: "t".into(),
        tenant_id: "acme".into(),
        principal_id: "p".into(),
        groups: vec![],
        secret_hash: hash_secret("s"),
        family_id: "f".into(),
        issued_at_secs: 0,
        expires_at_secs: 100,
        used: false,
        revoked: false,
        narrowing: None,
    };
    assert!(base.is_live(50));

    assert!(!base.is_live(100), "expiry is exclusive at the deadline");
    assert!(
        !RefreshToken {
            used: true,
            ..base.clone()
        }
        .is_live(50)
    );
    assert!(
        !RefreshToken {
            revoked: true,
            ..base.clone()
        }
        .is_live(50)
    );
}
