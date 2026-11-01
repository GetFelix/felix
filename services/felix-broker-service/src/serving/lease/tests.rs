use base64::engine::general_purpose::STANDARD;
use felix_authz::PermissionMatcher;

use super::*;
use crate::serving::revocation::{Revocable, Revocations};

const TEMPLATE: &str = "stream.publish:stream:t1/prod/wl.{leases}.out";

fn seed_line(byte: u8) -> String {
    STANDARD.encode([byte; 32])
}

fn tokens() -> Arc<LeaseTokens> {
    Arc::new(LeaseTokens::from_keys(&seed_line(1), DEFAULT_MAX_TTL).expect("keys"))
}

fn now() -> i64 {
    secs(SystemTime::now())
}

fn caller(perms: &[&str]) -> AuthContext {
    let perms: Vec<String> = perms.iter().map(|perm| (*perm).to_string()).collect();
    AuthContext {
        tenant_id: "t1".to_string(),
        matcher: PermissionMatcher::from_strings(&perms).expect("perms"),
        token: "caller-token".to_string(),
        subject: "node-a".to_string(),
        actor: None,
        publisher: None,
        issued_at: 1_000,
        expires_at: now() + 3_600,
        lease_token: false,
        revocable: Revocable::new(
            Arc::new(Revocations::default()),
            "t1".to_string(),
            "node-a".to_string(),
            None,
            1_000,
        ),
    }
}

fn plan(caller: &AuthContext, key: &str) -> Result<LeaseMint, LeaseRefusal> {
    LeaseMint::plan(&tokens(), caller, "prod", "leases", key)
}

fn minted(lease: ForwardLease) -> (String, u64) {
    match lease {
        ForwardLease::Minted { token, expires_at } => (token, expires_at),
        other => panic!("not minted: {other:?}"),
    }
}

#[test]
fn the_key_file_takes_comments_blank_lines_and_either_base64() {
    let seed = [9u8; 32];
    let text = format!(
        "# lease keys\n\n{}\n  {}  \n{}\n",
        URL_SAFE_NO_PAD.encode(seed),
        STANDARD.encode([8u8; 32]),
        URL_SAFE.encode([7u8; 32]),
    );
    let tokens = LeaseTokens::from_keys(&text, DEFAULT_MAX_TTL).expect("keys");
    let caller = caller(&[TEMPLATE]);
    let mint = LeaseMint::plan(&Arc::new(tokens), &caller, "prod", "leases", "w7").expect("plan");
    let (token, _) = minted(mint.mint(1, None));
    let kid = jsonwebtoken::decode_header(&token).unwrap().kid.unwrap();
    assert_eq!(kid, LeaseKey::from_seed(seed).kid, "the first key signs");
    assert!(kid.starts_with(KID_PREFIX));
}

#[test]
fn a_malformed_key_file_is_refused_without_echoing_it() {
    let secret_looking = STANDARD.encode([5u8; 31]);
    for text in [
        String::new(),
        "# only a comment\n".to_string(),
        "not base64 at all!".to_string(),
        secret_looking.clone(),
    ] {
        let err = LeaseTokens::from_keys(&text, DEFAULT_MAX_TTL)
            .err()
            .unwrap_or_else(|| panic!("{text:?} should be refused"));
        let message = format!("{err:#}");
        assert!(
            secret_looking.is_empty() || !message.contains(&secret_looking),
            "{message}"
        );
    }
}

/// A token signed by any key in the file verifies, so a rotation can add the
/// new key everywhere before any broker signs with it.
#[test]
fn every_key_in_the_file_verifies() {
    let old = LeaseTokens::from_keys(&seed_line(1), DEFAULT_MAX_TTL).expect("keys");
    let caller = caller(&[TEMPLATE]);
    let (token, _) = minted(
        LeaseMint::plan(&Arc::new(old), &caller, "prod", "leases", "w7")
            .unwrap()
            .mint(1, None),
    );
    let rotated = LeaseTokens::from_keys(
        &format!("{}\n{}", seed_line(2), seed_line(1)),
        DEFAULT_MAX_TTL,
    )
    .expect("keys");
    rotated
        .verify(&TenantId::new("t1"), &token)
        .expect("old key verifies");
    let other = LeaseTokens::from_keys(&seed_line(3), DEFAULT_MAX_TTL).expect("keys");
    assert!(other.verify(&TenantId::new("t1"), &token).is_err());
    assert!(
        rotated.verify(&TenantId::new("t2"), &token).is_err(),
        "tenant"
    );
}

#[test]
fn a_lease_token_carries_the_expansion_the_lease_and_the_callers_iat() {
    let tokens = tokens();
    let caller = caller(&[TEMPLATE, "cache.write:cache:t1/prod/leases"]);
    let mint = LeaseMint::plan(&tokens, &caller, "prod", "leases", "w7").expect("plan");
    let (token, expires_at) = minted(mint.mint(42, None));
    assert!(is_lease_token(&token));
    let claims = tokens.verify(&TenantId::new("t1"), &token).expect("verify");
    assert_eq!(claims.iss, LEASE_ISSUER);
    assert_eq!(claims.sub, "node-a");
    assert_eq!(claims.iat, 1_000);
    assert_eq!(claims.exp.unsigned_abs(), expires_at);
    assert_eq!(
        claims.perms,
        vec!["stream.publish:stream:t1/prod/wl.w7.out"]
    );
    assert_eq!(
        claims.lease,
        Some(LeaseClaim {
            cache: "prod/leases".to_string(),
            key: "w7".to_string(),
            version: 42,
        })
    );
    assert!(claims.act.is_none());
    assert!(claims.jti.is_some());
}

#[test]
fn the_lifetime_is_the_shortest_of_entry_caller_and_maximum() {
    let tokens = tokens();
    let mut caller = caller(&[TEMPLATE]);
    let now = now();
    let mint = LeaseMint::plan(&tokens, &caller, "prod", "leases", "w7").unwrap();
    let (_, exp) = minted(mint.mint(1, None));
    assert!(exp.abs_diff(now.unsigned_abs() + 300) <= 1, "maximum");
    let (_, exp) = minted(mint.mint(1, Some(SystemTime::now() + Duration::from_millis(30_900))));
    assert!(exp.abs_diff(now.unsigned_abs() + 30) <= 1, "entry, floored");
    caller.expires_at = now + 10;
    let mint = LeaseMint::plan(&tokens, &caller, "prod", "leases", "w7").unwrap();
    let (_, exp) = minted(mint.mint(1, Some(SystemTime::now() + Duration::from_secs(60))));
    assert_eq!(exp, (now + 10).unsigned_abs(), "caller");
}

#[test]
fn no_token_when_it_would_already_be_expired() {
    let mut caller = caller(&[TEMPLATE]);
    caller.expires_at = now() - 5;
    let mint = plan(&caller, "w7").expect("planned before the write");
    assert!(matches!(mint.mint(1, None), ForwardLease::Refused(_)));
    let caller = self::caller(&[TEMPLATE]);
    let mint = plan(&caller, "w7").unwrap();
    assert!(matches!(
        mint.mint(1, Some(SystemTime::now())),
        ForwardLease::Refused(_)
    ));
}

#[test]
fn the_mint_is_refused_before_the_write() {
    let mut lease = caller(&[TEMPLATE]);
    lease.lease_token = true;
    assert_eq!(plan(&lease, "w7").err(), Some(LeaseRefusal::LeaseToken));

    let mut delegated = caller(&[TEMPLATE]);
    delegated.actor = Some("gateway".to_string());
    assert_eq!(plan(&delegated, "w7").err(), Some(LeaseRefusal::Delegated));

    let plain = caller(&["stream.publish:stream:t1/prod/*"]);
    assert_eq!(plan(&plain, "w7").err(), Some(LeaseRefusal::NoTemplate));

    let other_cache = caller(&["stream.publish:stream:t1/prod/wl.{other}.out"]);
    assert_eq!(
        plan(&other_cache, "w7").err(),
        Some(LeaseRefusal::NoTemplate)
    );

    let templated = caller(&[TEMPLATE]);
    for key in ["*", "a/b", "a:b", "{leases}", ""] {
        assert!(
            matches!(plan(&templated, key), Err(LeaseRefusal::Key(_))),
            "{key:?}"
        );
    }
}
