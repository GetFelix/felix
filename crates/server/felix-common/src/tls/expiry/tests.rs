use super::*;

// 2030-01-01, 2031-01-01 and 2032-01-01 at 00:00:00Z.
const Y2030: i64 = 1_893_456_000;
const Y2031: i64 = 1_924_992_000;
const Y2032: i64 = 1_956_528_000;

struct Ca {
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
    der: CertificateDer<'static>,
}

fn params(name: &str, not_after: (i32, u8, u8)) -> rcgen::CertificateParams {
    let mut params = rcgen::CertificateParams::new(vec!["node".to_string()]).expect("params");
    // rcgen gives every certificate the same name otherwise, and paths are
    // built by name.
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, name);
    params.not_after = rcgen::date_time_ymd(not_after.0, not_after.1, not_after.2);
    params
}

fn self_signed_ca(name: &str, not_after: (i32, u8, u8)) -> Ca {
    let mut params = params(name, not_after);
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let key = rcgen::KeyPair::generate().expect("key");
    let der = params.self_signed(&key).expect("cert").der().clone();
    Ca {
        issuer: rcgen::Issuer::new(params, key),
        der,
    }
}

fn intermediate(name: &str, not_after: (i32, u8, u8), by: &Ca) -> Ca {
    let mut params = params(name, not_after);
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let key = rcgen::KeyPair::generate().expect("key");
    let der = params
        .signed_by(&key, &by.issuer)
        .expect("cert")
        .der()
        .clone();
    Ca {
        issuer: rcgen::Issuer::new(params, key),
        der,
    }
}

fn leaf(not_after: (i32, u8, u8), by: &Ca) -> CertificateDer<'static> {
    let key = rcgen::KeyPair::generate().expect("key");
    params("leaf", not_after)
        .signed_by(&key, &by.issuer)
        .expect("cert")
        .der()
        .clone()
}

fn expiry(anchors: &[&Ca]) -> ChainExpiry {
    let mut roots = RootCertStore::empty();
    for anchor in anchors {
        roots.add(anchor.der.clone()).expect("anchor");
    }
    ChainExpiry::new(
        Arc::new(roots),
        &rustls::crypto::aws_lc_rs::default_provider(),
        Presenter::Client,
    )
}

#[test]
fn an_expired_trust_anchor_in_the_chain_does_not_count() {
    let root = self_signed_ca("root", (2020, 1, 1));
    let inter = intermediate("inter", (2032, 1, 1), &root);
    let chain = [
        leaf((2031, 1, 1), &inter),
        inter.der.clone(),
        root.der.clone(),
    ];
    assert_eq!(expiry(&[&root]).not_after(&chain).unwrap(), Some(Y2031));
}

#[test]
fn an_intermediate_on_the_path_bounds_the_chain() {
    let root = self_signed_ca("root", (2020, 1, 1));
    let inter = intermediate("inter", (2030, 1, 1), &root);
    let chain = [
        leaf((2031, 1, 1), &inter),
        inter.der.clone(),
        root.der.clone(),
    ];
    assert_eq!(expiry(&[&root]).not_after(&chain).unwrap(), Some(Y2030));
}

#[test]
fn an_extra_certificate_off_the_path_is_ignored() {
    let root = self_signed_ca("root", (2040, 1, 1));
    let inter = intermediate("inter", (2032, 1, 1), &root);
    // Issued by another CA, so neither self-signed nor an anchor: only the
    // path says it is not part of this chain.
    let other = self_signed_ca("other", (2040, 1, 1));
    let stray = intermediate("stray", (2020, 1, 1), &other);
    let chain = [leaf((2031, 1, 1), &inter), inter.der.clone(), stray.der];
    assert_eq!(expiry(&[&root]).not_after(&chain).unwrap(), Some(Y2031));
}

#[test]
fn an_intermediate_that_is_itself_an_anchor_does_not_count() {
    let root = self_signed_ca("root", (2040, 1, 1));
    let inter = intermediate("inter", (2020, 1, 1), &root);
    let chain = [leaf((2031, 1, 1), &inter), inter.der.clone()];
    assert_eq!(expiry(&[&inter]).not_after(&chain).unwrap(), Some(Y2031));
}

#[test]
fn without_a_path_every_possible_intermediate_counts_but_no_anchor() {
    let root = self_signed_ca("root", (2020, 1, 1));
    let inter = intermediate("inter", (2030, 1, 1), &root);
    let chain = [
        leaf((2031, 1, 1), &inter),
        inter.der.clone(),
        root.der.clone(),
    ];
    assert_eq!(expiry(&[]).not_after(&chain).unwrap(), Some(Y2030));
    let unrelated = self_signed_ca("unrelated", (2040, 1, 1));
    assert_eq!(
        expiry(&[&unrelated]).not_after(&chain).unwrap(),
        Some(Y2030)
    );
    // The intermediate is an anchor here, though the path to it fails.
    let expired_leaf = [leaf((2020, 1, 1), &inter), inter.der.clone()];
    assert_eq!(
        expiry(&[&inter]).not_after(&expired_leaf).unwrap(),
        Some(1_577_836_800)
    );
}

#[test]
fn the_leaf_alone_bounds_a_chain_with_nothing_else() {
    let root = self_signed_ca("root", (2040, 1, 1));
    let chain = [leaf((2032, 1, 1), &root)];
    assert_eq!(expiry(&[&root]).not_after(&chain).unwrap(), Some(Y2032));
    assert_eq!(expiry(&[&root]).not_after(&[]).unwrap(), None);
}

#[test]
fn an_unreadable_leaf_is_refused_and_an_unreadable_extra_is_ignored() {
    let root = self_signed_ca("root", (2040, 1, 1));
    let junk = CertificateDer::from(&b"junk"[..]);
    assert!(
        expiry(&[&root])
            .not_after(std::slice::from_ref(&junk))
            .is_err()
    );
    let chain = [leaf((2031, 1, 1), &root), junk];
    assert_eq!(expiry(&[&root]).not_after(&chain).unwrap(), Some(Y2031));
    assert_eq!(expiry(&[]).not_after(&chain).unwrap(), Some(Y2031));
}

#[test]
fn a_spread_close_is_never_after_not_after() {
    for _ in 0..1_000 {
        let at = spread_before(Y2030, Duration::from_secs(30));
        assert!((Y2030 - 30..=Y2030).contains(&at), "{at}");
    }
    assert_eq!(spread_before(Y2030, Duration::ZERO), Y2030);
    assert_eq!(spread_before(i64::MIN, Duration::from_secs(30)), i64::MIN);
    // Past, or too close to spread.
    assert_eq!(spread_before(0, Duration::from_secs(30)), 0);
    let soon = i64::try_from(
        crate::clock::system_time()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
    .unwrap()
        + 2;
    assert_eq!(spread_before(soon, Duration::from_secs(30)), soon);
}

#[test]
fn a_past_instant_is_due_now() {
    assert!(until_unix(0).is_zero());
    assert!(until_unix(i64::MIN).is_zero());
    assert!(!until_unix(i64::MAX).is_zero());
}
