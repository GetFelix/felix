use super::*;

fn cert_ending(not_after: (i32, u8, u8)) -> CertificateDer<'static> {
    let mut params = rcgen::CertificateParams::new(vec!["node".to_string()]).expect("params");
    params.not_after = rcgen::date_time_ymd(not_after.0, not_after.1, not_after.2);
    let key = rcgen::KeyPair::generate().expect("key");
    params.self_signed(&key).expect("cert").der().clone()
}

#[test]
fn a_chain_is_valid_until_its_shortest_lived_certificate_ends() {
    let leaf = cert_ending((2031, 1, 1));
    let intermediate = cert_ending((2030, 1, 1));
    // 2030-01-01T00:00:00Z
    assert_eq!(
        chain_not_after(&[leaf.clone(), intermediate]),
        Some(1_893_456_000)
    );
    assert_eq!(chain_not_after(&[leaf]), Some(1_924_992_000));
    assert_eq!(chain_not_after(&[]), None);
    assert_eq!(chain_not_after(&[CertificateDer::from(&b"junk"[..])]), None);
}

#[test]
fn a_past_instant_is_due_now() {
    assert!(until_unix(0).is_zero());
    assert!(until_unix(i64::MIN).is_zero());
    assert!(!until_unix(i64::MAX).is_zero());
}
