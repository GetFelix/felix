use super::*;

#[test]
fn alpn_is_offered_only_when_asked_for() {
    let roots = Arc::new(RootCertStore::empty());
    let quiet = rustls_config(Some(Arc::clone(&roots)), false).expect("config");
    assert!(quiet.alpn_protocols.is_empty());

    let offering = rustls_config(Some(roots), true).expect("config");
    assert_eq!(
        offering.alpn_protocols,
        vec![felix_wire::CLIENT_ALPN.to_vec()]
    );
    quic_client_config(None, true).expect("the platform trust store is usable");
}
