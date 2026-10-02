use super::*;

#[test]
fn alpn_is_offered_only_when_asked_for() {
    let roots = Arc::new(RootCertStore::empty());
    let quiet = rustls_config(Some(Arc::clone(&roots)), None, false).expect("config");
    assert!(quiet.alpn_protocols.is_empty());

    let offering = rustls_config(Some(roots), None, true).expect("config");
    assert_eq!(
        offering.alpn_protocols,
        vec![felix_wire::CLIENT_ALPN.to_vec()]
    );
    quic_client_config(None, true).expect("the platform trust store is usable");
}

/// A CA certificate file and a client certificate and key, as PEM files.
struct PemFiles {
    dir: tempfile::TempDir,
}

impl PemFiles {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let ca = rcgen::generate_simple_self_signed(vec!["felix-ca".to_string()]).expect("ca");
        let client =
            rcgen::generate_simple_self_signed(vec!["felix-client".to_string()]).expect("client");
        std::fs::write(dir.path().join("ca.pem"), ca.cert.pem()).expect("write ca");
        std::fs::write(dir.path().join("client.pem"), client.cert.pem()).expect("write cert");
        std::fs::write(
            dir.path().join("client.key"),
            client.signing_key.serialize_pem(),
        )
        .expect("write key");
        Self { dir }
    }

    fn path(&self, name: &str) -> std::path::PathBuf {
        self.dir.path().join(name)
    }
}

#[test]
fn a_client_certificate_is_presented_from_pem_files() {
    let files = PemFiles::new();
    let roots = root_store_from_pem_file(files.path("ca.pem")).expect("roots");
    assert_eq!(roots.len(), 1);
    let identity =
        ClientIdentity::from_pem_files(files.path("client.pem"), files.path("client.key"))
            .expect("identity");
    assert!(!format!("{identity:?}").contains("key:"));

    let tls = rustls_config(Some(Arc::new(roots.clone())), Some(identity), true).expect("config");
    assert!(tls.client_auth_cert_resolver.has_certs());
    assert_eq!(tls.alpn_protocols, vec![felix_wire::CLIENT_ALPN.to_vec()]);

    let without = rustls_config(Some(Arc::new(roots.clone())), None, false).expect("config");
    assert!(!without.client_auth_cert_resolver.has_certs());

    let identity =
        ClientIdentity::from_pem_files(files.path("client.pem"), files.path("client.key"))
            .expect("identity");
    quic_client_config_with_identity(Some(Arc::new(roots)), identity, false).expect("quic config");
}

#[test]
fn unusable_pem_files_are_reported_by_name() {
    let files = PemFiles::new();
    let empty = files.path("empty.pem");
    std::fs::write(&empty, "").expect("write");
    let err = root_store_from_pem_file(&empty).expect_err("no certificates");
    assert!(format!("{err:#}").contains("empty.pem"), "{err:#}");

    // A certificate file is not a key file.
    let err = ClientIdentity::from_pem_files(files.path("client.pem"), files.path("client.pem"))
        .expect_err("no key");
    assert!(format!("{err:#}").contains("client.pem"), "{err:#}");

    let err = root_store_from_pem_file(files.path("missing.pem")).expect_err("missing");
    assert!(format!("{err:#}").contains("missing.pem"), "{err:#}");
}
