use super::build;

struct Files {
    dir: tempfile::TempDir,
}

impl Files {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let ca = rcgen::generate_simple_self_signed(vec!["felix-ca".to_string()]).unwrap();
        let client = rcgen::generate_simple_self_signed(vec!["felix-client".to_string()]).unwrap();
        let other = rcgen::generate_simple_self_signed(vec!["other".to_string()]).unwrap();
        let write = |name: &str, body: String| std::fs::write(dir.path().join(name), body).unwrap();
        write("ca.pem", ca.cert.pem());
        write("client.pem", client.cert.pem());
        write("client.key", client.signing_key.serialize_pem());
        write("other.key", other.signing_key.serialize_pem());
        write("empty.pem", String::new());
        Self { dir }
    }

    fn path(&self, name: &str) -> String {
        self.dir.path().join(name).display().to_string()
    }

    fn refused(&self, cert: Option<&str>, key: Option<&str>) -> String {
        let ca = self.path("ca.pem");
        let (cert, key) = (cert.map(|n| self.path(n)), key.map(|n| self.path(n)));
        build(Some(&ca), cert.as_deref(), key.as_deref(), false)
            .err()
            .expect("refused")
    }
}

#[test]
fn a_certificate_and_key_build_a_config() {
    let files = Files::new();
    let (ca, cert, key) = (
        files.path("ca.pem"),
        files.path("client.pem"),
        files.path("client.key"),
    );
    build(Some(&ca), Some(&cert), Some(&key), false).expect("with a certificate");
    build(Some(&ca), None, None, false).expect("without one");
}

#[test]
fn a_certificate_without_its_key_is_refused() {
    let files = Files::new();
    for (cert, key) in [(Some("client.pem"), None), (None, Some("client.key"))] {
        let message = files.refused(cert, key);
        assert!(message.contains("given together"), "{message}");
    }
}

#[test]
fn an_unusable_file_is_named() {
    let files = Files::new();
    let message = files.refused(Some("missing.pem"), Some("client.key"));
    assert!(message.contains("missing.pem"), "{message}");
    let message = files.refused(Some("empty.pem"), Some("client.key"));
    assert!(message.contains("empty.pem"), "{message}");
    // A certificate file is not a key file.
    let message = files.refused(Some("client.pem"), Some("client.pem"));
    assert!(message.contains("client.pem"), "{message}");
    let message = files.refused(Some("client.pem"), Some("missing.key"));
    assert!(message.contains("missing.key"), "{message}");
}

#[test]
fn a_key_for_another_certificate_is_refused() {
    let files = Files::new();
    let message = files.refused(Some("client.pem"), Some("other.key"));
    assert!(message.contains("client certificate"), "{message}");
}
