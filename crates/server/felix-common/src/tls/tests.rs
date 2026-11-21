use std::path::Path;

use super::*;

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// Write a fresh self-signed certificate and key for `name` over `label`.
fn write_pair(dir: &Path, label: &str, name: &str) -> IdentityFiles {
    let cert = rcgen::generate_simple_self_signed(vec![name.to_string()]).expect("cert");
    let cert_path = dir.join(format!("{label}.pem"));
    let key_path = dir.join(format!("{label}.key.pem"));
    std::fs::write(&cert_path, cert.cert.pem()).expect("write cert");
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).expect("write key");
    IdentityFiles {
        cert_path: cert_path.display().to_string(),
        cert_var: "TEST_CERT",
        key_path: key_path.display().to_string(),
        key_var: "TEST_KEY",
    }
}

#[test]
fn reload_swaps_in_a_rotated_certificate_and_only_reports_a_change() {
    let dir = tempfile::tempdir().expect("tempdir");
    let files = write_pair(dir.path(), "a", "one.test");
    let identity = ReloadingIdentity::load(files.clone(), provider()).expect("load");
    let before = identity.chain();

    assert!(
        !identity.reload().expect("reload"),
        "nothing changed on disk"
    );

    // Same paths, new material: what a renewal looks like.
    write_pair(dir.path(), "a", "one.test");
    assert!(
        identity.reload().expect("reload"),
        "the rotation was missed"
    );
    assert_ne!(identity.chain(), before);
}

#[test]
fn a_failed_reload_keeps_the_current_identity() {
    let dir = tempfile::tempdir().expect("tempdir");
    let files = write_pair(dir.path(), "a", "one.test");
    let identity = ReloadingIdentity::load(files.clone(), provider()).expect("load");
    let before = identity.chain();

    // Mid-rotation: the certificate is gone.
    std::fs::remove_file(&files.cert_path).expect("remove");
    let err = identity.reload().expect_err("a missing file reloaded");
    assert!(err.to_string().contains("TEST_CERT"), "{err}");
    assert_eq!(identity.chain(), before);
}

#[test]
fn a_key_for_another_certificate_is_refused_without_printing_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut files = write_pair(dir.path(), "a", "one.test");
    let other = write_pair(dir.path(), "b", "two.test");
    let key_pem = std::fs::read_to_string(&other.key_path).expect("key");
    files.key_path = other.key_path;

    let rendered = ReloadingIdentity::load(files, provider())
        .expect_err("a mismatched key loaded")
        .to_string();
    assert!(rendered.contains("does not match"), "{rendered}");
    assert!(
        !rendered.contains(key_pem.trim()),
        "the key reached an error"
    );
}

#[test]
fn an_empty_bundle_is_an_error_not_an_empty_trust_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("empty.pem");
    std::fs::write(&path, "").expect("write");
    let err = load_roots("TEST_CA", &path.display().to_string()).expect_err("empty bundle");
    assert!(matches!(err, TlsFileError::Empty { .. }), "{err}");
}

#[test]
fn a_rotated_certificate_that_fails_the_check_is_refused_and_the_old_one_kept() {
    let dir = tempfile::tempdir().expect("tempdir");
    let files = write_pair(dir.path(), "a", "good.test");
    let check: IdentityCheck = Arc::new(|chain| {
        let (_, cert) =
            x509_parser::parse_x509_certificate(&chain[0]).map_err(|e| e.to_string())?;
        let names = format!("{:?}", cert.subject_alternative_name());
        if names.contains("good.test") {
            Ok(())
        } else {
            Err("not good.test".to_string())
        }
    });
    let identity =
        ReloadingIdentity::load_checked(files.clone(), provider(), Some(Arc::clone(&check)))
            .expect("load");
    let before = identity.chain();

    let bad = write_pair(dir.path(), "b", "bad.test");
    std::fs::copy(&bad.cert_path, &files.cert_path).expect("swap cert");
    std::fs::copy(&bad.key_path, &files.key_path).expect("swap key");
    let err = identity.reload().expect_err("the check refuses it");
    assert!(matches!(err, TlsFileError::Refused { .. }), "{err}");
    assert_eq!(identity.chain(), before, "the old certificate stays");

    // The same check fails startup.
    assert!(ReloadingIdentity::load_checked(files, provider(), Some(check)).is_err());
}
