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

/// Write a self-signed pair for `names` over `files`' paths, valid until
/// the start of `year`.
fn overwrite(files: &IdentityFiles, names: &[&str], year: i32) {
    let key = rcgen::KeyPair::generate().expect("key");
    let mut params = rcgen::CertificateParams::new(
        names
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>(),
    )
    .expect("params");
    params.not_after = rcgen::date_time_ymd(year, 1, 1);
    let cert = params.self_signed(&key).expect("cert");
    std::fs::write(&files.cert_path, cert.pem()).expect("write cert");
    std::fs::write(&files.key_path, key.serialize_pem()).expect("write key");
}

#[test]
fn a_rotated_certificate_that_is_expired_or_drops_a_name_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let files = write_pair(dir.path(), "a", "one.test");
    overwrite(&files, &["one.test", "127.0.0.1"], 2099);
    let identity = ReloadingIdentity::load(files.clone(), provider())
        .expect("load")
        .keep_names();
    let before = identity.chain();

    for (names, year, why) in [
        (&["one.test", "127.0.0.1"][..], 2020, "expired"),
        (&["one.test"][..], 2099, "127.0.0.1"),
        (&["two.test", "127.0.0.1"][..], 2099, "one.test"),
    ] {
        overwrite(&files, names, year);
        let err = identity.reload().expect_err("refused");
        assert!(
            matches!(err, TlsFileError::Refused { .. }) && err.to_string().contains(why),
            "{err}"
        );
        assert_eq!(identity.chain(), before, "the old certificate stays");
    }

    // More names and a later end are fine.
    overwrite(&files, &["one.test", "127.0.0.1", "extra.test"], 2100);
    assert!(identity.reload().expect("accepted"));
}

#[test]
fn verify_chain_checks_the_issuer_and_the_clock() {
    let issuer = |year| {
        let key = rcgen::KeyPair::generate().expect("ca key");
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let cert = params.self_signed(&key).expect("ca");
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.der().clone()).expect("root");
        let leaf_key = rcgen::KeyPair::generate().expect("key");
        let mut leaf = rcgen::CertificateParams::new(vec!["one.test".to_string()]).expect("params");
        leaf.not_after = rcgen::date_time_ymd(year, 1, 1);
        let leaf = leaf
            .signed_by(&leaf_key, &rcgen::Issuer::new(params, key))
            .expect("leaf");
        (roots, leaf.der().clone())
    };
    let (roots, good) = issuer(2099);
    let (other_roots, _) = issuer(2099);
    let (expired_roots, expired) = issuer(2020);
    for presenter in [Presenter::Client, Presenter::Server] {
        verify_chain(std::slice::from_ref(&good), &roots, &provider(), presenter)
            .expect("verifies");
        verify_chain(
            std::slice::from_ref(&good),
            &other_roots,
            &provider(),
            presenter,
        )
        .expect_err("wrong CA");
        verify_chain(
            std::slice::from_ref(&expired),
            &expired_roots,
            &provider(),
            presenter,
        )
        .expect_err("expired");
    }
}
