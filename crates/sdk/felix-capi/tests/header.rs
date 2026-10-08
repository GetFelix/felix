//! The checked-in `include/felix.h` matches what cbindgen generates from the
//! source, so C, Go and C# callers never compile against a stale signature.
//!
//! `FELIX_CAPI_BLESS=1` rewrites the header instead of comparing;
//! `task capi:header` does that.

use std::path::Path;

#[test]
fn the_checked_in_header_is_current() {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let config =
        cbindgen::Config::from_file(crate_dir.join("cbindgen.toml")).expect("read cbindgen.toml");
    let mut generated = Vec::new();
    cbindgen::Builder::new()
        .with_src(crate_dir.join("src/lib.rs"))
        .with_config(config)
        .generate()
        .expect("generate the header")
        .write(&mut generated);
    let generated = String::from_utf8(generated).expect("the header is UTF-8");

    let path = crate_dir.join("include/felix.h");
    if std::env::var_os("FELIX_CAPI_BLESS").is_some() {
        std::fs::write(&path, &generated).expect("write include/felix.h");
        return;
    }
    let checked_in = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        checked_in == generated,
        "include/felix.h is out of date with the source; run `task capi:header` and commit it"
    );
}
