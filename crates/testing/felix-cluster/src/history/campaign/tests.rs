use anyhow::anyhow;

use super::*;

#[test]
fn a_failed_non_metadata_inject_does_not_fail_the_run() {
    let ok = after_failed_inject(
        FaultKind::Pause,
        anyhow!("stop timed out"),
        Ok(()),
        &"pause",
    );
    assert!(ok.is_ok());
    let heal_failed = after_failed_inject(
        FaultKind::Pause,
        anyhow!("stop timed out"),
        Err(anyhow!("heal failed")),
        &"pause",
    );
    assert!(heal_failed.is_ok());
}

#[test]
fn a_failed_metadata_inject_fails_the_run_with_the_inject_error() {
    let err = after_failed_inject(
        FaultKind::MetadataKill,
        anyhow!("no write accepted"),
        Ok(()),
        &"kill metadata 1",
    )
    .expect_err("a metadata inject failure is a bug");
    let text = format!("{err:#}");
    assert!(text.contains("kill metadata 1"), "{text}");
    assert!(text.contains("no write accepted"), "{text}");
}

#[test]
fn a_failed_metadata_heal_is_reported_with_the_inject_error() {
    let err = after_failed_inject(
        FaultKind::MetadataKill,
        anyhow!("no write accepted"),
        Err(anyhow!("members disagree")),
        &"kill metadata 1",
    )
    .expect_err("a heal failure is a bug");
    let text = format!("{err:#}");
    assert!(text.contains("heal kill metadata 1"), "{text}");
    assert!(text.contains("members disagree"), "{text}");
    assert!(text.contains("no write accepted"), "{text}");
}
