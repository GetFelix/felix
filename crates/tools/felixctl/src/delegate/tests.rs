use std::ffi::OsString;
use std::path::PathBuf;

use super::*;

#[test]
fn the_flag_beats_the_environment_beats_the_path() {
    let flag = Some(PathBuf::from("/opt/flag/felix-broker"));
    let env = Some(OsString::from("/opt/env/felix-broker"));
    assert_eq!(
        program(&BROKER, flag, env.clone()),
        PathBuf::from("/opt/flag/felix-broker")
    );
    assert_eq!(
        program(&BROKER, None, env),
        PathBuf::from("/opt/env/felix-broker")
    );
    assert_eq!(
        program(&BROKER, None, Some(OsString::new())),
        PathBuf::from("felix-broker")
    );
    assert_eq!(
        program(&CONTROLPLANE, None, None),
        PathBuf::from("felix-controlplane")
    );
}

#[test]
fn a_missing_binary_says_how_to_point_at_it() {
    let err = run(
        &CONTROLPLANE,
        Some(PathBuf::from("/nonexistent/felix-controlplane")),
        "migrate",
        &[],
    )
    .expect_err("nothing to run");
    assert_eq!(crate::error::exit_for(&err), Exit::Usage);
    let message = format!("{err:#}");
    assert!(message.contains("--controlplane-bin"), "{message}");
    assert!(message.contains("FELIX_CONTROLPLANE_BIN"), "{message}");
}

#[cfg(unix)]
#[test]
fn a_signal_is_reported_the_way_a_shell_does() {
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(exit_code(std::process::ExitStatus::from_raw(9)), 128 + 9);
    assert_eq!(exit_code(std::process::ExitStatus::from_raw(3 << 8)), 3);
}
