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
}

#[test]
fn a_missing_binary_says_how_to_point_at_it() {
    let err = run(
        &BROKER,
        Some(PathBuf::from("/nonexistent/felix-broker")),
        &["inspect"],
        &[OsString::from("inspect")],
    )
    .expect_err("nothing to run");
    assert_eq!(crate::error::exit_for(&err), Exit::Usage);
    let message = format!("{err:#}");
    assert!(message.contains("--broker-bin"), "{message}");
    assert!(message.contains("FELIX_BROKER_BIN"), "{message}");
}

#[cfg(unix)]
#[test]
fn a_signal_is_reported_the_way_a_shell_does() {
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(exit_code(std::process::ExitStatus::from_raw(9)), 128 + 9);
    assert_eq!(exit_code(std::process::ExitStatus::from_raw(3 << 8)), 3);
}

#[test]
fn an_unknown_subcommand_is_a_usage_error() {
    let err = run(&BROKER, None, &["inspect"], &[OsString::from("bogus")]).expect_err("refused");
    assert_eq!(crate::error::exit_for(&err), Exit::Usage);
}
