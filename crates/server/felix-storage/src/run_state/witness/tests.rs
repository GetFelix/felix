//! A directory is witnessed only as the last run on it left it, in the boot
//! that run wrote the witness in.
use super::*;

const BOOT: Option<BootId> = Some(BootId::from_bytes([1; 16]));
const REBOOTED: Option<BootId> = Some(BootId::from_bytes([2; 16]));

struct Dirs {
    root: tempfile::TempDir,
    shm: tempfile::TempDir,
}

impl Dirs {
    fn new() -> Self {
        Self {
            root: tempfile::tempdir().expect("root"),
            shm: tempfile::tempdir().expect("shm"),
        }
    }

    fn run(&self, token: u8) -> bool {
        witness(self.root.path(), Some(self.shm.path()), BOOT, [token; 16]).expect("witness")
    }

    fn witnessed_in(&self, boot: Option<BootId>) -> bool {
        witnessed(self.root.path(), Some(self.shm.path()), boot)
    }

    fn copy_token(&self) -> Vec<u8> {
        std::fs::read(self.root.path().join(TOKEN_FILE_NAME)).expect("read the token")
    }

    fn restore_token(&self, copy: &[u8]) {
        std::fs::write(self.root.path().join(TOKEN_FILE_NAME), copy).expect("restore");
    }
}

#[test]
fn the_directory_the_last_run_left_is_witnessed() {
    let dirs = Dirs::new();
    assert!(!dirs.witnessed_in(BOOT), "nothing ran yet");
    assert!(dirs.run(1));
    assert!(dirs.witnessed_in(BOOT));
}

/// A copy taken before the last run began holds that run's predecessor's
/// token.
#[test]
fn a_copy_from_before_the_last_run_is_not_witnessed() {
    let dirs = Dirs::new();
    dirs.run(1);
    let copy = dirs.copy_token();
    dirs.run(2);
    dirs.restore_token(&copy);
    assert!(!dirs.witnessed_in(BOOT));
}

#[test]
fn nothing_is_witnessed_after_a_reboot() {
    let dirs = Dirs::new();
    dirs.run(1);
    assert!(!dirs.witnessed_in(REBOOTED));
    assert!(!dirs.witnessed_in(None));
}

/// The same bytes at another path, as a directory copied or moved there is.
#[test]
fn another_directory_with_the_same_token_is_not_witnessed() {
    let dirs = Dirs::new();
    dirs.run(1);
    let elsewhere = tempfile::tempdir().expect("elsewhere");
    std::fs::write(elsewhere.path().join(TOKEN_FILE_NAME), dirs.copy_token()).expect("copy");
    assert!(!witnessed(elsewhere.path(), Some(dirs.shm.path()), BOOT));
}

/// A run that could not write its witness leaves none, so the copy the one
/// before it witnessed is not taken for the current directory.
#[test]
fn a_run_without_a_witness_removes_the_last_one() {
    let dirs = Dirs::new();
    dirs.run(1);
    let copy = dirs.copy_token();
    assert!(!witness(dirs.root.path(), Some(dirs.shm.path()), None, [2; 16]).expect("run"));
    assert!(!dirs.witnessed_in(BOOT));
    dirs.restore_token(&copy);
    assert!(!dirs.witnessed_in(BOOT));
}

#[test]
fn without_a_place_for_witnesses_nothing_is_witnessed() {
    let dirs = Dirs::new();
    assert!(!witness(dirs.root.path(), None, BOOT, [1; 16]).expect("run"));
    assert!(!witnessed(dirs.root.path(), None, BOOT));
    assert_eq!(dirs.copy_token(), hex(&[1; 16]).as_bytes());
}

#[test]
fn an_unreadable_token_is_not_witnessed() {
    let dirs = Dirs::new();
    dirs.run(1);
    dirs.restore_token(b"\xff\xfe");
    assert!(!dirs.witnessed_in(BOOT));
    std::fs::remove_file(dirs.root.path().join(TOKEN_FILE_NAME)).expect("remove");
    assert!(!dirs.witnessed_in(BOOT));
}
