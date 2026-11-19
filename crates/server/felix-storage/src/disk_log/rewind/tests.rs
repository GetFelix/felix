use super::*;

#[test]
fn a_marker_reads_back_as_written() {
    let dir = tempfile::tempdir().expect("dir");
    assert_eq!(load(dir.path()).expect("load"), None);
    for rewind in [Rewind::Reset(100), Rewind::Cut(7)] {
        begin(dir.path(), rewind).expect("begin");
        assert_eq!(load(dir.path()).expect("load"), Some(rewind));
    }
    finish(dir.path()).expect("finish");
    assert_eq!(load(dir.path()).expect("load"), None);
}

#[test]
fn a_marker_that_does_not_decode_fails_the_load() {
    let dir = tempfile::tempdir().expect("dir");
    let mut bytes = Rewind::Reset(100).encode();
    bytes[10] ^= 1;
    std::fs::write(dir.path().join(FILE_NAME), bytes).expect("write");
    assert!(load(dir.path()).is_err());
}
