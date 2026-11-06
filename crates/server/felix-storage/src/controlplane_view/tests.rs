//! A view that is not whole, or not this format, reads as absent.
use super::*;

const BODY: &[u8] = br#"{"node_id":"a"}"#;

#[test]
fn a_recorded_view_reads_back() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(read(dir.path()), None);
    record(dir.path(), BODY).unwrap();
    assert_eq!(read(dir.path()).as_deref(), Some(BODY));
    record(dir.path(), b"{}").unwrap();
    assert_eq!(read(dir.path()).as_deref(), Some(&b"{}"[..]));
}

#[test]
fn an_empty_body_round_trips() {
    assert_eq!(decode(&encode(b"")).as_deref(), Some(&b""[..]));
}

#[test]
fn every_truncation_reads_as_absent() {
    let bytes = encode(BODY);
    for len in 0..bytes.len() {
        assert_eq!(decode(&bytes[..len]), None, "cut at {len}");
    }
}

#[test]
fn trailing_bytes_read_as_absent() {
    let mut bytes = encode(BODY);
    bytes.push(0);
    assert_eq!(decode(&bytes), None);
}

#[test]
fn any_flipped_bit_reads_as_absent() {
    let bytes = encode(BODY);
    for at in 0..bytes.len() {
        for bit in 0..8 {
            let mut damaged = bytes.clone();
            damaged[at] ^= 1 << bit;
            assert_eq!(decode(&damaged), None, "byte {at} bit {bit}");
        }
    }
}

#[test]
fn an_unknown_version_reads_as_absent() {
    let mut bytes = encode(BODY);
    bytes[4..6].copy_from_slice(&(VERSION + 1).to_be_bytes());
    // A valid CRC, so only the version refuses it.
    let crc_at = bytes.len() - CRC_LEN;
    let crc = crate::segment::format::crc32(&[&bytes[..crc_at]]);
    bytes[crc_at..].copy_from_slice(&crc.to_be_bytes());
    assert_eq!(decode(&bytes), None);
}

#[test]
fn a_damaged_file_on_disk_reads_as_absent() {
    let dir = tempfile::tempdir().unwrap();
    record(dir.path(), BODY).unwrap();
    let path = dir.path().join(FILE_NAME);
    let mut bytes = std::fs::read(&path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    std::fs::write(&path, &bytes).unwrap();
    assert_eq!(read(dir.path()), None);
}

#[test]
fn a_leftover_temporary_is_never_read() {
    let dir = tempfile::tempdir().unwrap();
    let temporary = dir.path().join(FILE_NAME).with_extension("tmp");
    std::fs::write(&temporary, encode(BODY)).unwrap();
    assert_eq!(read(dir.path()), None);
    // And a later record goes over it.
    record(dir.path(), BODY).unwrap();
    assert_eq!(read(dir.path()).as_deref(), Some(BODY));
}

#[test]
fn a_failed_write_keeps_the_older_view() {
    let dir = tempfile::tempdir().unwrap();
    record(dir.path(), BODY).unwrap();
    // A directory where the temporary goes makes the write fail before
    // anything is renamed.
    std::fs::create_dir(dir.path().join(FILE_NAME).with_extension("tmp")).unwrap();
    assert!(record(dir.path(), b"{}").is_err());
    assert_eq!(read(dir.path()).as_deref(), Some(BODY));
}

#[test]
fn an_unreadable_file_reads_as_absent() {
    let dir = tempfile::tempdir().unwrap();
    // A directory in its place: the read fails with something other than
    // "not found", which must not stop a start.
    std::fs::create_dir(dir.path().join(FILE_NAME)).unwrap();
    assert_eq!(read(dir.path()), None);
}

#[test]
fn a_forgotten_view_reads_as_absent() {
    let dir = tempfile::tempdir().unwrap();
    forget(dir.path()).unwrap();
    record(dir.path(), BODY).unwrap();
    forget(dir.path()).unwrap();
    assert_eq!(read(dir.path()), None);
    assert!(!dir.path().join(FILE_NAME).exists());
}
