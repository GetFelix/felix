use super::*;

fn floor(floor: Offset, headers: &'static [u8]) -> CarriedFloor {
    CarriedFloor {
        offset: floor + 2,
        floor,
        headers: Bytes::from_static(headers),
    }
}

#[test]
fn a_lower_floor_never_replaces_a_higher_one() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(load(dir.path()).unwrap(), None);
    assert_eq!(
        raise(dir.path(), floor(4, b"four")).unwrap(),
        floor(4, b"four")
    );
    assert_eq!(
        raise(dir.path(), floor(3, b"three")).unwrap(),
        floor(4, b"four")
    );
    assert_eq!(load(dir.path()).unwrap(), Some(floor(4, b"four")));
    assert_eq!(raise(dir.path(), floor(9, b"")).unwrap(), floor(9, b""));
    assert_eq!(load(dir.path()).unwrap(), Some(floor(9, b"")));
}

#[test]
fn a_damaged_file_fails_the_load() {
    let dir = tempfile::tempdir().unwrap();
    raise(dir.path(), floor(4, b"four")).unwrap();
    let path = dir.path().join(FILE_NAME);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[18] ^= 1;
    std::fs::write(&path, bytes).unwrap();
    assert!(load(dir.path()).is_err());
}
