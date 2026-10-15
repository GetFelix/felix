use super::*;

fn header() -> Header {
    Header {
        store: STORE_CACHE,
        covered_through: 42,
        log_bytes: 9000,
        last_checksum: 0xdead_beef,
    }
}

fn entry(offset: u64) -> IndexEntry {
    IndexEntry {
        offset,
        version: offset + 1,
        expires_at_millis: offset * 10,
        bytes: 100 + offset as u32,
    }
}

fn written(dir: &Path, keys: &[&str]) -> Entries {
    let mut entries: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| (cache_key(key), entry(i as u64)))
        .collect();
    write_temporary(dir, &header(), &mut entries).expect("write");
    install(dir).expect("install");
    entries
}

type Entries = Vec<(Vec<u8>, IndexEntry)>;

fn read_all(dir: &Path) -> Result<Option<(Header, Entries)>, Rejected> {
    let Some(mut reader) = Reader::open(dir)? else {
        return Ok(None);
    };
    let header = reader.header();
    let mut entries = Vec::new();
    while let Some(entry) = reader.next_entry()? {
        entries.push(entry);
    }
    reader.finish()?;
    Ok(Some((header, entries)))
}

#[test]
fn a_snapshot_round_trips_sorted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let entries = written(dir.path(), &["zeta", "a", "mid", ""]);
    let (header_read, read) = read_all(dir.path()).expect("valid").expect("present");
    assert_eq!(header_read, header());
    assert_eq!(read, entries, "written in composite-key order");
    assert!(read.windows(2).all(|pair| pair[0].0 < pair[1].0));
}

#[test]
fn cache_keys_round_trip_and_reject_other_kinds() {
    for key in ["", "k", "a longer key with spaces", "ключ"] {
        assert_eq!(parse_cache_key(&cache_key(key)), Some(key));
    }
    let mut hash = cache_key("k");
    hash[0] = 1;
    assert_eq!(parse_cache_key(&hash), None);
    let mut long = cache_key("k");
    long.push(b'x');
    assert_eq!(parse_cache_key(&long), None);
}

#[test]
fn no_file_is_not_a_rejection() {
    let dir = tempfile::tempdir().expect("tempdir");
    assert!(matches!(read_all(dir.path()), Ok(None)));
}

#[test]
fn an_uninstalled_temporary_is_not_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut entries = vec![(cache_key("k"), entry(1))];
    write_temporary(dir.path(), &header(), &mut entries).expect("write");
    assert!(matches!(read_all(dir.path()), Ok(None)));
}

#[test]
fn every_flipped_byte_is_caught() {
    let dir = tempfile::tempdir().expect("tempdir");
    written(dir.path(), &["one", "two", "three"]);
    let path = dir.path().join(FILE_NAME);
    let good = std::fs::read(&path).expect("read");
    for at in 0..good.len() {
        let mut bad = good.clone();
        bad[at] ^= 0x40;
        std::fs::write(&path, &bad).expect("write");
        assert!(
            read_all(dir.path()).is_err(),
            "a flip at byte {at} of {} went unnoticed",
            good.len()
        );
    }
}

#[test]
fn a_short_or_long_file_is_caught() {
    let dir = tempfile::tempdir().expect("tempdir");
    written(dir.path(), &["one", "two"]);
    let path = dir.path().join(FILE_NAME);
    let good = std::fs::read(&path).expect("read");
    for len in 0..good.len() {
        std::fs::write(&path, &good[..len]).expect("write");
        assert!(read_all(dir.path()).is_err(), "cut to {len} bytes");
    }
    let mut long = good.clone();
    long.extend_from_slice(&[0; 8]);
    std::fs::write(&path, &long).expect("write");
    assert!(read_all(dir.path()).is_err(), "trailing bytes");
}

#[test]
fn remove_takes_both_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    written(dir.path(), &["k"]);
    let mut entries = vec![(cache_key("k"), entry(1))];
    write_temporary(dir.path(), &header(), &mut entries).expect("write");
    remove(dir.path()).expect("remove");
    remove(dir.path()).expect("removing nothing is fine");
    assert_eq!(std::fs::read_dir(dir.path()).expect("list").count(), 0);
}
