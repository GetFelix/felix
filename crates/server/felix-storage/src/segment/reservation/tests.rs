use tempfile::tempdir;

use super::*;

const MIB: u64 = 1024 * 1024;

fn reservation(limit: u64) -> (tempfile::TempDir, Reservation) {
    let dir = tempdir().expect("dir");
    let file = File::create(dir.path().join("f")).expect("create");
    let claimed = Reservation::initial_bytes(limit);
    (dir, Reservation::new(Arc::new(file), limit, claimed))
}

#[test]
fn a_segment_starts_with_a_megabyte_or_a_sixteenth_of_itself() {
    assert_eq!(Reservation::initial_bytes(256 * MIB), MIB);
    assert_eq!(Reservation::initial_bytes(4 * MIB), MIB / 4);
    assert_eq!(Reservation::initial_bytes(0), 0);
}

#[test]
fn the_reservation_doubles_once_writes_pass_half_of_it() {
    let (_dir, mut reservation) = reservation(64 * MIB);
    assert!(reservation.due(MIB / 2).is_none());

    let step = reservation.due(MIB / 2 + 1).expect("due");
    assert_eq!((step.from, step.to), (MIB, 2 * MIB));
    // Handed out once, even before it is applied.
    assert!(reservation.due(MIB / 2 + 1).is_none());

    let step = reservation.due(MIB + 1).expect("due");
    assert_eq!((step.from, step.to), (2 * MIB, 4 * MIB));
}

#[test]
fn the_reservation_stops_at_its_limit() {
    let (_dir, mut reservation) = reservation(3 * MIB);
    assert_eq!(reservation.due(MIB).expect("due").to, 2 * MIB);
    assert_eq!(reservation.due(2 * MIB).expect("due").to, 3 * MIB);
    assert!(reservation.due(3 * MIB).is_none());
}

#[test]
fn writes_that_outran_the_reservation_reserve_ahead_of_themselves() {
    let (_dir, mut reservation) = reservation(64 * MIB);
    let step = reservation.due(5 * MIB).expect("due");
    assert_eq!((step.from, step.to), (MIB, 10 * MIB));
}

#[test]
fn preallocation_off_never_reserves() {
    let (_dir, mut reservation) = reservation(0);
    assert!(reservation.due(MIB).is_none());
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn an_extension_handed_out_before_a_close_reserves_nothing() {
    use std::os::unix::fs::MetadataExt;

    let (dir, mut reservation) = reservation(64 * MIB);
    let step = reservation.due(MIB).expect("due");
    reservation.close();
    step.apply().expect("apply");
    let allocated = std::fs::metadata(dir.path().join("f"))
        .expect("meta")
        .blocks()
        * 512;
    assert!(
        allocated < MIB,
        "a closed reservation still took {allocated} bytes"
    );
}
