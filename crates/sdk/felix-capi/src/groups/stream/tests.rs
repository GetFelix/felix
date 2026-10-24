use std::ffi::{CStr, c_char, c_void};
use std::sync::Mutex;

use super::*;
use crate::status::FELIX_STATUS_AUTH;

type Seen = (FelixStatus, Option<String>, Option<Vec<(u64, bool)>>);

unsafe extern "C" fn positioned(
    user_data: *mut c_void,
    status: FelixStatus,
    message: *const c_char,
    positions: *const FelixGroupPosition,
    count: usize,
) {
    // SAFETY: every test passes a live `Mutex<Vec<Seen>>`.
    let calls = unsafe { &*user_data.cast::<Mutex<Vec<Seen>>>() };
    let rows = (!positions.is_null()).then(|| {
        // SAFETY: `count` entries, valid while the callback runs.
        unsafe { std::slice::from_raw_parts(positions, count) }
            .iter()
            .map(|p| (p.offset, p.moved))
            .collect()
    });
    let message = (!message.is_null()).then(|| {
        // SAFETY: NUL-terminated, valid during the callback.
        unsafe { CStr::from_ptr(message) }
            .to_string_lossy()
            .into_owned()
    });
    calls.lock().unwrap().push((status, message, rows));
}

type SeenInfos = (FelixStatus, Option<Vec<(bool, u64, u64)>>);

unsafe extern "C" fn described(
    user_data: *mut c_void,
    status: FelixStatus,
    _message: *const c_char,
    infos: *const FelixGroupInfo,
    count: usize,
) {
    // SAFETY: every test passes a live `Mutex<Vec<SeenInfos>>`.
    let calls = unsafe { &*user_data.cast::<Mutex<Vec<SeenInfos>>>() };
    let rows = (!infos.is_null()).then(|| {
        // SAFETY: `count` entries, valid while the callback runs.
        unsafe { std::slice::from_raw_parts(infos, count) }
            .iter()
            .map(|i| (i.has_committed, i.committed, i.dead_letters))
            .collect()
    });
    calls.lock().unwrap().push((status, rows));
}

/// One entry per shard, in shard order, each with its own `moved`.
#[test]
fn positions_arrive_indexed_by_shard() {
    let calls = Mutex::new(Vec::<Seen>::new());
    let data = (&raw const calls).cast_mut().cast::<c_void>();
    Completion::<Positions>::new(Some(positioned), data)
        .unwrap()
        .ok(Positions(vec![
            GroupPosition {
                offset: 4,
                moved: true,
            },
            GroupPosition {
                offset: 9,
                moved: false,
            },
        ]));
    Completion::<Positions>::new(Some(positioned), data)
        .unwrap()
        .fail(Failure {
            status: FELIX_STATUS_AUTH,
            message: "denied".to_string(),
        });
    let calls = calls.into_inner().unwrap();
    assert_eq!(calls[0].0, FELIX_STATUS_OK);
    assert_eq!(calls[0].2, Some(vec![(4, true), (9, false)]));
    assert_eq!(calls[1].0, FELIX_STATUS_AUTH);
    assert_eq!(calls[1].1.as_deref(), Some("denied"));
    assert_eq!(calls[1].2, None);
}

#[test]
fn infos_arrive_indexed_by_shard_and_a_failure_has_none() {
    let calls = Mutex::new(Vec::<SeenInfos>::new());
    let data = (&raw const calls).cast_mut().cast::<c_void>();
    let info = |committed, dead_letters| GroupInfo {
        committed,
        tail: 10,
        in_flight: 0,
        owed: 0,
        dead_letters,
    };
    Completion::<Infos>::new(Some(described), data)
        .unwrap()
        .ok(Infos(vec![info(Some(3), 1), info(None, 0)]));
    Completion::<Infos>::new(Some(described), data)
        .unwrap()
        .status(FELIX_STATUS_AUTH);
    let calls = calls.into_inner().unwrap();
    assert_eq!(calls[0].1, Some(vec![(true, 3, 1), (false, 0, 0)]));
    assert_eq!(calls[1].1, None);
}
