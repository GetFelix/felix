use std::ptr;
use std::time::Duration;

use tokio::sync::oneshot;

use super::*;
use crate::status::{
    FELIX_STATUS_CANCELLED, FELIX_STATUS_INVALID_ARGUMENT, FELIX_STATUS_OUTCOME_UNKNOWN,
};

#[test]
fn an_answer_can_be_taken_once() {
    let once = Once::new(7u64);
    assert_eq!(once.take().expect("first"), 7);
    let failure = once.take().unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
    assert!(
        failure.message.contains("already read"),
        "{}",
        failure.message
    );
    // And stays taken.
    assert!(once.take().is_err());
}

#[test]
fn every_call_checks_its_pointers() {
    // SAFETY: null pointers are the case; none is dereferenced.
    unsafe {
        felix_handed_free(ptr::null_mut());
        assert_eq!(
            felix_handed_offset(ptr::null(), ptr::null(), None, ptr::null_mut()),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            felix_handed_done(ptr::null(), ptr::null(), None, ptr::null_mut()),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            felix_producer_hand_off_batch(
                ptr::null(),
                c"t".as_ptr(),
                c"n".as_ptr(),
                c"s".as_ptr(),
                ptr::null(),
                ptr::null(),
                1,
                ptr::null(),
                0,
                ptr::null(),
                None,
                ptr::null_mut()
            ),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            felix_producer_hand_off_batches(
                ptr::null(),
                c"t".as_ptr(),
                c"n".as_ptr(),
                c"s".as_ptr(),
                ptr::null(),
                ptr::null(),
                1,
                ptr::null(),
                1,
                ptr::null(),
                None,
                ptr::null_mut()
            ),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            felix_producer_hand_off_abandon(
                ptr::null(),
                c"t".as_ptr(),
                c"n".as_ptr(),
                c"s".as_ptr(),
                ptr::null(),
                None,
                ptr::null_mut()
            ),
            FELIX_STATUS_INVALID_ARGUMENT
        );
    }
}

#[derive(Default)]
struct Recorder(Mutex<Vec<(FelixStatus, bool, u64)>>);

unsafe extern "C" fn published(
    user_data: *mut c_void,
    status: FelixStatus,
    _message: *const c_char,
    has_offset: bool,
    offset: u64,
) {
    // SAFETY: `user_data` is a `Recorder` the test keeps alive.
    let recorder = unsafe { &*(user_data as *const Recorder) };
    recorder
        .0
        .lock()
        .expect("recorder")
        .push((status, has_offset, offset));
}

impl Recorder {
    async fn seen(&self) -> Vec<(FelixStatus, bool, u64)> {
        for _ in 0..500 {
            let seen = self.0.lock().expect("recorder").clone();
            if !seen.is_empty() {
                return seen;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("no callback");
    }
}

fn completion(recorder: &Arc<Recorder>) -> Completion<Published> {
    Completion::<Published>::new(Some(published), Arc::as_ptr(recorder) as *mut c_void)
        .expect("callback")
}

#[tokio::test]
async fn an_answer_is_reported_when_it_arrives() {
    let recorder = Arc::new(Recorder::default());
    let (send, receive) = oneshot::channel::<Result<Option<u64>, Failure>>();
    tokio::spawn(await_answer(
        completion(&recorder),
        receive,
        CancellationToken::new(),
        None,
        |receive| async move { receive.await.expect("sent") },
        |done, outcome| done.result(outcome.map(Published)),
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(recorder.0.lock().expect("recorder").is_empty());
    send.send(Ok(Some(41))).expect("waiting");
    assert_eq!(recorder.seen().await, [(FELIX_STATUS_OK, true, 41)]);
}

#[tokio::test]
async fn a_failed_answer_is_reported_with_its_status() {
    let recorder = Arc::new(Recorder::default());
    tokio::spawn(await_answer(
        completion(&recorder),
        (),
        CancellationToken::new(),
        None,
        |()| async { Err::<Option<u64>, _>(Failure::invalid("refused")) },
        |done, outcome| done.result(outcome.map(Published)),
    ));
    assert_eq!(
        recorder.seen().await,
        [(FELIX_STATUS_INVALID_ARGUMENT, false, 0)]
    );
}

/// The batch is sent whether or not the caller still waits, so a cancel is
/// not "cancelled": its outcome is unknown.
#[tokio::test]
async fn a_cancel_while_waiting_reports_an_unknown_outcome() {
    let recorder = Arc::new(Recorder::default());
    let user = CancellationToken::new();
    let (_keep, receive) = oneshot::channel::<Result<Option<u64>, Failure>>();
    tokio::spawn(await_answer(
        completion(&recorder),
        receive,
        CancellationToken::new(),
        Some(user.clone()),
        |receive| async move { receive.await.expect("never answered") },
        |done, outcome| done.result(outcome.map(Published)),
    ));
    user.cancel();
    let seen = recorder.seen().await;
    assert_eq!(seen, [(FELIX_STATUS_OUTCOME_UNKNOWN, false, 0)]);
    assert_ne!(seen[0].0, FELIX_STATUS_CANCELLED);
}

#[tokio::test]
async fn closing_the_handle_ends_a_wait_the_same_way() {
    let recorder = Arc::new(Recorder::default());
    let handle = CancellationToken::new();
    let (_keep, receive) = oneshot::channel::<Result<Option<u64>, Failure>>();
    tokio::spawn(await_answer(
        completion(&recorder),
        receive,
        handle.clone(),
        None,
        |receive| async move { receive.await.expect("never answered") },
        |done, outcome| done.result(outcome.map(Published)),
    ));
    handle.cancel();
    assert_eq!(
        recorder.seen().await,
        [(FELIX_STATUS_OUTCOME_UNKNOWN, false, 0)]
    );
}
