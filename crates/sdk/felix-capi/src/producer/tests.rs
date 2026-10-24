use std::ptr;

use super::*;
use crate::status::{FELIX_STATUS_CANCELLED, FELIX_STATUS_INVALID_ARGUMENT};

#[test]
fn a_taken_slot_leases_nothing() {
    let slot = Slot::new(7u64);
    assert_eq!(*slot.lease().expect("open"), 7);
    assert!(slot.take().is_some());
    let failure = slot.lease().err().expect("closed");
    assert_eq!(failure.status, FELIX_STATUS_CLOSED);
    assert!(slot.take().is_none());
}

#[tokio::test]
async fn sole_waits_for_every_lease_to_end() {
    let slot = Slot::new(7u64);
    let first = slot.lease().expect("open");
    let second = slot.lease().expect("open");
    let taken = slot.take().expect("held");

    let waiting = tokio::spawn({
        let slot = Arc::clone(&slot);
        async move { slot.sole(taken).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiting.is_finished(), "returned while leases were held");
    drop(first);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiting.is_finished(), "returned with one lease held");
    drop(second);
    let value = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("woken by the last lease")
        .expect("task");
    assert_eq!(value, 7);
}

#[tokio::test]
async fn a_lease_ended_before_the_wait_is_not_missed() {
    let slot = Slot::new(1u64);
    let lease = slot.lease().expect("open");
    let taken = slot.take().expect("held");
    drop(lease);
    let value = tokio::time::timeout(Duration::from_secs(5), slot.sole(taken))
        .await
        .expect("the stored permit wakes it");
    assert_eq!(value, 1);
}

#[test]
fn batch_sizes_must_be_positive_and_cover_every_record() {
    assert!(check_sizes(&[2, 1], 3).is_ok());
    let zero = check_sizes(&[2, 0, 1], 3).unwrap_err();
    assert_eq!(zero.status, FELIX_STATUS_INVALID_ARGUMENT);
    assert!(zero.message.contains("batch 1"), "{}", zero.message);
    for sizes in [&[2usize][..], &[2, 2][..], &[usize::MAX, 2][..]] {
        let failure = check_sizes(sizes, 3).unwrap_err();
        assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
    }
}

#[test]
fn records_are_split_in_order() {
    let payloads = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
    let batches = split(payloads, &[2, 1]);
    assert_eq!(
        batches,
        vec![vec![b"a".to_vec(), b"b".to_vec()], vec![b"c".to_vec()]]
    );
}

#[test]
fn an_empty_publish_or_size_list_is_refused() {
    // SAFETY: nothing is read for a count of zero.
    let failure = unsafe { payloads(ptr::null(), ptr::null(), 0) }.unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
    // SAFETY: as above.
    let failure = unsafe { sizes(ptr::null(), 0) }.unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
    // SAFETY: a non-zero count with no array is refused before any read.
    let failure = unsafe { sizes(ptr::null(), 2) }.unwrap_err();
    assert!(failure.message.contains("null"), "{}", failure.message);
}

#[test]
fn the_id_accessors_check_their_pointers() {
    let mut id = 0u64;
    // SAFETY: a null producer is refused before it is read.
    let status = unsafe { felix_producer_id(ptr::null(), &mut id) };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
    // SAFETY: a null out is refused before anything is read.
    let status = unsafe { felix_producer_id(ptr::null(), ptr::null_mut()) };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
    // SAFETY: as above.
    let status = unsafe {
        felix_producer_id_for(
            ptr::null(),
            c"t".as_ptr(),
            c"n".as_ptr(),
            c"s".as_ptr(),
            &mut id,
        )
    };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
    // SAFETY: as above.
    let status = unsafe {
        felix_producer_id_for(
            ptr::null(),
            c"t".as_ptr(),
            c"n".as_ptr(),
            c"s".as_ptr(),
            ptr::null_mut(),
        )
    };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
    assert_eq!(id, 0);
}

#[test]
fn freeing_null_is_ignored() {
    // SAFETY: null is allowed.
    unsafe { felix_producer_free(ptr::null_mut()) };
}

#[tokio::test]
async fn hand_offs_run_in_the_order_they_were_queued() {
    let (forward, jobs) = mpsc::unbounded_channel::<Job>();
    let ran = Arc::new(Mutex::new(Vec::new()));
    // Earlier jobs are slower, so any concurrency would reorder them.
    for n in 0..20u64 {
        let ran = Arc::clone(&ran);
        let _ = forward.send(Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(20 - n)).await;
            ran.lock().expect("ran").push(n);
        }));
    }
    drop(forward);
    forward_in_order(jobs).await;
    assert_eq!(*ran.lock().expect("ran"), (0..20).collect::<Vec<_>>());
}

/// What a call's callback saw.
#[derive(Default)]
struct Recorder(Mutex<Vec<FelixStatus>>);

unsafe extern "C" fn record(user_data: *mut c_void, status: FelixStatus, _message: *const c_char) {
    // SAFETY: `user_data` is a `Recorder` the test keeps alive.
    let recorder = unsafe { &*(user_data as *const Recorder) };
    recorder.0.lock().expect("recorder").push(status);
}

impl Recorder {
    fn seen(&self) -> Vec<FelixStatus> {
        self.0.lock().expect("recorder").clone()
    }

    async fn called(&self) {
        for _ in 0..500 {
            if !self.seen().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("no callback");
    }
}

/// A forwarder held up by a job that waits for `release`.
fn blocked() -> (mpsc::UnboundedSender<Job>, oneshot::Sender<()>) {
    let (forward, jobs) = mpsc::unbounded_channel::<Job>();
    let (release, released) = oneshot::channel::<()>();
    let _ = forward.send(Box::pin(async move {
        let _ = released.await;
    }));
    tokio::spawn(forward_in_order(jobs));
    (forward, release)
}

/// Queue a call that records `id` when handed off, and run it.
fn call(
    slot: &Arc<Slot<u64>>,
    forward: &mpsc::UnboundedSender<Job>,
    handed: &Arc<Mutex<Vec<u8>>>,
    id: u8,
    handle: CancellationToken,
    user: Option<CancellationToken>,
    recorder: &Arc<Recorder>,
) {
    let handed = Arc::clone(handed);
    let queued = queue(slot.lease(), forward, move |_lease| async move {
        handed.lock().expect("handed").push(id);
        Ok::<u8, anyhow::Error>(id)
    });
    let done = Completion::<Done>::new(Some(record), Arc::as_ptr(recorder) as *mut c_void)
        .expect("callback");
    tokio::spawn(run_call(
        done,
        queued,
        handle,
        user,
        |id| async move { Ok::<u8, Failure>(id) },
        |done, outcome| done.result(outcome.map(|_| Done)),
    ));
}

#[tokio::test]
async fn a_cancel_before_the_hand_off_stops_it() {
    let slot = Slot::new(0u64);
    let (forward, release) = blocked();
    let handed = Arc::new(Mutex::new(Vec::new()));
    let (first, second) = (Arc::new(Recorder::default()), Arc::new(Recorder::default()));
    let token = CancellationToken::new();
    let handle = CancellationToken::new();
    call(
        &slot,
        &forward,
        &handed,
        1,
        handle.clone(),
        Some(token.clone()),
        &first,
    );
    call(&slot, &forward, &handed, 2, handle, None, &second);

    token.cancel();
    first.called().await;
    release.send(()).expect("forwarder is waiting");
    second.called().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(first.seen(), [FELIX_STATUS_CANCELLED]);
    assert_eq!(second.seen(), [FELIX_STATUS_OK]);
    assert_eq!(*handed.lock().expect("handed"), [2]);
}

#[tokio::test]
async fn every_queued_call_reports_once_when_the_handle_goes() {
    let slot = Slot::new(0u64);
    let (forward, release) = blocked();
    let handed = Arc::new(Mutex::new(Vec::new()));
    let handle = CancellationToken::new();
    let recorders: Vec<_> = (0..3).map(|_| Arc::new(Recorder::default())).collect();
    for (id, recorder) in (1..).zip(&recorders) {
        call(&slot, &forward, &handed, id, handle.clone(), None, recorder);
    }

    handle.cancel();
    for recorder in &recorders {
        recorder.called().await;
    }
    release.send(()).expect("forwarder is waiting");
    tokio::time::sleep(Duration::from_millis(50)).await;

    for recorder in &recorders {
        assert_eq!(recorder.seen(), [FELIX_STATUS_CANCELLED]);
    }
    assert!(handed.lock().expect("handed").is_empty());
}
