use std::ffi::{CStr, CString, c_char, c_void};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;

use super::{FelixTokenReply, TokenSource, felix_token_reply_err, felix_token_reply_ok, provider};
use crate::error::felix_last_error_message;
use crate::status::{FELIX_STATUS_INVALID_ARGUMENT, FELIX_STATUS_OK};

#[derive(Clone, Copy)]
pub(crate) enum Answer {
    /// Leave the reply for the test to complete.
    Hold,
    /// Complete it with a token, from another thread.
    OtherThread(&'static str),
    Fail(&'static str),
}

/// What the "app" sees: its fetch and release functions record here.
pub(crate) struct Probe {
    pub(crate) fetches: AtomicUsize,
    pub(crate) released: AtomicUsize,
    pub(crate) replies: Mutex<Vec<usize>>,
    answer: Mutex<Answer>,
}

impl Probe {
    /// Leaked so its address can be the callbacks' `user_data`.
    pub(crate) fn new(answer: Answer) -> &'static Probe {
        Box::leak(Box::new(Probe {
            fetches: AtomicUsize::new(0),
            released: AtomicUsize::new(0),
            replies: Mutex::new(Vec::new()),
            answer: Mutex::new(answer),
        }))
    }

    pub(crate) fn data(&'static self) -> *mut c_void {
        (self as *const Probe).cast_mut().cast()
    }

    pub(crate) fn source(&'static self) -> Arc<TokenSource> {
        Arc::new(TokenSource::new(fetch, self.data(), Some(release)))
    }

    pub(crate) fn reply(&self, index: usize) -> *const FelixTokenReply {
        self.replies.lock().unwrap()[index] as *const FelixTokenReply
    }
}

pub(crate) unsafe extern "C" fn fetch(data: *mut c_void, reply: *mut FelixTokenReply) {
    // SAFETY: every test passes a leaked `Probe`.
    let probe = unsafe { &*data.cast::<Probe>() };
    probe.fetches.fetch_add(1, Ordering::SeqCst);
    probe.replies.lock().unwrap().push(reply as usize);
    let reply = reply as usize;
    match *probe.answer.lock().unwrap() {
        Answer::Hold => {}
        Answer::OtherThread(token) => {
            std::thread::spawn(move || {
                let token = CString::new(token).unwrap();
                // SAFETY: the reply lives until release, which has not run.
                let status = unsafe {
                    felix_token_reply_ok(reply as *const FelixTokenReply, token.as_ptr())
                };
                assert_eq!(status, FELIX_STATUS_OK);
            });
        }
        Answer::Fail(message) => {
            let message = CString::new(message).unwrap();
            // SAFETY: as above.
            let status =
                unsafe { felix_token_reply_err(reply as *const FelixTokenReply, message.as_ptr()) };
            assert_eq!(status, FELIX_STATUS_OK);
        }
    }
}

pub(crate) unsafe extern "C" fn release(data: *mut c_void) {
    // SAFETY: every test passes a leaked `Probe`.
    unsafe { &*data.cast::<Probe>() }
        .released
        .fetch_add(1, Ordering::SeqCst);
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn jwt(exp: i64) -> String {
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    format!(
        "{}.{}.sig",
        b64.encode("{}"),
        b64.encode(format!("{{\"exp\":{exp}}}"))
    )
}

fn last_error() -> String {
    let message = felix_last_error_message();
    assert!(!message.is_null());
    // SAFETY: valid until the next call on this thread.
    unsafe { CStr::from_ptr(message) }
        .to_string_lossy()
        .into_owned()
}

async fn wait_for(what: &str, condition: impl Fn() -> bool) {
    for _ in 0..500 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

/// The fetch runs on a library thread, the reply comes from another, and a
/// connect with no initial token gets its first one that way.
#[tokio::test]
async fn a_reply_from_another_thread_is_the_token_and_no_initial_token_is_fine() {
    let probe = Probe::new(Answer::OtherThread("fetched"));
    let tokens = provider(&probe.source(), None);
    use felix_client::TokenProvider;
    assert_eq!(tokens.token().await.unwrap(), "fetched");
    assert_eq!(probe.fetches.load(Ordering::SeqCst), 1);
}

/// The initial token is used without asking the app.
#[tokio::test]
async fn the_initial_token_is_served_without_a_fetch() {
    let probe = Probe::new(Answer::OtherThread("never"));
    let tokens = provider(&probe.source(), Some("initial"));
    use felix_client::TokenProvider;
    assert_eq!(tokens.token().await.unwrap(), "initial");
    assert_eq!(probe.fetches.load(Ordering::SeqCst), 0);
}

/// A token due for refresh that is still valid survives a failed fetch.
#[tokio::test]
async fn a_failed_fetch_keeps_the_old_token_while_it_is_valid() {
    use felix_client::TokenProvider;
    let probe = Probe::new(Answer::Fail("the control plane is down"));
    // Refresh is due two thirds of the way in: after 2s of 4.
    let old = jwt(now() + 4);
    let tokens = provider(&probe.source(), Some(&old));
    assert_eq!(tokens.token().await.unwrap(), old);
    assert_eq!(probe.fetches.load(Ordering::SeqCst), 0);
    tokio::time::sleep(Duration::from_millis(2100)).await;
    assert_eq!(tokens.token().await.unwrap(), old);
    assert_eq!(probe.fetches.load(Ordering::SeqCst), 1);
}

/// With nothing valid to fall back on, the app's message is the error.
#[tokio::test]
async fn a_failed_fetch_with_no_valid_token_is_an_error_with_the_message() {
    use felix_client::TokenProvider;
    let probe = Probe::new(Answer::Fail("the control plane is down"));
    let tokens = provider(&probe.source(), None);
    let err = tokens.token().await.unwrap_err();
    assert!(
        format!("{err:#}").contains("the control plane is down"),
        "{err:#}"
    );
}

/// A reply the app never completes fails the fetch when the source closes
/// (the client is freed), rather than waiting forever.
#[tokio::test]
async fn an_unanswered_reply_fails_when_the_source_closes() {
    use felix_client::TokenProvider;
    let probe = Probe::new(Answer::Hold);
    let source = probe.source();
    let tokens = provider(&source, None);
    let waiting = tokio::spawn(async move { tokens.token().await.map_err(|e| format!("{e:#}")) });
    wait_for("the fetch", || probe.fetches.load(Ordering::SeqCst) == 1).await;
    source.close();
    let err = waiting.await.unwrap().unwrap_err();
    assert!(err.contains("freed"), "{err}");
    // A fetch after the close does not call the app at all.
    let tokens = provider(&source, None);
    assert!(tokens.token().await.is_err());
    assert_eq!(probe.fetches.load(Ordering::SeqCst), 1);
}

/// `token_release` waits for the last fetch, and runs once.
#[tokio::test]
async fn release_runs_once_after_the_last_fetch() {
    use felix_client::TokenProvider;
    let probe = Probe::new(Answer::Hold);
    let source = probe.source();
    let tokens = Arc::new(provider(&source, None));
    let held = Arc::clone(&tokens);
    let waiting = tokio::spawn(async move { held.token().await.map_err(|e| format!("{e:#}")) });
    wait_for("the fetch", || probe.fetches.load(Ordering::SeqCst) == 1).await;

    // The caller lets go of everything it had while a fetch is in flight.
    source.close();
    drop(source);
    drop(tokens);
    assert_eq!(
        probe.released.load(Ordering::SeqCst),
        0,
        "released mid-fetch"
    );

    // The fetch settles (closed, so as an error), then the last reference goes.
    assert!(waiting.await.unwrap().is_err());
    wait_for("the release", || probe.released.load(Ordering::SeqCst) == 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(probe.released.load(Ordering::SeqCst), 1);
}

/// The second answer is refused and changes nothing; the first one counts.
#[tokio::test]
async fn a_second_reply_is_an_error_and_the_first_stands() {
    use felix_client::TokenProvider;
    let probe = Probe::new(Answer::Hold);
    let tokens = Arc::new(provider(&probe.source(), None));
    let held = Arc::clone(&tokens);
    let waiting = tokio::spawn(async move { held.token().await.unwrap() });
    wait_for("the fetch", || probe.fetches.load(Ordering::SeqCst) == 1).await;

    let first = CString::new("first").unwrap();
    let second = CString::new("second").unwrap();
    // SAFETY: a live reply and strings.
    unsafe {
        assert_eq!(
            felix_token_reply_ok(probe.reply(0), first.as_ptr()),
            FELIX_STATUS_OK
        );
        let status = felix_token_reply_ok(probe.reply(0), second.as_ptr());
        assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
        assert!(
            last_error().contains("already completed"),
            "{}",
            last_error()
        );
        let status = felix_token_reply_err(probe.reply(0), std::ptr::null());
        assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
    }
    assert_eq!(waiting.await.unwrap(), "first");
}

/// A bad argument does not use up the reply.
#[tokio::test]
async fn a_refused_argument_leaves_the_reply_open() {
    use felix_client::TokenProvider;
    let probe = Probe::new(Answer::Hold);
    let tokens = Arc::new(provider(&probe.source(), None));
    let held = Arc::clone(&tokens);
    let waiting = tokio::spawn(async move { held.token().await.unwrap() });
    wait_for("the fetch", || probe.fetches.load(Ordering::SeqCst) == 1).await;

    let empty = CString::new("").unwrap();
    let good: *const c_char = c"good".as_ptr();
    // SAFETY: a live reply and strings; null is the case under test.
    unsafe {
        assert_eq!(
            felix_token_reply_ok(std::ptr::null(), good),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            felix_token_reply_ok(probe.reply(0), std::ptr::null()),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            felix_token_reply_ok(probe.reply(0), empty.as_ptr()),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(felix_token_reply_ok(probe.reply(0), good), FELIX_STATUS_OK);
    }
    assert_eq!(waiting.await.unwrap(), "good");
}

/// The core gives up on a fetch after its timeout. An answer that arrives
/// later is accepted and ignored, and the reply is still there to be refused
/// the second time.
#[tokio::test(start_paused = true)]
async fn a_reply_after_the_fetch_timed_out_is_a_harmless_no_op() {
    use felix_client::TokenProvider;
    let probe = Probe::new(Answer::Hold);
    let tokens = provider(&probe.source(), None);
    let err = tokens.token().await.unwrap_err();
    assert!(format!("{err:#}").contains("took longer"), "{err:#}");
    assert_eq!(probe.fetches.load(Ordering::SeqCst), 1);

    let late = CString::new("late").unwrap();
    // SAFETY: a live reply and string.
    unsafe {
        assert_eq!(
            felix_token_reply_ok(probe.reply(0), late.as_ptr()),
            FELIX_STATUS_OK
        );
        assert_eq!(
            felix_token_reply_ok(probe.reply(0), late.as_ptr()),
            FELIX_STATUS_INVALID_ARGUMENT
        );
    }
    // The late token was not kept: the next fetch is a new one.
    assert!(tokens.token().await.is_err());
    assert_eq!(probe.fetches.load(Ordering::SeqCst), 2);
}
