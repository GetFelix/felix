use std::ffi::{CString, c_char, c_void};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;

use super::{FelixTokenReply, TokenSource, felix_token_reply_err, felix_token_reply_ok, provider};
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

/// `token_release` never runs while a reply is unanswered, even once the
/// client is freed and every reference but the reply's is gone; the last
/// answer, from another thread, triggers it exactly once, and the late answer
/// is accepted.
#[tokio::test]
async fn release_waits_for_the_last_answer() {
    use felix_client::TokenProvider;
    let probe = Probe::new(Answer::Hold);
    let source = probe.source();
    let tokens = Arc::new(provider(&source, None));
    let held = Arc::clone(&tokens);
    let waiting = tokio::spawn(async move { held.token().await.map_err(|e| format!("{e:#}")) });
    wait_for("the fetch", || probe.fetches.load(Ordering::SeqCst) == 1).await;

    // The client is freed and the caller lets go of everything while the app
    // still has a reply.
    source.close();
    drop(source);
    assert!(waiting.await.unwrap().is_err());
    drop(tokens);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        probe.released.load(Ordering::SeqCst),
        0,
        "released before the reply was answered"
    );

    let reply = probe.reply(0) as usize;
    let answered = std::thread::spawn(move || {
        let token = CString::new("late").unwrap();
        // SAFETY: the reply is live until this call consumes it.
        unsafe { felix_token_reply_ok(reply as *const FelixTokenReply, token.as_ptr()) }
    })
    .join()
    .unwrap();
    assert_eq!(answered, FELIX_STATUS_OK);
    assert_eq!(probe.released.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(probe.released.load(Ordering::SeqCst), 1);
}

/// With every reply answered before the client is freed, the free releases.
#[tokio::test]
async fn release_runs_on_close_when_nothing_is_outstanding() {
    use felix_client::TokenProvider;
    let probe = Probe::new(Answer::OtherThread("t"));
    let source = probe.source();
    let tokens = provider(&source, None);
    assert_eq!(tokens.token().await.unwrap(), "t");
    wait_for("the reply to be freed", || source.held() == 0).await;
    assert_eq!(probe.released.load(Ordering::SeqCst), 0);
    source.close();
    assert_eq!(probe.released.load(Ordering::SeqCst), 1);
    drop(source);
    drop(tokens);
    assert_eq!(probe.released.load(Ordering::SeqCst), 1);
}

/// The first answer counts, and answering frees the reply.
#[tokio::test]
async fn an_answer_consumes_the_reply() {
    use felix_client::TokenProvider;
    let probe = Probe::new(Answer::Hold);
    let source = probe.source();
    let tokens = Arc::new(provider(&source, None));
    let held = Arc::clone(&tokens);
    let waiting = tokio::spawn(async move { held.token().await.unwrap() });
    wait_for("the fetch", || probe.fetches.load(Ordering::SeqCst) == 1).await;
    assert_eq!(source.held(), 1);

    let first = CString::new("first").unwrap();
    // SAFETY: a live reply and string; the reply is not used again.
    let status = unsafe { felix_token_reply_ok(probe.reply(0), first.as_ptr()) };
    assert_eq!(status, FELIX_STATUS_OK);
    assert_eq!(waiting.await.unwrap(), "first");
    assert_eq!(source.held(), 0);
}

/// Replies that were answered are not kept, however many fetches fail.
#[tokio::test]
async fn answered_replies_are_not_retained() {
    use felix_client::TokenProvider;
    let probe = Probe::new(Answer::Fail("down"));
    let source = probe.source();
    let tokens = provider(&source, None);
    for _ in 0..20 {
        assert!(tokens.token().await.is_err());
    }
    assert_eq!(probe.fetches.load(Ordering::SeqCst), 20);
    assert_eq!(source.held(), 0);

    // Answered from another thread after the fetch returned.
    let probe = Probe::new(Answer::OtherThread("t"));
    let source = probe.source();
    let tokens = provider(&source, None);
    assert_eq!(tokens.token().await.unwrap(), "t");
    wait_for("the reply to be freed", || source.held() == 0).await;
}

/// An unanswered reply is held until it is answered or the source goes.
#[tokio::test]
async fn an_unanswered_reply_is_held_until_the_source_goes() {
    use felix_client::TokenProvider;
    let probe = Probe::new(Answer::Hold);
    let source = probe.source();
    let tokens = Arc::new(provider(&source, None));
    let held = Arc::clone(&tokens);
    let waiting = tokio::spawn(async move { held.token().await });
    wait_for("the fetch", || probe.fetches.load(Ordering::SeqCst) == 1).await;
    source.close();
    assert!(waiting.await.unwrap().is_err());
    assert_eq!(source.held(), 1);
}

/// An empty initial token is no token: the first use fetches.
#[tokio::test]
async fn an_empty_initial_token_is_fetched_over() {
    use felix_client::TokenProvider;
    let probe = Probe::new(Answer::OtherThread("fetched"));
    let tokens = provider(&probe.source(), Some(""));
    assert_eq!(tokens.token().await.unwrap(), "fetched");
    assert_eq!(probe.fetches.load(Ordering::SeqCst), 1);
}

/// A client with a valid token that does nothing never asks the app.
#[tokio::test(start_paused = true)]
async fn an_idle_client_makes_no_fetch() {
    use felix_client::TokenProvider;
    let probe = Probe::new(Answer::OtherThread("never"));
    let tokens = provider(&probe.source(), Some("opaque-token"));
    tokio::time::sleep(Duration::from_secs(3600)).await;
    assert_eq!(probe.fetches.load(Ordering::SeqCst), 0);
    assert_eq!(tokens.token().await.unwrap(), "opaque-token");
    assert_eq!(probe.fetches.load(Ordering::SeqCst), 0);
}

/// Streams opening together share one fetch through the app's callback.
#[tokio::test]
async fn concurrent_callers_share_one_fetch() {
    use felix_client::TokenProvider;
    let probe = Probe::new(Answer::Hold);
    let tokens = Arc::new(provider(&probe.source(), None));
    let callers: Vec<_> = (0..8)
        .map(|_| {
            let tokens = Arc::clone(&tokens);
            tokio::spawn(async move { tokens.token().await.unwrap() })
        })
        .collect();
    wait_for("the fetch", || probe.fetches.load(Ordering::SeqCst) == 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(probe.fetches.load(Ordering::SeqCst), 1);
    let token = CString::new("shared").unwrap();
    // SAFETY: a live reply and string.
    unsafe { felix_token_reply_ok(probe.reply(0), token.as_ptr()) };
    for caller in callers {
        assert_eq!(caller.await.unwrap(), "shared");
    }
    assert_eq!(probe.fetches.load(Ordering::SeqCst), 1);
}

/// A refused token is dropped; the next ask fetches once, and a late refusal
/// of the old token does not throw away the new one.
#[tokio::test]
async fn a_refused_token_costs_one_fetch() {
    use felix_client::TokenProvider;
    let probe = Probe::new(Answer::OtherThread("replacement"));
    let tokens = provider(&probe.source(), Some("old"));
    assert_eq!(tokens.token().await.unwrap(), "old");
    tokens.invalidate("old");
    assert_eq!(tokens.token().await.unwrap(), "replacement");
    tokens.invalidate("old");
    assert_eq!(tokens.token().await.unwrap(), "replacement");
    assert_eq!(probe.fetches.load(Ordering::SeqCst), 1);
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

/// The core stops waiting on a fetch after its timeout but not the fetch. An
/// answer that arrives later is used, with no second call to the app.
#[tokio::test(start_paused = true)]
async fn a_reply_after_the_fetch_timed_out_is_used() {
    use felix_client::TokenProvider;
    let probe = Probe::new(Answer::Hold);
    let source = probe.source();
    let tokens = provider(&source, None);
    let err = tokens.token().await.unwrap_err();
    assert!(format!("{err:#}").contains("took longer"), "{err:#}");
    assert_eq!(probe.fetches.load(Ordering::SeqCst), 1);

    let late = CString::new("late").unwrap();
    // SAFETY: a live reply and string.
    unsafe {
        assert_eq!(source.held(), 1);
        assert_eq!(
            felix_token_reply_ok(probe.reply(0), late.as_ptr()),
            FELIX_STATUS_OK
        );
    }
    assert_eq!(source.held(), 0);
    tokio::task::yield_now().await;
    assert_eq!(tokens.token().await.unwrap(), "late");
    assert_eq!(probe.fetches.load(Ordering::SeqCst), 1);
}
