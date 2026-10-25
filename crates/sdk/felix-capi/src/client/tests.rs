use std::ffi::{CStr, c_char, c_void};
use std::sync::Mutex;

use felix_client::Seed;

use super::{ConnectOptions, Endpoints, FelixConnectOptions, parse_addrs, resolve_names};
use crate::boundary::Failure;
use crate::completion::Completion;
use crate::status::{FELIX_STATUS_AUTH, FELIX_STATUS_INVALID_ARGUMENT, FELIX_STATUS_OK};

/// Names reach the client as names, so it can resolve them again later.
#[test]
fn names_are_kept_as_names() {
    let seeds = parse_addrs("127.0.0.1:5000, localhost:5000").expect("parse");
    assert_eq!(
        seeds,
        [
            Seed::Addr("127.0.0.1:5000".parse().unwrap()),
            Seed::Name("localhost:5000".to_string()),
        ]
    );
}

#[test]
fn an_address_without_a_port_is_an_invalid_argument() {
    let failure = parse_addrs("localhost").unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
}

/// A name that does not resolve is still the caller's bad argument.
#[tokio::test]
async fn a_name_that_does_not_resolve_is_an_invalid_argument() {
    let seeds = parse_addrs("felix.invalid:5000").expect("parse");
    let failure = resolve_names(&seeds).await.unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
    assert!(
        failure.message.contains("could not resolve"),
        "{}",
        failure.message
    );
}

type Seen = (i32, Option<Vec<String>>);

unsafe extern "C" fn listed(
    user_data: *mut c_void,
    status: i32,
    _message: *const c_char,
    addrs: *const *const c_char,
    count: usize,
) {
    // SAFETY: every test passes a live `Mutex<Vec<Seen>>`.
    let calls = unsafe { &*user_data.cast::<Mutex<Vec<Seen>>>() };
    let addrs = (!addrs.is_null()).then(|| {
        // SAFETY: `count` NUL-terminated strings, valid while this runs.
        unsafe { std::slice::from_raw_parts(addrs, count) }
            .iter()
            .map(|&addr| {
                unsafe { CStr::from_ptr(addr) }
                    .to_string_lossy()
                    .into_owned()
            })
            .collect()
    });
    calls.lock().unwrap().push((status, addrs));
}

/// The addresses reach C as strings in order, and a failure carries none.
#[test]
fn endpoints_are_handed_over_as_strings() {
    let calls = Mutex::new(Vec::<Seen>::new());
    let data = (&raw const calls).cast_mut().cast::<c_void>();
    Completion::<Endpoints>::new(Some(listed), data)
        .unwrap()
        .ok(Endpoints::new(vec![
            "127.0.0.1:5000".to_string(),
            "[::1]:5001".to_string(),
        ]));
    Completion::<Endpoints>::new(Some(listed), data)
        .unwrap()
        .fail(Failure {
            status: FELIX_STATUS_AUTH,
            message: "denied".to_string(),
        });
    let calls = calls.into_inner().unwrap();
    assert_eq!(
        calls[0],
        (
            FELIX_STATUS_OK,
            Some(vec!["127.0.0.1:5000".to_string(), "[::1]:5001".to_string()])
        )
    );
    assert_eq!(calls[1], (FELIX_STATUS_AUTH, None));
}

#[test]
fn endpoints_refuses_a_null_client_and_a_null_callback() {
    // SAFETY: null is the case under test.
    let status = unsafe {
        super::felix_client_endpoints(
            std::ptr::null(),
            std::ptr::null(),
            Some(listed),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
}

fn read(options: &FelixConnectOptions) -> ConnectOptions {
    // SAFETY: a live struct, readable up to its own `size`.
    unsafe { ConnectOptions::read(options) }.expect("valid options")
}

#[test]
fn no_options_means_the_defaults() {
    // SAFETY: null is the case under test.
    let options = unsafe { ConnectOptions::read(std::ptr::null()) }.unwrap();
    assert!(!options.publishers && !options.timestamps);
}

/// A caller built against an older header passes a smaller struct, and the
/// fields it does not cover stay off whatever the bytes after it hold.
#[test]
fn a_field_is_read_only_when_size_covers_it() {
    let full = FelixConnectOptions {
        size: size_of::<FelixConnectOptions>(),
        publishers: true,
        timestamps: true,
        token_fetch: None,
        token_user_data: std::ptr::null_mut(),
        token_release: None,
    };
    let options = read(&full);
    assert!(options.publishers && options.timestamps);

    let older = FelixConnectOptions {
        size: std::mem::offset_of!(FelixConnectOptions, timestamps),
        ..full
    };
    let options = read(&older);
    assert!(options.publishers);
    assert!(!options.timestamps);

    let oldest = FelixConnectOptions {
        size: size_of::<usize>(),
        ..full
    };
    let options = read(&oldest);
    assert!(!options.publishers && !options.timestamps);
}

#[test]
fn a_size_smaller_than_the_size_field_is_refused() {
    let bad = FelixConnectOptions {
        size: 0,
        publishers: true,
        timestamps: true,
        token_fetch: None,
        token_user_data: std::ptr::null_mut(),
        token_release: None,
    };
    // SAFETY: a live struct.
    let failure = unsafe { ConnectOptions::read(&bad) }.unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
}

mod tokens {
    use std::ffi::{c_char, c_void};
    use std::ptr;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::time::Duration;

    use felix_client::TokenProvider;

    use super::super::{
        ConnectOptions, FelixClient, FelixConnectOptions, Shared, felix_client_connect_with,
    };
    use crate::cancel::{
        felix_cancel_token_cancel, felix_cancel_token_free, felix_cancel_token_new,
    };
    use crate::status::{FELIX_STATUS_CANCELLED, FELIX_STATUS_INVALID_ARGUMENT, FELIX_STATUS_OK};
    use crate::token::provider;
    use crate::token::tests::{Answer, Probe, fetch, release};

    fn options(probe: &'static Probe) -> FelixConnectOptions {
        FelixConnectOptions {
            size: size_of::<FelixConnectOptions>(),
            publishers: false,
            timestamps: false,
            token_fetch: Some(fetch),
            token_user_data: probe.data(),
            token_release: Some(release),
        }
    }

    /// A caller built before the token fields existed passes a smaller
    /// struct; what follows it in memory must not turn a provider on.
    #[test]
    fn the_token_fields_are_read_only_when_size_covers_them() {
        let probe = Probe::new(Answer::Hold);
        let full = options(probe);
        // SAFETY: a live struct, readable up to its own `size`.
        let read = |o: &FelixConnectOptions| unsafe { ConnectOptions::read(o) }.unwrap();
        let got = read(&full);
        assert!(got.token_fetch.is_some() && got.token_release.is_some());
        assert_eq!(got.token_user_data, probe.data());

        let older = FelixConnectOptions {
            size: std::mem::offset_of!(FelixConnectOptions, token_fetch),
            ..options(probe)
        };
        let got = read(&older);
        assert!(got.token_fetch.is_none() && got.token_release.is_none());
        assert!(got.token_user_data.is_null());

        // Covers the callback and its data but not the release function.
        let partial = FelixConnectOptions {
            size: std::mem::offset_of!(FelixConnectOptions, token_release),
            ..options(probe)
        };
        let got = read(&partial);
        assert!(got.token_fetch.is_some() && got.token_release.is_none());
    }

    unsafe extern "C" fn connected(
        data: *mut c_void,
        status: i32,
        _message: *const c_char,
        client: *mut FelixClient,
    ) {
        assert!(client.is_null());
        // SAFETY: each test passes a live sender.
        unsafe { &*data.cast::<mpsc::Sender<i32>>() }
            .send(status)
            .unwrap();
    }

    fn connect(
        addrs: *const c_char,
        token: *const c_char,
        options: *const FelixConnectOptions,
        sender: &mpsc::Sender<i32>,
        cancelled: bool,
    ) -> i32 {
        let cancel = felix_cancel_token_new();
        if cancelled {
            // SAFETY: a live token.
            unsafe { felix_cancel_token_cancel(cancel) };
        }
        // SAFETY: strings are literals or null, and the sender outlives the call.
        let status = unsafe {
            felix_client_connect_with(
                addrs,
                c"t1".as_ptr(),
                token,
                ptr::null(),
                ptr::null(),
                options,
                cancel,
                Some(connected),
                ptr::from_ref(sender).cast_mut().cast(),
            )
        };
        // SAFETY: a live token that calls may outlive.
        unsafe { felix_cancel_token_free(cancel) };
        status
    }

    /// A NULL token is for a caller with a fetch; without one it is refused.
    /// The callback's release does not run for a call that was refused, but
    /// does for one that was accepted.
    #[test]
    fn a_null_token_needs_a_fetch_and_release_follows_acceptance() {
        let (sender, receiver) = mpsc::channel();

        let probe = Probe::new(Answer::Hold);
        let status = connect(
            c"127.0.0.1:1".as_ptr(),
            ptr::null(),
            ptr::null(),
            &sender,
            true,
        );
        assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);

        // Refused for another reason: the app still owns its data.
        let with = options(probe);
        let status = connect(ptr::null(), ptr::null(), &with, &sender, true);
        assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
        let status = connect(c"nonsense".as_ptr(), ptr::null(), &with, &sender, true);
        assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
        assert_eq!(
            probe.released.load(Ordering::SeqCst),
            0,
            "released a refused call"
        );

        // Accepted with no token at all.
        let status = connect(c"127.0.0.1:1".as_ptr(), ptr::null(), &with, &sender, true);
        assert_eq!(status, FELIX_STATUS_OK);
        assert_eq!(
            receiver.recv_timeout(Duration::from_secs(10)),
            Ok(FELIX_STATUS_CANCELLED)
        );
        for _ in 0..500 {
            if probe.released.load(Ordering::SeqCst) == 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(probe.released.load(Ordering::SeqCst), 1);
        assert_eq!(probe.fetches.load(Ordering::SeqCst), 0);
    }

    /// Freeing the last handle fails a fetch the app never answered, so
    /// neither the shutdown nor the waiting call hangs.
    #[test]
    fn freeing_the_client_fails_a_fetch_the_app_never_answered() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let handle = runtime.handle().clone();
        let probe = Probe::new(Answer::Hold);
        let source = probe.source();
        let tokens = provider(&source, None);
        let shared = Shared::new(runtime).with_tokens(Some(source));

        let (sender, receiver) = mpsc::channel();
        // Tracked, as the library's own tasks are: shutdown waits for them.
        handle.spawn(shared.tracker.track_future(async move {
            let _ = sender.send(tokens.token().await.map_err(|e| format!("{e:#}")));
        }));
        for _ in 0..500 {
            if probe.fetches.load(Ordering::SeqCst) == 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(probe.fetches.load(Ordering::SeqCst), 1);

        drop(shared);
        let err = receiver
            .recv_timeout(Duration::from_secs(10))
            .expect("the waiting fetch was never failed")
            .unwrap_err();
        assert!(err.contains("freed"), "{err}");
        for _ in 0..500 {
            if probe.released.load(Ordering::SeqCst) == 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(probe.released.load(Ordering::SeqCst), 1);
    }
}
