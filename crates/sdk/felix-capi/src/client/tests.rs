use std::ffi::{CStr, c_char, c_void};
use std::sync::Mutex;

use felix_client::Seed;

use super::{ConnectOptions, Endpoints, FelixConnectOptions, parse_addrs, resolve_names};
use crate::boundary::Failure;
use crate::completion::Completion;
use crate::status::{FELIX_STATUS_AUTH, FELIX_STATUS_INVALID_ARGUMENT, FELIX_STATUS_OK};

impl FelixConnectOptions {
    /// Every field off or zero, for a test to set what it names.
    fn zeroed() -> Self {
        Self {
            size: size_of::<Self>(),
            publishers: false,
            timestamps: false,
            token_fetch: None,
            token_user_data: std::ptr::null_mut(),
            token_release: None,
            reconnect_attempts: 0,
            reconnect_deadline_ms: 0,
            reconnect_backoff_ms: 0,
            reconnect_max_backoff_ms: 0,
            client_cert_file: std::ptr::null(),
            client_key_file: std::ptr::null(),
            disable_record_headers: false,
            ack_on_commit: false,
            no_publish_shard_streams: false,
            client_sub_queue_policy: 0,
            publish_sharding: 0,
            client_sub_queue_capacity: 0,
            broker_sub_queue_capacity: 0,
            publish_conn_pool: 0,
            publish_streams_per_conn: 0,
            publish_queue_depth: 0,
            publish_shard_streams: 0,
            cache_conn_pool: 0,
            cache_streams_per_conn: 0,
            event_conn_pool: 0,
            cluster_conn_pool: 0,
            cluster_streams_per_conn: 0,
            event_router_max_pending: 0,
            publish_inflight_bytes: 0,
            max_frame_bytes: 0,
            event_conn_recv_window: 0,
            event_stream_recv_window: 0,
            event_send_window: 0,
            cache_conn_recv_window: 0,
            cache_stream_recv_window: 0,
            cache_send_window: 0,
            connect_timeout_ms: 0,
            finish_timeout_ms: 0,
        }
    }
}

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
            cursor: None,
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
        reconnect_attempts: 0,
        reconnect_deadline_ms: 0,
        reconnect_backoff_ms: 0,
        reconnect_max_backoff_ms: 0,
        client_cert_file: std::ptr::null(),
        client_key_file: std::ptr::null(),
        disable_record_headers: false,
        ..FelixConnectOptions::zeroed()
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
fn reconnect_options_default_and_apply() {
    let full = FelixConnectOptions {
        size: size_of::<FelixConnectOptions>(),
        publishers: false,
        timestamps: false,
        token_fetch: None,
        token_user_data: std::ptr::null_mut(),
        token_release: None,
        reconnect_attempts: 0,
        reconnect_deadline_ms: 0,
        reconnect_backoff_ms: 0,
        reconnect_max_backoff_ms: 0,
        client_cert_file: std::ptr::null(),
        client_key_file: std::ptr::null(),
        disable_record_headers: false,
        ..FelixConnectOptions::zeroed()
    };
    let policy = read(&full).reconnect_policy().unwrap();
    let default = felix_client::ReconnectPolicy::default();
    assert_eq!(policy.attempts, default.attempts);
    assert_eq!(policy.deadline, None);

    let set = FelixConnectOptions {
        reconnect_attempts: 2,
        reconnect_deadline_ms: 1500,
        reconnect_backoff_ms: 50,
        reconnect_max_backoff_ms: 400,
        ..full
    };
    let policy = read(&set).reconnect_policy().unwrap();
    assert_eq!(policy.attempts, 2);
    assert_eq!(policy.backoff, std::time::Duration::from_millis(50));
    assert_eq!(policy.max_backoff, std::time::Duration::from_millis(400));
    assert_eq!(
        policy.deadline,
        Some(std::time::Duration::from_millis(1500))
    );

    // A caller built before the fields existed gets the defaults.
    let older = FelixConnectOptions {
        size: std::mem::offset_of!(FelixConnectOptions, reconnect_attempts),
        ..set
    };
    let policy = read(&older).reconnect_policy().unwrap();
    assert_eq!(policy.attempts, default.attempts);
    assert_eq!(policy.deadline, None);
    assert_eq!(policy.backoff, default.backoff);
    assert_eq!(policy.max_backoff, default.max_backoff);

    // A caller built with attempts and a deadline but no backoff fields.
    let before_backoff = FelixConnectOptions {
        size: std::mem::offset_of!(FelixConnectOptions, reconnect_backoff_ms),
        ..set
    };
    let policy = read(&before_backoff).reconnect_policy().unwrap();
    assert_eq!(policy.attempts, 2);
    assert_eq!(policy.backoff, default.backoff);
    assert_eq!(policy.max_backoff, default.max_backoff);
}

#[test]
fn a_max_backoff_below_the_backoff_is_allowed_and_a_year_caps_both() {
    let base = FelixConnectOptions {
        size: size_of::<FelixConnectOptions>(),
        publishers: false,
        timestamps: false,
        token_fetch: None,
        token_user_data: std::ptr::null_mut(),
        token_release: None,
        reconnect_attempts: 0,
        reconnect_deadline_ms: 0,
        reconnect_backoff_ms: 500,
        reconnect_max_backoff_ms: 100,
        client_cert_file: std::ptr::null(),
        client_key_file: std::ptr::null(),
        disable_record_headers: false,
        ..FelixConnectOptions::zeroed()
    };
    let policy = read(&base).reconnect_policy().unwrap();
    assert_eq!(policy.backoff, std::time::Duration::from_millis(500));
    assert_eq!(policy.max_backoff, std::time::Duration::from_millis(100));
    // Only the max set, below the default backoff of 200 ms.
    let only_max = FelixConnectOptions {
        reconnect_backoff_ms: 0,
        ..base
    };
    assert!(read(&only_max).reconnect_policy().is_ok());
    // Only the backoff set, above the default max of 2 s.
    let only_backoff = FelixConnectOptions {
        reconnect_backoff_ms: 5000,
        reconnect_max_backoff_ms: 0,
        ..base
    };
    assert!(read(&only_backoff).reconnect_policy().is_ok());
    // u64::MAX milliseconds counts as a year, so no wait can overflow the clock.
    let huge = FelixConnectOptions {
        reconnect_backoff_ms: u64::MAX,
        reconnect_max_backoff_ms: u64::MAX,
        ..base
    };
    let policy = read(&huge).reconnect_policy().unwrap();
    let year = std::time::Duration::from_secs(365 * 24 * 3600);
    assert_eq!(policy.backoff, year);
    assert_eq!(policy.max_backoff, year);
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
        reconnect_attempts: 0,
        reconnect_deadline_ms: 0,
        reconnect_backoff_ms: 0,
        reconnect_max_backoff_ms: 0,
        client_cert_file: std::ptr::null(),
        client_key_file: std::ptr::null(),
        disable_record_headers: false,
        ..FelixConnectOptions::zeroed()
    };
    // SAFETY: a live struct.
    let failure = unsafe { ConnectOptions::read(&bad) }.unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
}

/// A caller built before the certificate fields existed passes a smaller
/// struct; what follows it in memory must not name a file.
#[test]
fn the_certificate_fields_are_read_only_when_size_covers_them() {
    let cert = c"/some/client.pem".as_ptr();
    let key = c"/some/client.key".as_ptr();
    let full = FelixConnectOptions {
        size: size_of::<FelixConnectOptions>(),
        publishers: false,
        timestamps: false,
        token_fetch: None,
        token_user_data: std::ptr::null_mut(),
        token_release: None,
        reconnect_attempts: 0,
        reconnect_deadline_ms: 0,
        reconnect_backoff_ms: 0,
        reconnect_max_backoff_ms: 0,
        client_cert_file: cert,
        client_key_file: key,
        disable_record_headers: false,
        ..FelixConnectOptions::zeroed()
    };
    let got = read(&full);
    assert_eq!((got.client_cert_file, got.client_key_file), (cert, key));

    let cert_only = FelixConnectOptions {
        size: std::mem::offset_of!(FelixConnectOptions, client_key_file),
        ..full
    };
    let got = read(&cert_only);
    assert_eq!(got.client_cert_file, cert);
    assert!(got.client_key_file.is_null());

    let older = FelixConnectOptions {
        size: std::mem::offset_of!(FelixConnectOptions, client_cert_file),
        ..full
    };
    let got = read(&older);
    assert!(got.client_cert_file.is_null() && got.client_key_file.is_null());
}

mod client_certificate {
    use super::super::tls_config;
    use crate::status::FELIX_STATUS_INVALID_ARGUMENT;

    struct Files {
        dir: tempfile::TempDir,
    }

    impl Files {
        fn new() -> Self {
            let dir = tempfile::tempdir().expect("tempdir");
            let ca = rcgen::generate_simple_self_signed(vec!["felix-ca".to_string()]).unwrap();
            let client =
                rcgen::generate_simple_self_signed(vec!["felix-client".to_string()]).unwrap();
            std::fs::write(dir.path().join("ca.pem"), ca.cert.pem()).unwrap();
            std::fs::write(dir.path().join("client.pem"), client.cert.pem()).unwrap();
            std::fs::write(
                dir.path().join("client.key"),
                client.signing_key.serialize_pem(),
            )
            .unwrap();
            std::fs::write(dir.path().join("empty.pem"), "").unwrap();
            Self { dir }
        }

        fn path(&self, name: &str) -> String {
            self.dir.path().join(name).display().to_string()
        }
    }

    fn refused(cert: Option<&str>, key: Option<&str>, ca: &str) -> String {
        let failure = tls_config(Some(ca), cert, key).err().expect("refused");
        assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
        failure.message
    }

    #[test]
    fn a_certificate_and_key_build_a_config() {
        let files = Files::new();
        let (cert, key) = (files.path("client.pem"), files.path("client.key"));
        tls_config(Some(&files.path("ca.pem")), Some(&cert), Some(&key)).expect("with identity");
        tls_config(Some(&files.path("ca.pem")), None, None).expect("without");
    }

    #[test]
    fn a_certificate_without_its_key_is_refused() {
        let files = Files::new();
        let (ca, cert) = (files.path("ca.pem"), files.path("client.pem"));
        let message = refused(Some(&cert), None, &ca);
        assert!(message.contains("together"), "{message}");
        let message = refused(None, Some(&files.path("client.key")), &ca);
        assert!(message.contains("together"), "{message}");
    }

    #[test]
    fn an_unusable_file_is_named() {
        let files = Files::new();
        let ca = files.path("ca.pem");
        let (cert, key) = (files.path("client.pem"), files.path("client.key"));
        let message = refused(Some(&files.path("missing.pem")), Some(&key), &ca);
        assert!(message.contains("missing.pem"), "{message}");
        let message = refused(Some(&files.path("empty.pem")), Some(&key), &ca);
        assert!(message.contains("empty.pem"), "{message}");
        // A certificate file is not a key file.
        let message = refused(Some(&cert), Some(&cert), &ca);
        assert!(message.contains("client.pem"), "{message}");
        let message = refused(Some(&cert), Some(&files.path("missing.key")), &ca);
        assert!(message.contains("missing.key"), "{message}");
    }

    #[test]
    fn a_key_for_another_certificate_is_refused() {
        let files = Files::new();
        let other = rcgen::generate_simple_self_signed(vec!["other".to_string()]).unwrap();
        std::fs::write(
            files.dir.path().join("other.key"),
            other.signing_key.serialize_pem(),
        )
        .unwrap();
        let message = refused(
            Some(&files.path("client.pem")),
            Some(&files.path("other.key")),
            &files.path("ca.pem"),
        );
        assert!(message.contains("client certificate"), "{message}");
    }
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
            reconnect_attempts: 0,
            reconnect_deadline_ms: 0,
            reconnect_backoff_ms: 0,
            reconnect_max_backoff_ms: 0,
            client_cert_file: std::ptr::null(),
            client_key_file: std::ptr::null(),
            disable_record_headers: false,
            ..FelixConnectOptions::zeroed()
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

    /// A connect that is accepted and then fails through `on_connect` still
    /// ends the client, so `token_release` runs once.
    #[test]
    fn release_runs_when_an_accepted_connect_fails_asynchronously() {
        let (sender, receiver) = mpsc::channel();
        let probe = Probe::new(Answer::Hold);
        let with = options(probe);
        let status = connect(
            c"nonexistent.invalid:1".as_ptr(),
            c"tok".as_ptr(),
            &with,
            &sender,
            false,
        );
        assert_eq!(status, FELIX_STATUS_OK);
        let reported = receiver.recv_timeout(Duration::from_secs(60)).unwrap();
        assert_ne!(reported, FELIX_STATUS_OK);
        for _ in 0..500 {
            if probe.released.load(Ordering::SeqCst) == 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(probe.released.load(Ordering::SeqCst), 1);
    }

    /// Freeing the last handle fails a fetch the app never answered, so
    /// neither the shutdown nor the waiting call hangs. The app's data lives
    /// until it answers.
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
        // The app still holds a reply, so its data is not released yet.
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(probe.released.load(Ordering::SeqCst), 0);

        // Answering after the free is accepted and triggers the release.
        let reply = probe.reply(0) as usize;
        let status = std::thread::spawn(move || {
            let token = std::ffi::CString::new("late").unwrap();
            // SAFETY: the reply is live until this call consumes it.
            unsafe {
                crate::token::felix_token_reply_ok(
                    reply as *const crate::token::FelixTokenReply,
                    token.as_ptr(),
                )
            }
        })
        .join()
        .unwrap();
        assert_eq!(status, FELIX_STATUS_OK);
        assert_eq!(probe.released.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn record_headers_are_on_unless_the_caller_turns_them_off() {
    let mut options = FelixConnectOptions {
        size: size_of::<FelixConnectOptions>(),
        publishers: false,
        timestamps: false,
        token_fetch: None,
        token_user_data: std::ptr::null_mut(),
        token_release: None,
        reconnect_attempts: 0,
        reconnect_deadline_ms: 0,
        reconnect_backoff_ms: 0,
        reconnect_max_backoff_ms: 0,
        client_cert_file: std::ptr::null(),
        client_key_file: std::ptr::null(),
        disable_record_headers: false,
        ..FelixConnectOptions::zeroed()
    };
    assert!(!read(&options).disable_record_headers);
    options.disable_record_headers = true;
    assert!(read(&options).disable_record_headers);
    // A caller built before the field existed leaves headers on.
    options.size = std::mem::offset_of!(FelixConnectOptions, disable_record_headers);
    assert!(!read(&options).disable_record_headers);
}

/// The config `felix_client_connect_with` hands the client carries the caller's
/// features and tuning.
#[test]
fn the_connect_config_applies_features_and_tuning() {
    use super::connect_config;
    let mut options = FelixConnectOptions::zeroed();
    options.publishers = true;
    options.disable_record_headers = true;
    options.max_frame_bytes = 4096;
    options.publish_conn_pool = 3;
    options.client_sub_queue_policy = crate::tuning::FELIX_QUEUE_BLOCK;
    options.ack_on_commit = true;
    options.connect_timeout_ms = 2500;
    // SAFETY: `options` is live and its size covers it.
    let read = unsafe { ConnectOptions::read(&options) }.expect("read");
    let config = connect_config(&read, "t1", Some("tok"), None, None, None).expect("config");
    assert_eq!(config.auth_tenant_id.as_deref(), Some("t1"));
    assert_eq!(config.auth_token.as_deref(), Some("tok"));
    assert!(config.publishers && !config.timestamps && !config.record_headers);
    assert_eq!(config.max_frame_bytes, 4096);
    assert_eq!(config.publish_conn_pool, 3);
    assert_eq!(
        config.client_sub_queue_policy,
        felix_client::ClientSubQueuePolicy::Block
    );
    assert!(config.ack_on_commit);
    assert_eq!(
        config.connect_timeout,
        std::time::Duration::from_millis(2500)
    );

    let plain = connect_config(
        &unsafe { ConnectOptions::read(std::ptr::null()) }.expect("defaults"),
        "t1",
        None,
        None,
        None,
        None,
    )
    .expect("config");
    assert!(plain.record_headers);
    assert_eq!(plain.max_frame_bytes, 16 * 1024 * 1024);
}

#[test]
fn connect_options_carry_the_tuning_and_refuse_a_bad_value() {
    let mut options = FelixConnectOptions::zeroed();
    options.max_frame_bytes = 4096;
    // SAFETY: `options` is live and its size covers it.
    let read = unsafe { ConnectOptions::read(&options) }.expect("read");
    let mut config = felix_client::ClientConfig::optimized_defaults(
        felix_client::quic_client_config(None, false).expect("quic config"),
    );
    read.tuning.apply(&mut config);
    assert_eq!(config.max_frame_bytes, 4096);

    options.client_sub_queue_policy = 42;
    // SAFETY: as above.
    let failure = unsafe { ConnectOptions::read(&options) }.unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
}
