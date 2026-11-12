use std::ffi::{CStr, c_char, c_void};
use std::sync::Mutex;

use felix_client::{BrokerEndpoint, ShardOwner};
use serde_json::{Value, json};

use super::*;
use crate::status::FELIX_STATUS_INVALID_ARGUMENT;

type Seen<T> = Mutex<Vec<(FelixStatus, T)>>;

fn text_of(p: *const c_char) -> Option<String> {
    // SAFETY: each pointer the callbacks receive is null or a live C string.
    (!p.is_null()).then(|| unsafe { CStr::from_ptr(p) }.to_str().unwrap().to_string())
}

unsafe extern "C" fn json_cb(
    user_data: *mut c_void,
    status: FelixStatus,
    _message: *const c_char,
    json: *const c_char,
) {
    // SAFETY: the test passes a live `Seen`.
    let seen = unsafe { &*user_data.cast::<Seen<Option<String>>>() };
    seen.lock().unwrap().push((status, text_of(json)));
}

unsafe extern "C" fn page_cb(
    user_data: *mut c_void,
    status: FelixStatus,
    _message: *const c_char,
    json: *const c_char,
    next: *const c_char,
) {
    // SAFETY: the test passes a live `Seen`.
    let seen = unsafe { &*user_data.cast::<Seen<(Option<String>, Option<String>)>>() };
    seen.lock()
        .unwrap()
        .push((status, (text_of(json), text_of(next))));
}

unsafe extern "C" fn supports_cb(
    user_data: *mut c_void,
    status: FelixStatus,
    _message: *const c_char,
    flags: u32,
) {
    // SAFETY: the test passes a live `Seen`.
    let seen = unsafe { &*user_data.cast::<Seen<u32>>() };
    seen.lock().unwrap().push((status, flags));
}

unsafe extern "C" fn routing_cb(
    user_data: *mut c_void,
    status: FelixStatus,
    _message: *const c_char,
    shards: u32,
    routing: FelixShardRouting,
) {
    // SAFETY: the test passes a live `Seen`.
    let seen = unsafe { &*user_data.cast::<Seen<(u32, i32)>>() };
    seen.lock().unwrap().push((status, (shards, routing)));
}

fn user<T>(seen: &Seen<T>) -> *mut c_void {
    ptr::from_ref(seen).cast_mut().cast()
}

#[test]
fn a_json_answer_reaches_c_as_one_string_and_failure_as_null() {
    let seen: Seen<Option<String>> = Mutex::new(Vec::new());
    let owners = vec![ShardOwner {
        shard: 1,
        node_id: Some("n1".into()),
        addr: Some("10.0.0.1:4433".into()),
        generation: 7,
        unavailable: None,
    }];
    // SAFETY: the callback's contract; `seen` outlives the call.
    unsafe {
        Json::deliver(
            Some(json_cb),
            user(&seen),
            FELIX_STATUS_OK,
            ptr::null(),
            Some(Json::of(&owners).unwrap()),
        );
        Json::deliver(
            Some(json_cb),
            user(&seen),
            FELIX_STATUS_INVALID_ARGUMENT,
            ptr::null(),
            None,
        );
    }
    let seen = seen.lock().unwrap();
    let parsed: Value = serde_json::from_str(seen[0].1.as_ref().unwrap()).unwrap();
    assert_eq!(
        parsed,
        json!([{"shard": 1, "node_id": "n1", "addr": "10.0.0.1:4433", "generation": 7}])
    );
    assert_eq!(seen[1], (FELIX_STATUS_INVALID_ARGUMENT, None));
}

#[test]
fn topology_serializes_with_the_wire_field_names() {
    let brokers = vec![BrokerEndpoint {
        node_id: "n1".into(),
        addr: "10.0.0.1:4433".into(),
    }];
    let json = Json::of(&brokers).unwrap();
    let parsed: Value = serde_json::from_str(json.0.to_str().unwrap()).unwrap();
    assert_eq!(parsed, json!([{"node_id": "n1", "addr": "10.0.0.1:4433"}]));
}

#[test]
fn a_page_carries_its_next_cursor_as_a_string_to_pass_back() {
    let cursor = SubscriptionCursor {
        tenant_id: "acme".into(),
        namespace: "default".into(),
        stream: "orders".into(),
        shard: 2,
        subscriber_id: 41,
    };
    let seen: Seen<(Option<String>, Option<String>)> = Mutex::new(Vec::new());
    let page = Page {
        json: text(&json!({"node_id": "n1", "subscriptions": [], "next_cursor": cursor})).unwrap(),
        next: Some(text(&cursor).unwrap()),
    };
    let last = Page {
        json: text(&json!({"node_id": "n1", "subscriptions": [], "next_cursor": null})).unwrap(),
        next: None,
    };
    // SAFETY: the callback's contract; `seen` outlives the call.
    unsafe {
        Page::deliver(
            Some(page_cb),
            user(&seen),
            FELIX_STATUS_OK,
            ptr::null(),
            Some(page),
        );
        Page::deliver(
            Some(page_cb),
            user(&seen),
            FELIX_STATUS_OK,
            ptr::null(),
            Some(last),
        );
        Page::deliver(
            Some(page_cb),
            user(&seen),
            FELIX_STATUS_INVALID_ARGUMENT,
            ptr::null(),
            None,
        );
    }
    let seen = seen.lock().unwrap();
    let (json, next) = &seen[0].1;
    let page: Value = serde_json::from_str(json.as_ref().unwrap()).unwrap();
    assert_eq!(page["next_cursor"]["subscriber_id"], 41);
    // The string handed to C parses back to the cursor it named.
    assert_eq!(parse_cursor(next.as_ref().unwrap()).unwrap(), cursor);
    assert_eq!(seen[1].1.1, None, "the last page has no cursor");
    assert_eq!(seen[2].1, (None, None));
}

#[test]
fn a_cursor_that_is_not_one_is_refused() {
    for bad in ["", "not json", "{}", "[1]", r#"{"tenant_id": 3}"#] {
        let failure = parse_cursor(bad).unwrap_err();
        assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT, "{bad}");
        assert!(failure.message.contains("cursor"), "{}", failure.message);
    }
}

#[test]
fn support_bits_follow_what_the_connection_answers() {
    assert_eq!(support_flags(false, false, false, false, false, false), 0);
    assert_eq!(
        support_flags(true, false, false, false, false, false),
        FELIX_SUPPORTS_INSPECT
    );
    assert_eq!(
        support_flags(false, true, true, false, false, true),
        FELIX_SUPPORTS_TOPOLOGY | FELIX_SUPPORTS_ACK_ON_COMMIT | FELIX_SUPPORTS_SHARD_OWNERS
    );
    assert_eq!(support_flags(true, true, true, true, true, true), 0b11_1111);
    let bits = [
        FELIX_SUPPORTS_INSPECT,
        FELIX_SUPPORTS_TOPOLOGY,
        FELIX_SUPPORTS_ACK_ON_COMMIT,
        FELIX_SUPPORTS_STREAM_SHARDS,
        FELIX_SUPPORTS_CACHE_SHARDS,
        FELIX_SUPPORTS_SHARD_OWNERS,
    ];
    assert!(bits.iter().all(|b| b.count_ones() == 1));
}

#[test]
fn flags_and_routing_reach_c_and_failure_is_zero() {
    let flags: Seen<u32> = Mutex::new(Vec::new());
    let routing: Seen<(u32, i32)> = Mutex::new(Vec::new());
    // SAFETY: the callbacks' contracts; both outlive the calls.
    unsafe {
        Supports::deliver(
            Some(supports_cb),
            user(&flags),
            FELIX_STATUS_OK,
            ptr::null(),
            Some(Supports(5)),
        );
        Supports::deliver(
            Some(supports_cb),
            user(&flags),
            FELIX_STATUS_INVALID_ARGUMENT,
            ptr::null(),
            None,
        );
        Routing::deliver(
            Some(routing_cb),
            user(&routing),
            FELIX_STATUS_OK,
            ptr::null(),
            Some(Routing(8, routing_code(ShardRouting::JumpHash))),
        );
        Routing::deliver(
            Some(routing_cb),
            user(&routing),
            FELIX_STATUS_INVALID_ARGUMENT,
            ptr::null(),
            None,
        );
    }
    assert_eq!(
        *flags.lock().unwrap(),
        vec![(FELIX_STATUS_OK, 5), (FELIX_STATUS_INVALID_ARGUMENT, 0)]
    );
    assert_eq!(
        *routing.lock().unwrap(),
        vec![
            (FELIX_STATUS_OK, (8, FELIX_ROUTING_JUMP_HASH)),
            (FELIX_STATUS_INVALID_ARGUMENT, (0, FELIX_ROUTING_MODULO))
        ]
    );
    assert_eq!(routing_code(ShardRouting::Modulo), FELIX_ROUTING_MODULO);
}

#[test]
fn a_shard_kind_maps_and_an_unknown_one_is_refused() {
    assert_eq!(shard_kind(FELIX_KIND_STREAM).unwrap(), ShardKind::Stream);
    assert_eq!(shard_kind(FELIX_KIND_CACHE).unwrap(), ShardKind::Cache);
    for bad in [-1, 2, 99] {
        assert_eq!(
            shard_kind(bad).unwrap_err().status,
            FELIX_STATUS_INVALID_ARGUMENT
        );
    }
}

#[test]
fn a_null_filter_lists_everything() {
    // SAFETY: null is the case.
    let filter = unsafe { read_filter(ptr::null()) }.unwrap();
    assert_eq!(filter, SubscriptionFilter::default());
}

fn filter_with(size: usize) -> FelixSubscriptionFilter {
    FelixSubscriptionFilter {
        size,
        tenant_id: c"acme".as_ptr(),
        ns: c"default".as_ptr(),
        stream: c"orders".as_ptr(),
        principal: c"alice".as_ptr(),
        shard: 3,
        has_shard: true,
        dropping: true,
    }
}

#[test]
fn a_full_filter_is_read_field_by_field() {
    let raw = filter_with(size_of::<FelixSubscriptionFilter>());
    // SAFETY: `raw` is live and its size covers it.
    let filter = unsafe { read_filter(&raw) }.unwrap();
    assert_eq!(
        filter,
        SubscriptionFilter {
            tenant_id: Some("acme".into()),
            namespace: Some("default".into()),
            stream: Some("orders".into()),
            shard: Some(3),
            principal: Some("alice".into()),
            dropping: true,
        }
    );
}

#[test]
fn a_shard_without_has_shard_does_not_narrow() {
    let mut raw = filter_with(size_of::<FelixSubscriptionFilter>());
    raw.has_shard = false;
    // SAFETY: as above.
    assert_eq!(unsafe { read_filter(&raw) }.unwrap().shard, None);
}

#[test]
fn fields_past_the_callers_size_are_not_read() {
    let raw = filter_with(std::mem::offset_of!(FelixSubscriptionFilter, ns));
    // SAFETY: the size stops before `ns`, so only the first string and
    // `size` are read, whatever follows in memory.
    let filter = unsafe { read_filter(&raw) }.unwrap();
    assert_eq!(filter.tenant_id.as_deref(), Some("acme"));
    assert_eq!(filter.namespace, None);
    assert_eq!(filter.stream, None);
    assert_eq!(filter.principal, None);
    assert_eq!(filter.shard, None);
    assert!(!filter.dropping);
}

#[test]
fn a_size_smaller_than_the_size_field_is_refused() {
    let raw = filter_with(1);
    // SAFETY: `raw` is live; only `size` is read before the refusal.
    let failure = unsafe { read_filter(&raw) }.unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
}

#[test]
fn filter_layout_is_stable() {
    // The ABI the C# marshaller and the header promise.
    use std::mem::offset_of;
    assert_eq!(offset_of!(FelixSubscriptionFilter, size), 0);
    assert_eq!(offset_of!(FelixSubscriptionFilter, tenant_id), 8);
    assert_eq!(offset_of!(FelixSubscriptionFilter, ns), 16);
    assert_eq!(offset_of!(FelixSubscriptionFilter, stream), 24);
    assert_eq!(offset_of!(FelixSubscriptionFilter, principal), 32);
    assert_eq!(offset_of!(FelixSubscriptionFilter, shard), 40);
    assert_eq!(offset_of!(FelixSubscriptionFilter, has_shard), 44);
    assert_eq!(offset_of!(FelixSubscriptionFilter, dropping), 45);
}

#[test]
fn every_call_refuses_a_missing_client() {
    let noop_json: FelixJsonCallback = None;
    // SAFETY: the null client is the case; nothing else is read.
    unsafe {
        assert_eq!(
            felix_client_supports(ptr::null(), ptr::null(), None, ptr::null_mut()),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            felix_client_inspect_shard(
                ptr::null(),
                FELIX_KIND_STREAM,
                c"t".as_ptr(),
                c"n".as_ptr(),
                c"s".as_ptr(),
                0,
                ptr::null(),
                noop_json,
                ptr::null_mut()
            ),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            felix_client_list_subscriptions(
                ptr::null(),
                ptr::null(),
                0,
                ptr::null(),
                ptr::null(),
                None,
                ptr::null_mut()
            ),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            felix_client_topology(ptr::null(), ptr::null(), noop_json, ptr::null_mut()),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            felix_client_stream_routing(
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
        assert_eq!(
            felix_client_shard_owners(
                ptr::null(),
                FELIX_KIND_CACHE,
                c"t".as_ptr(),
                c"n".as_ptr(),
                c"c".as_ptr(),
                ptr::null(),
                noop_json,
                ptr::null_mut()
            ),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            felix_client_cache_shards(
                ptr::null(),
                c"t".as_ptr(),
                c"n".as_ptr(),
                c"c".as_ptr(),
                ptr::null(),
                None,
                ptr::null_mut()
            ),
            FELIX_STATUS_INVALID_ARGUMENT
        );
    }
}
