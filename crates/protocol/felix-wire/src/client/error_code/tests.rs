use crate::{ErrorCode, ErrorDetail, Message, RetryClass};

#[test]
fn every_code_round_trips_by_name_and_number() {
    for code in ErrorCode::ALL {
        assert_eq!(ErrorCode::parse(code.as_str()), code);
        assert_eq!(ErrorCode::from_u16(code.to_u16()), code);
        assert_ne!(code.to_u16(), 0, "0 is reserved for unknown codes");
    }
    for class in RetryClass::ALL {
        assert_eq!(RetryClass::parse(class.as_str()), class);
        assert_eq!(RetryClass::from_u8(class.to_u8()), class);
    }
}

#[test]
fn codes_are_snake_case_and_distinct() {
    let mut names: Vec<&str> = ErrorCode::ALL.iter().map(ErrorCode::as_str).collect();
    for name in &names {
        assert!(
            name.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
            "{name} is not snake_case"
        );
    }
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), ErrorCode::ALL.len());
}

#[test]
fn a_coded_error_round_trips() {
    let message = Message::Error {
        message: "stream s cannot be subscribed to right now".to_string(),
        code: Some(ErrorCode::ShardUnavailable),
        retry: Some(RetryClass::Retry),
        detail: Some(ErrorDetail {
            reason: Some("not_ready".to_string()),
            retry_after_ms: None,
        }),
    };
    let decoded = Message::decode(message.encode().expect("encode")).expect("decode");
    assert_eq!(decoded, message);

    let message = Message::PublishError {
        request_id: 4,
        message: "no majority".to_string(),
        code: Some(ErrorCode::QuorumTimeout),
        retry: Some(RetryClass::OutcomeUnknown),
        detail: None,
    };
    let frame = message.encode().expect("encode");
    let json = std::str::from_utf8(&frame.payload)
        .expect("utf8")
        .to_string();
    assert!(json.contains(r#""code":"quorum_timeout""#), "{json}");
    assert!(json.contains(r#""retry":"outcome_unknown""#), "{json}");
    assert_eq!(Message::decode(frame).expect("decode"), message);
}

/// A newer broker's code must not cost an older client the frame: the code is
/// kept as unknown and the retry class still says what to do.
#[test]
fn an_unknown_code_decodes_with_its_retry_class() {
    let frame = crate::Frame::new(
        0,
        bytes::Bytes::from_static(
            br#"{"type":"error","message":"m","code":"tenant_frozen","retry":"retry_after","detail":{"reason":"x","future_field":1}}"#,
        ),
    )
    .expect("frame");
    match Message::decode(frame).expect("decode") {
        Message::Error {
            code,
            retry,
            detail,
            ..
        } => {
            assert_eq!(code, Some(ErrorCode::Unknown("tenant_frozen".to_string())));
            assert_eq!(retry, Some(RetryClass::RetryAfter));
            assert_eq!(detail.and_then(|d| d.reason).as_deref(), Some("x"));
        }
        other => panic!("expected error, got {other:?}"),
    }

    // An unknown class is read as fatal rather than failing the frame.
    let frame = crate::Frame::new(
        0,
        bytes::Bytes::from_static(
            br#"{"type":"publish_error","request_id":1,"message":"m","code":"internal","retry":"someday"}"#,
        ),
    )
    .expect("frame");
    match Message::decode(frame).expect("decode") {
        Message::PublishError { code, retry, .. } => {
            assert_eq!(code, Some(ErrorCode::Internal));
            assert_eq!(retry, Some(RetryClass::Fatal));
        }
        other => panic!("expected publish_error, got {other:?}"),
    }
}

/// The compatibility claim: without a code, the frames are exactly the ones a
/// broker that predates codes sends, and stripping a coded one gets back there.
#[test]
fn an_uncoded_error_is_byte_identical_to_the_old_shape() {
    #[derive(serde::Serialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    enum OldShape {
        Error { message: String },
        PublishError { request_id: u64, message: String },
    }

    let old = serde_json::to_vec(&OldShape::Error {
        message: "forbidden".to_string(),
    })
    .expect("old");
    let new = Message::error("forbidden").encode().expect("new");
    assert_eq!(new.payload.as_ref(), old.as_slice());

    let old = serde_json::to_vec(&OldShape::PublishError {
        request_id: 7,
        message: "server overloaded".to_string(),
    })
    .expect("old");
    let new = Message::publish_error(7, "server overloaded")
        .encode()
        .expect("new");
    assert_eq!(new.payload.as_ref(), old.as_slice());
}

#[test]
fn every_code_has_a_default_retry_class() {
    assert_eq!(
        ErrorCode::QuorumTimeout.default_retry(),
        RetryClass::OutcomeUnknown
    );
    assert_eq!(ErrorCode::Forbidden.default_retry(), RetryClass::Fatal);
    assert_eq!(ErrorCode::Draining.default_retry(), RetryClass::Retry);
    assert_eq!(ErrorCode::NotLeader.default_retry(), RetryClass::Redirect);
}
