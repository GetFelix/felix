//! The client-certificate envelope: round trips, the capability it needs,
//! and every frame an owner must refuse rather than read.

use super::*;

fn chain() -> Vec<Bytes> {
    vec![
        Bytes::from_static(b"leaf certificate"),
        Bytes::from_static(b"intermediate"),
    ]
}

fn cache_op(credential: &str, condition: Option<ForwardCacheCondition>) -> ForwardCacheOp {
    ForwardCacheOp {
        correlation_id: 42,
        shard: shard(),
        op: CacheOpKind::Put,
        key: "k".to_string(),
        value: Bytes::from_static(b"v"),
        ttl_ms: 0,
        credential: credential.to_string(),
        condition,
    }
}

fn certified(message: InternalMessage) -> InternalMessage {
    InternalMessage::ClientCertified(ClientCertified {
        client_chain: chain(),
        message: Box::new(message),
    })
}

/// Every request the envelope may carry, incarnated or not.
fn forwards() -> Vec<InternalMessage> {
    let incarnated = match authorized_forward() {
        InternalMessage::ForwardPublish(mut m) => {
            m.shard.incarnation = Some(9);
            InternalMessage::ForwardPublish(m)
        }
        _ => unreachable!(),
    };
    vec![
        authorized_forward(),
        incarnated,
        InternalMessage::ForwardCacheOp(cache_op("eyJ.token.sig", None)),
        InternalMessage::ForwardCacheOp(cache_op(
            "eyJ.token.sig",
            Some(ForwardCacheCondition::Absent),
        )),
        InternalMessage::LeaseForwardCacheOp(cache_op(
            "eyJ.token.sig",
            Some(ForwardCacheCondition::Absent),
        )),
    ]
}

fn frame(kind: Kind, body: &[u8]) -> Bytes {
    let mut frame = BytesMut::new();
    InternalHeader {
        kind,
        length: body.len() as u32,
    }
    .encode(&mut frame);
    frame.extend_from_slice(body);
    frame.freeze()
}

/// The envelope's body for `chain` around the encoded `inner` frame.
fn envelope(correlation_id: u64, chain: &[&[u8]], inner: &Bytes) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_u64(correlation_id);
    body.put_u8(chain.len() as u8);
    for cert in chain {
        body.put_u32(cert.len() as u32);
        body.extend_from_slice(cert);
    }
    body.extend_from_slice(&inner[6..8]);
    body.extend_from_slice(&inner[InternalHeader::LEN..]);
    body.to_vec()
}

#[test]
fn certified_forwards_round_trip() {
    for message in forwards() {
        let certified = certified(message);
        let frame = certified.encode().expect("encode");
        assert_eq!(
            InternalHeader::decode(&frame).expect("header").kind,
            Kind::ClientCertified
        );
        assert_eq!(InternalMessage::decode(frame).expect("decode"), certified);
    }
}

/// The envelope is the chain in front of the request's own frame, which is
/// byte for byte what it is without one.
#[test]
fn the_envelope_is_the_chain_in_front_of_the_plain_frame() {
    for message in forwards() {
        let plain = message.encode().expect("encode");
        let wrapped = certified(message).encode().expect("encode");
        let chain = chain();
        let refs: Vec<&[u8]> = chain.iter().map(|c| c.as_ref()).collect();
        assert_eq!(
            &wrapped[InternalHeader::LEN..],
            &envelope(42, &refs, &plain)[..]
        );
    }
}

#[test]
fn a_certified_forward_needs_client_certs_and_what_the_forward_needs() {
    assert_eq!(PeerCapabilities::CLIENT_CERTS.bits(), 1 << 27);
    assert_eq!(
        certified(authorized_forward()).required_capability(),
        Some(PeerCapabilities::CLIENT_CERTS)
    );
    let sequenced = InternalMessage::ForwardPublish(ForwardPublish {
        producer: Some(ForwardProducer {
            producer_id: 1,
            sequence: 2,
            per_record: true,
        }),
        ..match authorized_forward() {
            InternalMessage::ForwardPublish(m) => m,
            _ => unreachable!(),
        }
    });
    assert_eq!(
        certified(sequenced).required_capability(),
        Some(PeerCapabilities::CLIENT_CERTS.union(PeerCapabilities::RECORD_SEQUENCED_FORWARDS))
    );
    assert!(certified(authorized_forward()).is_forward_request());
    assert_eq!(certified(authorized_forward()).correlation_id(), 42);
}

#[test]
fn the_encoder_refuses_what_an_owner_would_refuse() {
    let refused = |client_chain: Vec<Bytes>, message: InternalMessage| {
        InternalMessage::ClientCertified(ClientCertified {
            client_chain,
            message: Box::new(message),
        })
        .encode()
        .is_err()
    };
    // No chain, an empty certificate, too many, too many bytes.
    assert!(refused(Vec::new(), authorized_forward()));
    assert!(refused(vec![Bytes::new()], authorized_forward()));
    assert!(refused(
        vec![Bytes::from_static(b"c"); MAX_CLIENT_CERTS + 1],
        authorized_forward()
    ));
    assert!(refused(
        vec![Bytes::from(vec![1u8; MAX_CLIENT_CHAIN_BYTES + 1])],
        authorized_forward()
    ));
    // A forward with no credential, and anything that is not a forward.
    assert!(refused(chain(), forward()));
    assert!(refused(
        chain(),
        InternalMessage::ForwardCacheOp(cache_op("", None))
    ));
    assert!(refused(chain(), replicate()));
    assert!(refused(chain(), certified(authorized_forward())));
    assert!(refused(
        chain(),
        InternalMessage::FromCopy(FromCopy {
            copy: [1; 16],
            message: Box::new(authorized_forward()),
        })
    ));
    // Nor may a copy's envelope go around this one.
    assert!(
        InternalMessage::FromCopy(FromCopy {
            copy: [1; 16],
            message: Box::new(certified(authorized_forward())),
        })
        .encode()
        .is_err()
    );
    // At the bounds is fine.
    assert!(!refused(
        vec![Bytes::from_static(b"c"); MAX_CLIENT_CERTS],
        authorized_forward()
    ));
    assert!(!refused(
        vec![Bytes::from(vec![1u8; MAX_CLIENT_CHAIN_BYTES])],
        authorized_forward()
    ));
}

#[test]
fn the_decoder_refuses_every_malformed_envelope() {
    let inner = authorized_forward().encode().expect("encode");
    let decodes = |body: &[u8]| InternalMessage::decode(frame(Kind::ClientCertified, body)).is_ok();
    assert!(decodes(&envelope(42, &[b"leaf"], &inner)));

    // No certificate, too many, an empty one, too many bytes.
    assert!(!decodes(&envelope(42, &[], &inner)));
    assert!(!decodes(&envelope(
        42,
        &[b"c".as_slice(); MAX_CLIENT_CERTS + 1],
        &inner
    )));
    assert!(!decodes(&envelope(42, &[b"leaf".as_slice(), b""], &inner)));
    let half = vec![1u8; MAX_CLIENT_CHAIN_BYTES / 2 + 1];
    assert!(!decodes(&envelope(42, &[&half, &half], &inner)));
    // A certificate length past the end of the body.
    let mut body = envelope(42, &[b"leaf"], &inner);
    body[9..13].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(!decodes(&body));
    // A correlation id that is not the inner request's.
    assert!(!decodes(&envelope(43, &[b"leaf"], &inner)));
    // Inside: a forward with no credential, a cache op with none, a
    // replication batch, another envelope, a copy's envelope.
    let legacy = forward().encode().expect("encode");
    assert!(!decodes(&envelope(42, &[b"leaf"], &legacy)));
    let bare_cache = InternalMessage::ForwardCacheOp(cache_op("", None))
        .encode()
        .expect("encode");
    assert!(!decodes(&envelope(42, &[b"leaf"], &bare_cache)));
    let batch = replicate().encode().expect("encode");
    assert!(!decodes(&envelope(42, &[b"leaf"], &batch)));
    let nested = certified(authorized_forward()).encode().expect("encode");
    assert!(!decodes(&envelope(42, &[b"leaf"], &nested)));
    let copied = InternalMessage::FromCopy(FromCopy {
        copy: [1; 16],
        message: Box::new(authorized_forward()),
    })
    .encode()
    .expect("encode");
    assert!(!decodes(&envelope(42, &[b"leaf"], &copied)));
    // An incarnation envelope around something that is not a forward.
    let incarnated_batch = match replicate() {
        InternalMessage::ReplicateRecords(mut m) => {
            m.shard.incarnation = Some(3);
            InternalMessage::ReplicateRecords(m)
        }
        _ => unreachable!(),
    }
    .encode()
    .expect("encode");
    assert!(!decodes(&envelope(42, &[b"leaf"], &incarnated_batch)));
    // Nor does a copy's envelope carry this one.
    let mut body = BytesMut::new();
    body.put_u64(42);
    body.put_slice(&[1; 16]);
    body.put_u16(Kind::ClientCertified as u16);
    body.extend_from_slice(&nested[InternalHeader::LEN..]);
    assert!(InternalMessage::decode(frame(Kind::FromCopy, &body)).is_err());
    // Every truncation.
    let whole = envelope(42, &[b"leaf"], &inner);
    for cut in 0..whole.len() {
        assert!(!decodes(&whole[..cut]), "cut at {cut}");
    }
}

/// The envelope's bytes count toward the frame limit, so a forward that
/// fits alone is refused once its chain no longer fits beside it.
#[test]
fn a_certified_forward_never_exceeds_the_frame_limit() {
    let sized = |len: usize| match authorized_forward() {
        InternalMessage::ForwardPublish(mut m) => {
            m.payloads = vec![Bytes::from(vec![7u8; len])];
            InternalMessage::ForwardPublish(m)
        }
        _ => unreachable!(),
    };
    let plain = sized(0).encode().expect("encode");
    let wrapped = certified(sized(0)).encode().expect("encode");
    let at_limit = MAX_BODY_BYTES as usize - (wrapped.len() - InternalHeader::LEN);
    assert!(sized(at_limit).encode().is_ok());
    let fits = certified(sized(at_limit)).encode().expect("at the limit");
    assert_eq!(
        InternalHeader::decode(&fits).expect("header").length,
        MAX_BODY_BYTES
    );
    assert!(sized(at_limit + 1).encode().is_ok());
    assert!(certified(sized(at_limit + 1)).encode().is_err());
    assert!(plain.len() < wrapped.len());
}
