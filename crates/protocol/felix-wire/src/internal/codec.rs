//! The byte layout of every internal body: `InternalMessage::encode` and
//! `decode`, and the length-checked primitives they are built from.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::replicate::GenerationStart;
use super::replicate::{ProducerMark, put_marks, take_marks};
use super::{
    AckMode, CacheOpKind, ErrorCode, Fence, FenceOk, ForwardCacheCondition, ForwardCacheError,
    ForwardCacheOk, ForwardCacheOp, ForwardCacheOutcome, ForwardPublish, ForwardPublishError,
    ForwardPublishOk, Hello, HelloOk, InternalHeader, InternalMessage, Kind, MAX_BATCH_PAYLOADS,
    MAX_BODY_BYTES, MAX_CREDENTIAL_BYTES, MAX_IDENT_BYTES, NotLeader, PeerCapabilities, Ping, Pong,
    ReplicaLog, ReplicateBootstrap, ReplicateError, ReplicateFetch, ReplicateOk, ReplicateRebuild,
    ReplicateRecords, ShardRef,
};
use crate::error::{Error, Result};

/// Every payload is a 4-byte length prefix plus its bytes, so a body can never
/// hold more payloads than it has 4-byte groups left.
const LEN_PREFIX: usize = 4;

/// The most generations one labelled batch names: as many as a log's history
/// keeps.
const MAX_GENERATIONS: usize = 512;

impl InternalMessage {
    /// Encode a complete frame: header then body.
    pub fn encode(&self) -> Result<Bytes> {
        let mut body = BytesMut::new();
        match self {
            Self::ForwardPublish(m) => {
                body.put_u64(m.correlation_id);
                put_str(&mut body, &m.shard.tenant_id)?;
                put_str(&mut body, &m.shard.namespace)?;
                put_str(&mut body, &m.shard.stream)?;
                body.put_u32(m.shard.shard);
                body.put_u64(m.shard.generation);
                body.put_u8(m.ack as u8);
                if m.payloads.len() > MAX_BATCH_PAYLOADS {
                    return Err(Error::FrameTooLarge);
                }
                body.put_u32(m.payloads.len() as u32);
                for payload in &m.payloads {
                    body.put_u32(u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?);
                    body.extend_from_slice(payload);
                }
                // Last, and only on the authorized kind: the legacy layout in
                // front of it is frozen.
                if !m.credential.is_empty() {
                    put_credential(&mut body, &m.credential)?;
                }
            }
            Self::ForwardPublishOk(m) => {
                body.put_u64(m.correlation_id);
                body.put_u64(m.first_offset);
                body.put_u64(m.last_offset);
            }
            Self::ForwardPublishError(m) => {
                body.put_u64(m.correlation_id);
                body.put_u16(m.code as u16);
                put_str(&mut body, &m.detail)?;
            }
            Self::NotLeader(m) => {
                body.put_u64(m.correlation_id);
                put_str(&mut body, &m.node_id)?;
                put_str(&mut body, &m.advertise_addr)?;
                body.put_u64(m.generation);
            }
            Self::Hello(m) => {
                body.put_u64(m.correlation_id);
                put_str(&mut body, &m.node_id)?;
                if let Some(capabilities) = m.capabilities {
                    body.put_u64(capabilities.bits());
                }
            }
            Self::HelloOk(m) => {
                body.put_u64(m.correlation_id);
                put_str(&mut body, &m.node_id)?;
                if let Some(capabilities) = m.capabilities {
                    body.put_u64(capabilities.bits());
                }
            }
            Self::Fence(m) => {
                body.put_u64(m.correlation_id);
                put_str(&mut body, &m.shard.tenant_id)?;
                put_str(&mut body, &m.shard.namespace)?;
                put_str(&mut body, &m.shard.stream)?;
                body.put_u32(m.shard.shard);
                body.put_u64(m.shard.generation);
                body.put_u8(m.log as u8);
            }
            Self::ReplicateFetch(m) => {
                body.put_u64(m.correlation_id);
                put_str(&mut body, &m.shard.tenant_id)?;
                put_str(&mut body, &m.shard.namespace)?;
                put_str(&mut body, &m.shard.stream)?;
                body.put_u32(m.shard.shard);
                body.put_u64(m.shard.generation);
                body.put_u8(m.log as u8);
                body.put_u64(m.from_offset);
                body.put_u32(m.max_bytes);
                // `labelled` is the kind, not the body.
            }
            Self::Ping(Ping { correlation_id }) | Self::Pong(Pong { correlation_id }) => {
                body.put_u64(*correlation_id);
            }
            Self::FenceOk(m) => {
                body.put_u64(m.correlation_id);
                body.put_u64(m.log_end);
                body.put_u64(m.commit_offset);
                body.put_u64(m.last_generation);
            }
            Self::ReplicateRecords(m)
            | Self::ReplicateCacheRecords(m)
            | Self::ReplicateGroupRecords(m)
            | Self::ReplicateDeadLetterRecords(m)
            | Self::ReplicateCounterRecords(m)
            | Self::ReplicateMarkedRecords(m) => {
                body.put_u64(m.correlation_id);
                put_str(&mut body, &m.shard.tenant_id)?;
                put_str(&mut body, &m.shard.namespace)?;
                put_str(&mut body, &m.shard.stream)?;
                body.put_u32(m.shard.shard);
                body.put_u64(m.shard.generation);
                body.put_u64(m.first_offset);
                body.put_u64(m.checksum);
                if m.payloads.len() > MAX_BATCH_PAYLOADS {
                    return Err(Error::FrameTooLarge);
                }
                body.put_u32(m.payloads.len() as u32);
                for payload in &m.payloads {
                    body.put_u32(u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?);
                    body.extend_from_slice(payload);
                }
                // Only the marked, committed and labelled kinds have the
                // section, and it has one mark per record however many are
                // unmarked; the other kinds' layout is frozen and cannot carry
                // any.
                let labelled = m.generations.as_deref();
                if matches!(self, Self::ReplicateMarkedRecords(_))
                    || m.commit_offset.is_some()
                    || labelled.is_some()
                    || !m.publishers.is_empty()
                {
                    let mut marks = m.marks.clone();
                    marks.resize(m.payloads.len(), ProducerMark::None);
                    put_marks(&mut body, &marks, &m.publishers);
                } else {
                    debug_assert!(
                        m.marks.iter().all(|mark| *mark == ProducerMark::None),
                        "producer marks travel only as ReplicateMarkedRecords",
                    );
                }
                if m.commit_offset.is_some() || labelled.is_some() {
                    let log = match self {
                        Self::ReplicateCacheRecords(_) => ReplicaLog::Cache,
                        Self::ReplicateGroupRecords(_) => ReplicaLog::GroupCursors,
                        Self::ReplicateDeadLetterRecords(_) => ReplicaLog::GroupDeadLetters,
                        Self::ReplicateCounterRecords(_) => ReplicaLog::Counters,
                        _ => ReplicaLog::Stream,
                    };
                    body.put_u8(log as u8);
                    match labelled {
                        Some(generations) => {
                            body.put_u8(u8::from(m.commit_offset.is_some()));
                            body.put_u64(m.commit_offset.unwrap_or(0));
                            if generations.len() > MAX_GENERATIONS {
                                return Err(Error::FrameTooLarge);
                            }
                            body.put_u32(generations.len() as u32);
                            for start in generations {
                                body.put_u64(start.generation);
                                body.put_u64(start.start_offset);
                            }
                        }
                        None => body.put_u64(m.commit_offset.unwrap_or(0)),
                    }
                }
                if self.kind() == Kind::ReplicateTimedRecords {
                    let times = m.times.as_deref().unwrap_or_default();
                    debug_assert_eq!(times.len(), m.payloads.len(), "one time per record");
                    for index in 0..m.payloads.len() {
                        body.put_u64(times.get(index).copied().unwrap_or(0));
                    }
                }
            }
            Self::ReplicateOk(m) => {
                body.put_u64(m.correlation_id);
                body.put_u64(m.durable_offset);
            }
            Self::ReplicateError(m) => {
                body.put_u64(m.correlation_id);
                body.put_u16(m.code as u16);
                body.put_u64(m.expected_offset);
                put_str(&mut body, &m.detail)?;
            }
            Self::ReplicateBootstrap(m)
            | Self::ReplicateCacheBootstrap(m)
            | Self::ReplicateGroupBootstrap(m)
            | Self::ReplicateDeadLetterBootstrap(m)
            | Self::ReplicateCounterBootstrap(m) => {
                body.put_u64(m.correlation_id);
                put_str(&mut body, &m.shard.tenant_id)?;
                put_str(&mut body, &m.shard.namespace)?;
                put_str(&mut body, &m.shard.stream)?;
                body.put_u32(m.shard.shard);
                body.put_u64(m.shard.generation);
                body.put_u64(m.base_offset);
            }
            Self::ReplicateRebuild(m) => {
                body.put_u64(m.correlation_id);
                put_str(&mut body, &m.shard.tenant_id)?;
                put_str(&mut body, &m.shard.namespace)?;
                put_str(&mut body, &m.shard.stream)?;
                body.put_u32(m.shard.shard);
                body.put_u64(m.shard.generation);
                body.put_u8(m.log as u8);
                body.put_u64(m.base_offset);
            }
            Self::ForwardCacheOp(m) => {
                body.put_u64(m.correlation_id);
                put_str(&mut body, &m.shard.tenant_id)?;
                put_str(&mut body, &m.shard.namespace)?;
                put_str(&mut body, &m.shard.stream)?;
                body.put_u32(m.shard.shard);
                body.put_u64(m.shard.generation);
                body.put_u8(m.op as u8);
                put_str(&mut body, &m.key)?;
                body.put_u64(m.ttl_ms);
                body.put_u32(u32::try_from(m.value.len()).map_err(|_| Error::FrameTooLarge)?);
                body.extend_from_slice(&m.value);
                match m.condition {
                    // The conditional kind always has a credential slot; an
                    // empty one is what a broker without auth forwards.
                    Some(condition) => {
                        if m.credential.is_empty() {
                            body.put_u32(0);
                        } else {
                            put_credential(&mut body, &m.credential)?;
                        }
                        match condition {
                            ForwardCacheCondition::Unconditional => body.put_u8(0),
                            ForwardCacheCondition::Absent => body.put_u8(1),
                            ForwardCacheCondition::Version(version) => {
                                body.put_u8(2);
                                body.put_u64(version);
                            }
                        }
                    }
                    None if !m.credential.is_empty() => {
                        put_credential(&mut body, &m.credential)?;
                    }
                    None => {}
                }
            }
            Self::ForwardCacheOk(m) => {
                body.put_u64(m.correlation_id);
                if let Some(outcome) = m.outcome {
                    body.put_u8(u8::from(outcome.applied));
                    match outcome.version {
                        Some(version) => {
                            body.put_u8(1);
                            body.put_u64(version);
                        }
                        None => body.put_u8(0),
                    }
                }
                // A presence byte rather than a zero length, so an empty stored
                // value stays distinguishable from a miss.
                match &m.value {
                    Some(value) => {
                        body.put_u8(1);
                        body.put_u32(u32::try_from(value.len()).map_err(|_| Error::FrameTooLarge)?);
                        body.extend_from_slice(value);
                    }
                    None => body.put_u8(0),
                }
            }
            Self::ForwardCacheError(m) => {
                body.put_u64(m.correlation_id);
                body.put_u16(m.code as u16);
                put_str(&mut body, &m.detail)?;
            }
        }

        let length = u32::try_from(body.len()).map_err(|_| Error::FrameTooLarge)?;
        if length > MAX_BODY_BYTES {
            return Err(Error::FrameTooLarge);
        }

        let mut frame = BytesMut::with_capacity(InternalHeader::LEN + body.len());
        InternalHeader {
            kind: self.kind(),
            length,
        }
        .encode(&mut frame);
        frame.extend_from_slice(&body);
        Ok(frame.freeze())
    }

    /// Decode a complete frame.
    ///
    /// Every peer-provided length is checked against what remains before it is
    /// used to size anything. A broker is authenticated, not assumed correct.
    pub fn decode(buf: Bytes) -> Result<Self> {
        let header = InternalHeader::decode(&buf)?;
        let body = buf.slice(InternalHeader::LEN..);
        if body.len() != header.length as usize {
            return Err(Error::Incomplete);
        }
        let mut body = body;

        match header.kind {
            kind @ (Kind::ForwardPublish | Kind::AuthorizedForwardPublish) => {
                let correlation_id = take_u64(&mut body)?;
                let tenant_id = take_str(&mut body)?;
                let namespace = take_str(&mut body)?;
                let stream = take_str(&mut body)?;
                let shard = take_u32(&mut body)?;
                let generation = take_u64(&mut body)?;
                let ack = AckMode::from_u8(take_u8(&mut body)?)?;

                let declared = take_u32(&mut body)? as usize;
                // Bounded against what the body could actually hold, before it
                // reaches `with_capacity`. Same trap the client binary path
                // documents: a tiny frame declaring u32::MAX payloads otherwise
                // reserves enough address space to abort the process.
                if declared > MAX_BATCH_PAYLOADS || declared > body.remaining() / LEN_PREFIX {
                    return Err(Error::Incomplete);
                }
                let mut payloads = Vec::with_capacity(declared);
                for _ in 0..declared {
                    let len = take_u32(&mut body)? as usize;
                    if len > body.remaining() {
                        return Err(Error::Incomplete);
                    }
                    payloads.push(body.split_to(len));
                }
                let credential = if kind == Kind::AuthorizedForwardPublish {
                    take_credential(&mut body)?
                } else {
                    String::new()
                };
                if body.has_remaining() {
                    // Trailing bytes mean the body did not describe itself, so
                    // something is wrong with the peer, not merely with this
                    // message.
                    return Err(Error::Incomplete);
                }

                Ok(Self::ForwardPublish(ForwardPublish {
                    correlation_id,
                    shard: ShardRef {
                        tenant_id,
                        namespace,
                        stream,
                        shard,
                        generation,
                    },
                    ack,
                    payloads,
                    credential,
                }))
            }
            Kind::ForwardPublishOk => {
                let message = ForwardPublishOk {
                    correlation_id: take_u64(&mut body)?,
                    first_offset: take_u64(&mut body)?,
                    last_offset: take_u64(&mut body)?,
                };
                expect_empty(&body)?;
                Ok(Self::ForwardPublishOk(message))
            }
            Kind::ForwardPublishError => {
                let message = ForwardPublishError {
                    correlation_id: take_u64(&mut body)?,
                    code: ErrorCode::from_u16(take_u16(&mut body)?)?,
                    detail: take_str(&mut body)?,
                };
                expect_empty(&body)?;
                Ok(Self::ForwardPublishError(message))
            }
            Kind::NotLeader => {
                let message = NotLeader {
                    correlation_id: take_u64(&mut body)?,
                    node_id: take_str(&mut body)?,
                    advertise_addr: take_str(&mut body)?,
                    generation: take_u64(&mut body)?,
                };
                expect_empty(&body)?;
                Ok(Self::NotLeader(message))
            }
            Kind::Hello | Kind::HelloCapable => {
                let message = Hello {
                    correlation_id: take_u64(&mut body)?,
                    node_id: take_str(&mut body)?,
                    capabilities: match header.kind {
                        Kind::HelloCapable => {
                            Some(PeerCapabilities::from_bits(take_u64(&mut body)?))
                        }
                        _ => None,
                    },
                };
                expect_empty(&body)?;
                Ok(Self::Hello(message))
            }
            Kind::HelloOk | Kind::HelloCapableOk => {
                let message = HelloOk {
                    correlation_id: take_u64(&mut body)?,
                    node_id: take_str(&mut body)?,
                    capabilities: match header.kind {
                        Kind::HelloCapableOk => {
                            Some(PeerCapabilities::from_bits(take_u64(&mut body)?))
                        }
                        _ => None,
                    },
                };
                expect_empty(&body)?;
                Ok(Self::HelloOk(message))
            }
            Kind::Fence => {
                let message = Fence {
                    correlation_id: take_u64(&mut body)?,
                    shard: ShardRef {
                        tenant_id: take_str(&mut body)?,
                        namespace: take_str(&mut body)?,
                        stream: take_str(&mut body)?,
                        shard: take_u32(&mut body)?,
                        generation: take_u64(&mut body)?,
                    },
                    log: ReplicaLog::from_u8(take_u8(&mut body)?)?,
                };
                expect_empty(&body)?;
                Ok(Self::Fence(message))
            }
            Kind::ReplicateFetch | Kind::ReplicateLabelledFetch | Kind::ReplicateTimedFetch => {
                let message = ReplicateFetch {
                    correlation_id: take_u64(&mut body)?,
                    shard: ShardRef {
                        tenant_id: take_str(&mut body)?,
                        namespace: take_str(&mut body)?,
                        stream: take_str(&mut body)?,
                        shard: take_u32(&mut body)?,
                        generation: take_u64(&mut body)?,
                    },
                    log: ReplicaLog::from_u8(take_u8(&mut body)?)?,
                    from_offset: take_u64(&mut body)?,
                    max_bytes: take_u32(&mut body)?,
                    labelled: header.kind != Kind::ReplicateFetch,
                    timed: header.kind == Kind::ReplicateTimedFetch,
                };
                expect_empty(&body)?;
                Ok(Self::ReplicateFetch(message))
            }
            Kind::Ping | Kind::Pong => {
                let correlation_id = take_u64(&mut body)?;
                expect_empty(&body)?;
                Ok(if header.kind == Kind::Ping {
                    Self::Ping(Ping { correlation_id })
                } else {
                    Self::Pong(Pong { correlation_id })
                })
            }
            Kind::FenceOk => {
                let message = FenceOk {
                    correlation_id: take_u64(&mut body)?,
                    log_end: take_u64(&mut body)?,
                    commit_offset: take_u64(&mut body)?,
                    last_generation: take_u64(&mut body)?,
                };
                expect_empty(&body)?;
                Ok(Self::FenceOk(message))
            }
            Kind::ReplicateRecords
            | Kind::ReplicateCacheRecords
            | Kind::ReplicateGroupRecords
            | Kind::ReplicateDeadLetterRecords
            | Kind::ReplicateCounterRecords
            | Kind::ReplicateMarkedRecords
            | Kind::ReplicateCommittedRecords
            | Kind::ReplicateLabelledRecords
            | Kind::ReplicateTimedRecords => {
                let correlation_id = take_u64(&mut body)?;
                let tenant_id = take_str(&mut body)?;
                let namespace = take_str(&mut body)?;
                let stream = take_str(&mut body)?;
                let shard = take_u32(&mut body)?;
                let generation = take_u64(&mut body)?;
                let first_offset = take_u64(&mut body)?;
                let checksum = take_u64(&mut body)?;

                let declared = take_u32(&mut body)? as usize;
                // Bounded against what the body could hold before it reaches
                // `with_capacity`, exactly as `ForwardPublish` documents.
                if declared > MAX_BATCH_PAYLOADS || declared > body.remaining() / LEN_PREFIX {
                    return Err(Error::Incomplete);
                }
                let mut payloads = Vec::with_capacity(declared);
                for _ in 0..declared {
                    let len = take_u32(&mut body)? as usize;
                    if len > body.remaining() {
                        return Err(Error::Incomplete);
                    }
                    payloads.push(body.split_to(len));
                }
                let timed = header.kind == Kind::ReplicateTimedRecords;
                let labelled = header.kind == Kind::ReplicateLabelledRecords || timed;
                let committed = header.kind == Kind::ReplicateCommittedRecords || labelled;
                let (mut marks, publishers) =
                    if header.kind == Kind::ReplicateMarkedRecords || committed {
                        take_marks(&mut body, payloads.len())?
                    } else {
                        (Vec::new(), Vec::new())
                    };
                let (log, commit_offset) = if labelled {
                    let log = ReplicaLog::from_u8(take_u8(&mut body)?)?;
                    let present = take_u8(&mut body)?;
                    let commit = take_u64(&mut body)?;
                    // An absent offset still occupies the field, and it must be
                    // zero: the encoder writes zero, so accepting anything else
                    // lets two different frames decode to the same message.
                    let commit = match present {
                        0 if commit == 0 => None,
                        1 => Some(commit),
                        _ => return Err(Error::Incomplete),
                    };
                    (Some(log), commit)
                } else if committed {
                    let log = ReplicaLog::from_u8(take_u8(&mut body)?)?;
                    (Some(log), Some(take_u64(&mut body)?))
                } else {
                    (None, None)
                };
                let generations = if labelled {
                    let declared = take_u32(&mut body)? as usize;
                    if declared > MAX_GENERATIONS || declared > body.remaining() / 16 {
                        return Err(Error::Incomplete);
                    }
                    let mut generations = Vec::with_capacity(declared);
                    for _ in 0..declared {
                        generations.push(GenerationStart {
                            generation: take_u64(&mut body)?,
                            start_offset: take_u64(&mut body)?,
                        });
                    }
                    Some(generations)
                } else {
                    None
                };
                let times = if timed {
                    if body.remaining() / 8 < payloads.len() {
                        return Err(Error::Incomplete);
                    }
                    Some((0..payloads.len()).map(|_| body.get_u64()).collect())
                } else {
                    None
                };
                expect_empty(&body)?;
                // An unmarked batch has no marks, as the leader built it; the
                // checksum covers marks only when there are any.
                if (committed || !publishers.is_empty())
                    && marks.iter().all(|mark| *mark == ProducerMark::None)
                {
                    marks.clear();
                }

                let message = ReplicateRecords {
                    correlation_id,
                    shard: ShardRef {
                        tenant_id,
                        namespace,
                        stream,
                        shard,
                        generation,
                    },
                    first_offset,
                    checksum,
                    payloads,
                    marks,
                    publishers,
                    commit_offset,
                    generations,
                    times,
                };
                if let Some(log) = log {
                    return Ok(match log {
                        ReplicaLog::Stream if message.marks.is_empty() => {
                            Self::ReplicateRecords(message)
                        }
                        ReplicaLog::Stream => Self::ReplicateMarkedRecords(message),
                        ReplicaLog::Cache => Self::ReplicateCacheRecords(message),
                        ReplicaLog::GroupCursors => Self::ReplicateGroupRecords(message),
                        ReplicaLog::GroupDeadLetters => Self::ReplicateDeadLetterRecords(message),
                        ReplicaLog::Counters => Self::ReplicateCounterRecords(message),
                    });
                }
                Ok(match header.kind {
                    Kind::ReplicateCacheRecords => Self::ReplicateCacheRecords(message),
                    Kind::ReplicateGroupRecords => Self::ReplicateGroupRecords(message),
                    Kind::ReplicateDeadLetterRecords => Self::ReplicateDeadLetterRecords(message),
                    Kind::ReplicateCounterRecords => Self::ReplicateCounterRecords(message),
                    // Sent as marked only to carry its publishers.
                    Kind::ReplicateMarkedRecords
                        if message.marks.is_empty() && !message.publishers.is_empty() =>
                    {
                        Self::ReplicateRecords(message)
                    }
                    Kind::ReplicateMarkedRecords => Self::ReplicateMarkedRecords(message),
                    _ => Self::ReplicateRecords(message),
                })
            }
            Kind::ReplicateOk => {
                let message = ReplicateOk {
                    correlation_id: take_u64(&mut body)?,
                    durable_offset: take_u64(&mut body)?,
                };
                expect_empty(&body)?;
                Ok(Self::ReplicateOk(message))
            }
            Kind::ReplicateError => {
                let message = ReplicateError {
                    correlation_id: take_u64(&mut body)?,
                    code: ErrorCode::from_u16(take_u16(&mut body)?)?,
                    expected_offset: take_u64(&mut body)?,
                    detail: take_str(&mut body)?,
                };
                expect_empty(&body)?;
                Ok(Self::ReplicateError(message))
            }
            Kind::ReplicateBootstrap
            | Kind::ReplicateCacheBootstrap
            | Kind::ReplicateGroupBootstrap
            | Kind::ReplicateDeadLetterBootstrap
            | Kind::ReplicateCounterBootstrap => {
                let message = ReplicateBootstrap {
                    correlation_id: take_u64(&mut body)?,
                    shard: ShardRef {
                        tenant_id: take_str(&mut body)?,
                        namespace: take_str(&mut body)?,
                        stream: take_str(&mut body)?,
                        shard: take_u32(&mut body)?,
                        generation: take_u64(&mut body)?,
                    },
                    base_offset: take_u64(&mut body)?,
                };
                expect_empty(&body)?;
                Ok(match header.kind {
                    Kind::ReplicateCacheBootstrap => Self::ReplicateCacheBootstrap(message),
                    Kind::ReplicateGroupBootstrap => Self::ReplicateGroupBootstrap(message),
                    Kind::ReplicateDeadLetterBootstrap => {
                        Self::ReplicateDeadLetterBootstrap(message)
                    }
                    Kind::ReplicateCounterBootstrap => Self::ReplicateCounterBootstrap(message),
                    _ => Self::ReplicateBootstrap(message),
                })
            }
            Kind::ReplicateRebuild => {
                let message = ReplicateRebuild {
                    correlation_id: take_u64(&mut body)?,
                    shard: ShardRef {
                        tenant_id: take_str(&mut body)?,
                        namespace: take_str(&mut body)?,
                        stream: take_str(&mut body)?,
                        shard: take_u32(&mut body)?,
                        generation: take_u64(&mut body)?,
                    },
                    log: ReplicaLog::from_u8(take_u8(&mut body)?)?,
                    base_offset: take_u64(&mut body)?,
                };
                expect_empty(&body)?;
                Ok(Self::ReplicateRebuild(message))
            }
            kind @ (Kind::ForwardCacheOp
            | Kind::AuthorizedForwardCacheOp
            | Kind::ConditionalForwardCacheOp) => {
                let correlation_id = take_u64(&mut body)?;
                let shard = ShardRef {
                    tenant_id: take_str(&mut body)?,
                    namespace: take_str(&mut body)?,
                    stream: take_str(&mut body)?,
                    shard: take_u32(&mut body)?,
                    generation: take_u64(&mut body)?,
                };
                let op = CacheOpKind::from_u8(take_u8(&mut body)?)?;
                let key = take_str(&mut body)?;
                let ttl_ms = take_u64(&mut body)?;
                let len = take_u32(&mut body)? as usize;
                if len > body.remaining() {
                    return Err(Error::Incomplete);
                }
                let value = body.split_to(len);
                let (credential, condition) = match kind {
                    Kind::AuthorizedForwardCacheOp => (take_credential(&mut body)?, None),
                    Kind::ConditionalForwardCacheOp => {
                        let credential = take_optional_credential(&mut body)?;
                        let condition = match take_u8(&mut body)? {
                            0 => ForwardCacheCondition::Unconditional,
                            1 => ForwardCacheCondition::Absent,
                            2 => ForwardCacheCondition::Version(take_u64(&mut body)?),
                            _ => return Err(Error::Incomplete),
                        };
                        (credential, Some(condition))
                    }
                    _ => (String::new(), None),
                };
                expect_empty(&body)?;
                Ok(Self::ForwardCacheOp(ForwardCacheOp {
                    correlation_id,
                    shard,
                    op,
                    key,
                    value,
                    ttl_ms,
                    credential,
                    condition,
                }))
            }
            kind @ (Kind::ForwardCacheOk | Kind::ConditionalForwardCacheOk) => {
                let correlation_id = take_u64(&mut body)?;
                let outcome = if kind == Kind::ConditionalForwardCacheOk {
                    let applied = match take_u8(&mut body)? {
                        0 => false,
                        1 => true,
                        _ => return Err(Error::Incomplete),
                    };
                    let version = match take_u8(&mut body)? {
                        0 => None,
                        1 => Some(take_u64(&mut body)?),
                        _ => return Err(Error::Incomplete),
                    };
                    Some(ForwardCacheOutcome { applied, version })
                } else {
                    None
                };
                let value = match take_u8(&mut body)? {
                    0 => None,
                    1 => {
                        let len = take_u32(&mut body)? as usize;
                        if len > body.remaining() {
                            return Err(Error::Incomplete);
                        }
                        Some(body.split_to(len))
                    }
                    // Neither present nor absent is not a value this can guess
                    // at: a miss and an empty value mean different things.
                    _ => return Err(Error::Incomplete),
                };
                expect_empty(&body)?;
                Ok(Self::ForwardCacheOk(ForwardCacheOk {
                    correlation_id,
                    value,
                    outcome,
                }))
            }
            Kind::ForwardCacheError => {
                let message = ForwardCacheError {
                    correlation_id: take_u64(&mut body)?,
                    code: ErrorCode::from_u16(take_u16(&mut body)?)?,
                    detail: take_str(&mut body)?,
                };
                expect_empty(&body)?;
                Ok(Self::ForwardCacheError(message))
            }
        }
    }
}

fn put_str(buf: &mut BytesMut, value: &str) -> Result<()> {
    if value.len() > MAX_IDENT_BYTES {
        return Err(Error::FrameTooLarge);
    }
    buf.put_u32(value.len() as u32);
    buf.extend_from_slice(value.as_bytes());
    Ok(())
}

/// A credential is a string with its own, larger bound.
fn put_credential(buf: &mut BytesMut, value: &str) -> Result<()> {
    if value.is_empty() || value.len() > MAX_CREDENTIAL_BYTES {
        return Err(Error::FrameTooLarge);
    }
    buf.put_u32(value.len() as u32);
    buf.extend_from_slice(value.as_bytes());
    Ok(())
}

fn take_credential(buf: &mut Bytes) -> Result<String> {
    let len = take_u32(buf)? as usize;
    // An authorized kind with an empty credential is a contradiction, and
    // decoding it as the legacy kind would let a peer choose the weaker check
    // by sending the stronger kind.
    if len == 0 || len > MAX_CREDENTIAL_BYTES || len > buf.remaining() {
        return Err(Error::Incomplete);
    }
    let bytes = buf.split_to(len);
    String::from_utf8(bytes.to_vec()).map_err(|_| Error::Incomplete)
}

/// A credential that may be absent, written as a zero length.
fn take_optional_credential(buf: &mut Bytes) -> Result<String> {
    let len = take_u32(buf)? as usize;
    if len > MAX_CREDENTIAL_BYTES || len > buf.remaining() {
        return Err(Error::Incomplete);
    }
    let bytes = buf.split_to(len);
    String::from_utf8(bytes.to_vec()).map_err(|_| Error::Incomplete)
}

fn take_str(buf: &mut Bytes) -> Result<String> {
    let len = take_u32(buf)? as usize;
    if len > MAX_IDENT_BYTES || len > buf.remaining() {
        return Err(Error::Incomplete);
    }
    let bytes = buf.split_to(len);
    String::from_utf8(bytes.to_vec()).map_err(|_| Error::InvalidUtf8)
}

fn take_u8(buf: &mut Bytes) -> Result<u8> {
    if buf.remaining() < 1 {
        return Err(Error::Incomplete);
    }
    Ok(buf.get_u8())
}

fn take_u16(buf: &mut Bytes) -> Result<u16> {
    if buf.remaining() < 2 {
        return Err(Error::Incomplete);
    }
    Ok(buf.get_u16())
}

fn take_u32(buf: &mut Bytes) -> Result<u32> {
    if buf.remaining() < 4 {
        return Err(Error::Incomplete);
    }
    Ok(buf.get_u32())
}

fn take_u64(buf: &mut Bytes) -> Result<u64> {
    if buf.remaining() < 8 {
        return Err(Error::Incomplete);
    }
    Ok(buf.get_u64())
}

/// Reject trailing bytes: a body that does not describe itself exactly means
/// the peer and this decoder disagree about the layout.
fn expect_empty(buf: &Bytes) -> Result<()> {
    if buf.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(())
}
