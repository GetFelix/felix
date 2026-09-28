//! Which Kafka APIs are answered, at which versions, and by what.
//!
//! The version ranges are what `ApiVersions` advertises and what a request is
//! checked against. They are chosen from what librdkafka and the Java client
//! negotiate, and stop short of the versions that address topics by id
//! (`Fetch` v13 on): Felix streams have names, not ids.

mod fetch;
mod groups;
mod list_offsets;
mod metadata;
mod partition;
mod produce;
mod producer_id;
pub(crate) mod sasl;
mod transactions;
mod versions;

use std::time::Duration;

use anyhow::{Context, Result};
use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, FetchRequest, InitProducerIdRequest, ListOffsetsRequest, MetadataRequest,
    ProduceRequest, RequestHeader, SaslAuthenticateRequest, SaslHandshakeRequest,
};
use kafka_protocol::protocol::{Decodable, Encodable};
use tokio_util::sync::CancellationToken;

use crate::cluster::Principal;
use crate::service::Shared;

/// `(api, lowest version, highest version)` answered.
pub(crate) const SUPPORTED: &[(ApiKey, i16, i16)] = &[
    (ApiKey::Produce, 3, 9),
    (ApiKey::Fetch, 4, 12),
    (ApiKey::ListOffsets, 1, 7),
    (ApiKey::Metadata, 0, 12),
    (ApiKey::ApiVersions, 0, 3),
    // librdkafka will not use SASL unless v0 is listed, but v0 carries the
    // exchange outside Kafka framing, so a v0 handshake is refused and v1
    // (which moves it into `SaslAuthenticate`) is what works.
    (ApiKey::SaslHandshake, 0, 1),
    (ApiKey::SaslAuthenticate, 0, 2),
    // For idempotent producers. A transactional id is refused.
    (ApiKey::InitProducerId, 0, 4),
    // Answered only to refuse: Felix has no Kafka consumer groups. See
    // `groups`.
    (ApiKey::FindCoordinator, 0, 4),
];

/// Group APIs a client should never reach, since `FindCoordinator` is always
/// refused, answered with the same refusal if one arrives anyway.
pub(crate) const REFUSED_GROUP_APIS: &[(ApiKey, i16, i16)] = &[
    (ApiKey::JoinGroup, 0, 5),
    (ApiKey::SyncGroup, 0, 3),
    (ApiKey::Heartbeat, 0, 3),
    (ApiKey::LeaveGroup, 0, 3),
    (ApiKey::OffsetCommit, 2, 7),
    (ApiKey::OffsetFetch, 2, 7),
];

/// Transaction APIs, not advertised and answered with the refusal if a client
/// sends one anyway. See `transactions`.
pub(crate) const REFUSED_TRANSACTION_APIS: &[(ApiKey, i16, i16)] = &[
    (ApiKey::AddPartitionsToTxn, 0, 3),
    (ApiKey::AddOffsetsToTxn, 0, 3),
    (ApiKey::EndTxn, 0, 3),
    (ApiKey::TxnOffsetCommit, 0, 3),
];

/// What to do after a request.
pub(crate) enum Answer {
    Respond {
        correlation_id: i32,
        header_version: i16,
        body: Bytes,
    },
    /// Say nothing and read the next request: an `acks=0` produce.
    Silent,
    /// Close the connection without answering. Kafka brokers do the same for
    /// a request they cannot parse or were never offered.
    Close(&'static str),
}

/// Per-connection state: who the client is, and where SASL has got to.
pub(crate) struct Session {
    principal: Option<Principal>,
    sasl: SaslState,
    closing: bool,
    /// How long to stop reading after the current answer, because the
    /// tenant is over its publish quota. Kafka's own quota enforcement: the
    /// response carries the same time as `throttle_time_ms`, so a client that
    /// honours it waits the same window instead of on top of it.
    throttle: Duration,
    /// The TLS client's certificate chain, DER, leaf first. Empty without one.
    peer_certs: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SaslState {
    /// Nothing yet, or authenticated and free to start again.
    Idle,
    /// `SaslHandshake` chose PLAIN; `SaslAuthenticate` is next.
    Handshaken,
}

impl Session {
    pub(crate) fn new(shared: &Shared, peer_certs: Vec<Vec<u8>>) -> Self {
        Self {
            principal: shared
                .settings
                .anonymous_tenant
                .as_ref()
                .map(|tenant| Principal::anonymous(tenant.clone())),
            sasl: SaslState::Idle,
            closing: false,
            throttle: Duration::ZERO,
            peer_certs,
        }
    }

    /// Whether the connection should close once the last answer is written.
    pub(crate) fn closing(&self) -> bool {
        self.closing
    }

    /// The mute the last request earned, taken so it is served once.
    pub(crate) fn take_throttle(&mut self) -> Duration {
        std::mem::take(&mut self.throttle)
    }

    /// Whether the connection has a principal, by SASL or as the anonymous
    /// tenant.
    pub(crate) fn authenticated(&self) -> bool {
        self.principal.is_some()
    }
}

/// Answer one request frame.
pub(crate) async fn handle(
    shared: &Shared,
    session: &mut Session,
    frame: Bytes,
    shutdown: &CancellationToken,
) -> Result<Answer> {
    let request = match parse(frame)? {
        Parsed::Request(request) => request,
        Parsed::Answer(answer) => return Ok(answer),
    };
    let Request {
        api,
        version,
        correlation_id,
        header,
        body,
    } = *request;
    let header_version = api.response_header_version(version);
    let (body, error) = match body {
        Body::ApiVersions => versions::answer(version)?,
        Body::SaslHandshake(request) => sasl::handshake(session, request, version)?,
        Body::SaslAuthenticate(request) => {
            sasl::authenticate(shared, session, request, version).await?
        }
        Body::Metadata(request) => {
            metadata::answer(shared, session.principal.as_ref(), request, version).await?
        }
        Body::ListOffsets(request) => {
            list_offsets::answer(shared, session.principal.as_ref(), request, version).await?
        }
        Body::Produce(request) => {
            let (body, error, throttle) =
                produce::answer(shared, session.principal.as_ref(), request, version).await?;
            session.throttle = throttle;
            match body {
                Some(body) => (body, error),
                None => {
                    crate::metrics::request(api, error);
                    return Ok(Answer::Silent);
                }
            }
        }
        Body::InitProducerId(request) => {
            producer_id::answer(shared, session.principal.as_ref(), request, version)?
        }
        Body::Fetch(request) => {
            fetch::answer(
                shared,
                session.principal.as_ref(),
                request,
                version,
                shutdown,
            )
            .await?
        }
        Body::Refused { body, error } => (body, error),
    };
    crate::metrics::request(api, error);
    tracing::trace!(
        ?api,
        version,
        error,
        client = header.client_id.as_deref().unwrap_or("-"),
        "kafka request",
    );
    Ok(Answer::Respond {
        correlation_id,
        header_version,
        body,
    })
}

/// A request frame decoded, or the answer it gets without reaching the
/// cluster.
pub(crate) enum Parsed {
    Request(Box<Request>),
    Answer(Answer),
}

/// A request whose header and body decoded.
pub(crate) struct Request {
    pub(crate) api: ApiKey,
    pub(crate) version: i16,
    pub(crate) correlation_id: i32,
    pub(crate) header: RequestHeader,
    pub(crate) body: Body,
}

/// A decoded request body, one variant per API answered.
pub(crate) enum Body {
    ApiVersions,
    SaslHandshake(SaslHandshakeRequest),
    SaslAuthenticate(SaslAuthenticateRequest),
    Metadata(MetadataRequest),
    ListOffsets(ListOffsetsRequest),
    Produce(ProduceRequest),
    InitProducerId(InitProducerIdRequest),
    Fetch(FetchRequest),
    /// A group or transaction API. Its refusal needs nothing from the
    /// cluster, so it is already encoded.
    Refused {
        body: Bytes,
        error: i16,
    },
}

/// Decode a request frame as far as it goes without the cluster.
///
/// Every byte a client controls in a request is read here, which is what the
/// deterministic tests in `api/tests/parse.rs` drive. An error closes the connection.
pub(crate) fn parse(mut frame: Bytes) -> Result<Parsed> {
    let (raw_key, version, correlation_id) = peek_header(&frame)?;
    let Ok(api) = ApiKey::try_from(raw_key) else {
        crate::metrics::refused("unknown_api");
        tracing::debug!(api_key = raw_key, "kafka request for an unknown api");
        return Ok(Parsed::Answer(Answer::Close("unknown api key")));
    };

    // A client asks for the newest `ApiVersions` it knows before it knows what
    // this broker speaks, so an unsupported version is answered (in the v0
    // shape) rather than refused.
    if api == ApiKey::ApiVersions && !in_range(api, version) {
        crate::metrics::request(api, versions::UNSUPPORTED_VERSION);
        return Ok(Parsed::Answer(Answer::Respond {
            correlation_id,
            header_version: 0,
            body: versions::unsupported(),
        }));
    }
    let listed = |apis: &[(ApiKey, i16, i16)]| {
        apis.iter()
            .any(|(key, min, max)| *key == api && (*min..=*max).contains(&version))
    };
    let refused_group_api = listed(REFUSED_GROUP_APIS);
    let refused_transaction_api = listed(REFUSED_TRANSACTION_APIS);
    if !in_range(api, version) && !refused_group_api && !refused_transaction_api {
        crate::metrics::refused("unsupported_api");
        tracing::debug!(
            ?api,
            version,
            "kafka request for an api this listener does not offer"
        );
        return Ok(Parsed::Answer(Answer::Close("api or version not offered")));
    }

    let header = RequestHeader::decode(&mut frame, api.request_header_version(version))
        .context("decode request header")?;
    let body = match api {
        ApiKey::ApiVersions => Body::ApiVersions,
        ApiKey::SaslHandshake => Body::SaslHandshake(decode(&mut frame, version)?),
        ApiKey::SaslAuthenticate => Body::SaslAuthenticate(decode(&mut frame, version)?),
        ApiKey::Metadata => Body::Metadata(decode(&mut frame, version)?),
        ApiKey::ListOffsets => Body::ListOffsets(decode(&mut frame, version)?),
        ApiKey::Produce => Body::Produce(decode(&mut frame, version)?),
        ApiKey::InitProducerId => Body::InitProducerId(decode(&mut frame, version)?),
        ApiKey::Fetch => Body::Fetch(decode(&mut frame, version)?),
        _ => {
            let (body, error) = if refused_transaction_api {
                transactions::refuse(api, &mut frame, version)?
            } else {
                groups::refuse(api, &mut frame, version)?
            };
            Body::Refused { body, error }
        }
    };
    Ok(Parsed::Request(Box::new(Request {
        api,
        version,
        correlation_id,
        header,
        body,
    })))
}

/// The api key, version and correlation id every request starts with, read
/// without decoding the rest.
fn peek_header(frame: &Bytes) -> Result<(i16, i16, i32)> {
    // The connection already refuses a shorter frame; checked here too so the
    // fuzz target, which skips the connection, gets an error and not a panic.
    let mut head = frame.get(..8).context("request shorter than its header")?;
    Ok((head.get_i16(), head.get_i16(), head.get_i32()))
}

fn in_range(api: ApiKey, version: i16) -> bool {
    SUPPORTED
        .iter()
        .any(|(key, min, max)| *key == api && (*min..=*max).contains(&version))
}

fn decode<T: Decodable>(frame: &mut Bytes, version: i16) -> Result<T> {
    T::decode(frame, version).context("decode request body")
}

/// Encode a response body, and the error code it is counted under.
pub(crate) fn encode<T: Encodable>(message: &T, version: i16, error: i16) -> Result<(Bytes, i16)> {
    let mut out = BytesMut::new();
    message
        .encode(&mut out, version)
        .context("encode response")?;
    Ok((out.freeze(), error))
}

/// The first non-zero code among many, for counting a per-partition answer.
pub(crate) fn first_error(codes: impl IntoIterator<Item = i16>) -> i16 {
    codes.into_iter().find(|code| *code != 0).unwrap_or(0)
}

#[cfg(test)]
mod tests;
