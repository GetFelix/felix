//! What the Kafka side needs to know about the cluster it is part of.
//!
//! The broker service implements [`Cluster`]: it verifies credentials, knows
//! which broker leads each shard, and knows where every broker's Kafka listener
//! is. Keeping that behind a trait is what lets this crate be tested against a
//! single in-process broker, and keeps control-plane and routing types out of
//! it.

use std::any::Any;
use std::time::Duration;

use async_trait::async_trait;
use felix_authz::{Action, Namespace, PermissionMatcher, StreamName, TenantId, stream_resource};
use felix_broker::{PublishOutcome, StreamHandle};

/// The broker's side of the Kafka listener.
#[async_trait]
pub trait Cluster: Send + Sync + 'static {
    /// Verify a Felix token for `tenant_id`, the same check the QUIC `Auth`
    /// frame gets.
    async fn authenticate(&self, tenant_id: &str, token: &str) -> Result<Principal, String>;

    /// [`Self::authenticate`] for a connection whose TLS client presented
    /// `peer_certs` (DER, leaf first; empty without TLS or without a client
    /// certificate), so the broker can bind the token to that certificate.
    async fn authenticate_peer(
        &self,
        tenant_id: &str,
        token: &str,
        peer_certs: &[Vec<u8>],
    ) -> Result<Principal, String> {
        let _ = peer_certs;
        self.authenticate(tenant_id, token).await
    }

    /// Every broker a Kafka client may be sent to, this one included.
    fn brokers(&self) -> Vec<Endpoint>;

    /// This broker's node id, as [`Endpoint::node_id`] and
    /// [`Placement::Remote`] spell it.
    fn local_node_id(&self) -> String;

    /// Who serves one shard right now.
    fn placement(&self, shard: &ShardRef<'_>) -> Placement;

    /// Admit one write to a shard, the way a Felix publish is admitted: this
    /// broker must lead the shard, hold its lease, and the shard's write fence
    /// must be open. Hold the permit until the write is durable, so a move
    /// waits for it.
    async fn admit_write(&self, shard: &ShardRef<'_>) -> Result<WritePermit, WriteError>;

    /// Wait until a written batch has what its stream's consistency asks for:
    /// a majority of the replica set on a `Quorum` stream, nothing more on a
    /// `Leader` one.
    async fn await_consistency(
        &self,
        shard: &ShardRef<'_>,
        handle: &StreamHandle,
        outcome: &PublishOutcome,
    ) -> Result<(), WriteError>;

    /// The offset below which a shard's records are committed, which is where
    /// a fetch stops and what it reports as the high watermark. `None` when
    /// everything durable here is committed: a `Leader` stream, or a shard
    /// with no replicas. A `Quorum` shard's is its quorum mark, since a record
    /// past it can be lost at failover and its offset reused.
    fn committed_until(&self, shard: &ShardRef<'_>, handle: &StreamHandle) -> Option<u64> {
        let _ = (shard, handle);
        None
    }

    /// Account for `records` records of `bytes` payload that `tenant_id` is
    /// about to produce, and say how long to hold the request to keep the
    /// tenant within its publish quota. Kafka clients are throttled by delay,
    /// not refused, and the delay is reported back as `throttle_time_ms`.
    fn admit_produce(&self, _tenant_id: &str, _records: u64, _bytes: u64) -> Duration {
        Duration::ZERO
    }
}

/// A write's place in its shard's fence. Dropping it lets a move proceed.
#[derive(Default)]
pub struct WritePermit {
    _guard: Option<Box<dyn Any + Send + Sync>>,
}

impl WritePermit {
    /// A permit that holds `guard` until it is dropped.
    pub fn holding(guard: impl Any + Send + Sync) -> Self {
        Self {
            _guard: Some(Box::new(guard)),
        }
    }
}

/// Why a write was not taken, or not confirmed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteError {
    /// This broker does not lead the shard, or stopped leading it before the
    /// write claimed its offsets. Nothing was written.
    NotLeader,
    /// Written here, but a majority did not confirm it in time. It may
    /// survive or not.
    QuorumTimeout,
    /// Leadership moved after the write and before a majority held it.
    LeadershipLost,
}

/// A broker's Kafka listener, as clients are told to reach it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub node_id: String,
    pub host: String,
    pub port: u16,
}

impl Endpoint {
    /// Parse a `host:port` address. IPv6 hosts are bracketed.
    pub fn parse(node_id: impl Into<String>, addr: &str) -> Option<Self> {
        let (host, port) = addr.rsplit_once(':')?;
        let port: u16 = port.parse().ok().filter(|port| *port != 0)?;
        let host = host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(host);
        if host.is_empty() {
            return None;
        }
        Some(Self {
            node_id: node_id.into(),
            host: host.to_string(),
            port,
        })
    }
}

/// One stream shard, which a Kafka client calls a partition.
#[derive(Debug, Clone, Copy)]
pub struct ShardRef<'a> {
    pub tenant_id: &'a str,
    pub namespace: &'a str,
    pub stream: &'a str,
    pub shard: u32,
}

/// Who serves a shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    /// This broker leads it and has it open.
    Local { replicas: Vec<String> },
    /// Another broker leads it.
    Remote {
        leader: String,
        replicas: Vec<String>,
    },
    /// Nobody can serve it right now: unassigned, still opening, or moving.
    Unavailable,
}

/// Who a connection is, once it has authenticated.
#[derive(Debug, Clone)]
pub struct Principal {
    tenant_id: String,
    access: Access,
}

#[derive(Debug, Clone)]
enum Access {
    /// The dev switch: every stream of the tenant is readable and writable.
    Anonymous,
    Token(PermissionMatcher),
}

impl Principal {
    /// A principal whose reads are checked against a verified token's
    /// permissions.
    pub fn with_permissions(tenant_id: impl Into<String>, matcher: PermissionMatcher) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            access: Access::Token(matcher),
        }
    }

    /// A principal for an unauthenticated connection, allowed to read and
    /// write every stream of `tenant_id`. Only for the anonymous dev switch.
    pub fn anonymous(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            access: Access::Anonymous,
        }
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Whether this principal may read a stream: the same check as a QUIC
    /// subscribe, `stream.subscribe` on the stream's resource.
    pub fn may_read(&self, namespace: &str, stream: &str) -> bool {
        self.may(Action::StreamSubscribe, namespace, stream)
    }

    /// Whether this principal may write a stream: the same check as a QUIC
    /// publish, `stream.publish` on the stream's resource.
    pub fn may_publish(&self, namespace: &str, stream: &str) -> bool {
        self.may(Action::StreamPublish, namespace, stream)
    }

    fn may(&self, action: Action, namespace: &str, stream: &str) -> bool {
        match &self.access {
            Access::Anonymous => true,
            Access::Token(matcher) => matcher.allows(
                action,
                &stream_resource(
                    &TenantId::new(&self.tenant_id),
                    &Namespace::new(namespace),
                    &StreamName::new(stream),
                ),
            ),
        }
    }
}

/// A Kafka broker id for a Felix node id.
///
/// Kafka names brokers by `i32` and Felix by string, so this hashes (FNV-1a,
/// top bit cleared so it is never negative). Stable across restarts and the
/// same on every broker, which is what a client needs: an id that means the
/// same node whichever broker it asked.
pub fn kafka_node_id(node_id: &str) -> i32 {
    let mut hash: u32 = 0x811c_9dc5;
    for byte in node_id.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    (hash & 0x7fff_ffff) as i32
}

#[cfg(test)]
mod tests;
