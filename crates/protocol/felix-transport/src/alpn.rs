//! A server that negotiates an ALPN protocol with clients that offer one and
//! still accepts clients that offer none.
//!
//! QUIC is strict about ALPN where TLS over TCP is not: rustls refuses a QUIC
//! handshake that ends without a protocol when the server configured any, or
//! when the client offered any (RFC 9001 §8.1). So one rustls config cannot
//! both require `felix/1` of clients that know it and accept the clients that
//! predate it. This picks between two configs per connection, by reading the
//! ALPN extension out of the ClientHello before rustls sees it: a hello that
//! offers protocols goes to the config that lists ours, one that offers none
//! goes to a config that lists none. Everything after that choice is rustls.

use std::any::Any;
use std::sync::Arc;

use anyhow::{Context, Result};
use quinn::crypto::rustls::QuicServerConfig;
use quinn::crypto::{
    self, ExportKeyingMaterialError, KeyPair, Keys, PacketKey, UnsupportedVersion,
};
use quinn_proto::transport_parameters::TransportParameters;
use quinn_proto::{ConnectionId, Side, TransportError};

/// Build a server config that selects one of `protocols` for a client that
/// offers any of them, refuses a client that offers only others, and accepts
/// a client that offers none.
///
/// `tls` must not list protocols of its own; they come from `protocols`.
pub fn alpn_optional_server_config(
    tls: rustls::ServerConfig,
    protocols: &[&[u8]],
) -> Result<quinn::ServerConfig> {
    let mut with = tls.clone();
    with.alpn_protocols = protocols.iter().map(|protocol| protocol.to_vec()).collect();
    let mut without = tls;
    without.alpn_protocols.clear();
    let crypto = AlpnOptional {
        with: Arc::new(QuicServerConfig::try_from(with).context("QUIC TLS config with ALPN")?),
        without: Arc::new(
            QuicServerConfig::try_from(without).context("QUIC TLS config without ALPN")?,
        ),
    };
    Ok(quinn::ServerConfig::with_crypto(Arc::new(crypto)))
}

/// The two configs a connection is started on, chosen by its ClientHello.
struct AlpnOptional {
    with: Arc<QuicServerConfig>,
    without: Arc<QuicServerConfig>,
}

impl crypto::ServerConfig for AlpnOptional {
    fn initial_keys(
        &self,
        version: u32,
        dst_cid: &ConnectionId,
    ) -> std::result::Result<Keys, UnsupportedVersion> {
        // Initial keys depend on the version and the connection id only, so
        // either config gives the same ones.
        self.with.initial_keys(version, dst_cid)
    }

    fn retry_tag(&self, version: u32, orig_dst_cid: &ConnectionId, packet: &[u8]) -> [u8; 16] {
        self.with.retry_tag(version, orig_dst_cid, packet)
    }

    fn start_session(
        self: Arc<Self>,
        version: u32,
        params: &TransportParameters,
    ) -> Box<dyn crypto::Session> {
        Box::new(Deferred {
            config: self,
            version,
            params: *params,
            hello: Vec::new(),
            inner: None,
        })
    }
}

/// A session that holds the ClientHello until it has all of it, then hands it
/// to the rustls session for the config the hello asked for.
struct Deferred {
    config: Arc<AlpnOptional>,
    version: u32,
    params: TransportParameters,
    hello: Vec<u8>,
    inner: Option<Box<dyn crypto::Session>>,
}

impl crypto::Session for Deferred {
    fn initial_keys(&self, dst_cid: &ConnectionId, side: Side) -> Keys {
        match &self.inner {
            Some(inner) => inner.initial_keys(dst_cid, side),
            // start_session only runs for a version initial_keys accepted.
            None => crypto::ServerConfig::initial_keys(&*self.config.with, self.version, dst_cid)
                .expect("the version was accepted"),
        }
    }

    fn handshake_data(&self) -> Option<Box<dyn Any>> {
        self.inner.as_ref()?.handshake_data()
    }

    fn peer_identity(&self) -> Option<Box<dyn Any>> {
        self.inner.as_ref()?.peer_identity()
    }

    fn early_crypto(&self) -> Option<(Box<dyn crypto::HeaderKey>, Box<dyn PacketKey>)> {
        self.inner.as_ref()?.early_crypto()
    }

    fn early_data_accepted(&self) -> Option<bool> {
        self.inner.as_ref()?.early_data_accepted()
    }

    fn is_handshaking(&self) -> bool {
        self.inner
            .as_ref()
            .is_none_or(|inner| inner.is_handshaking())
    }

    fn read_handshake(&mut self, buf: &[u8]) -> std::result::Result<bool, TransportError> {
        if let Some(inner) = &mut self.inner {
            return inner.read_handshake(buf);
        }
        self.hello.extend_from_slice(buf);
        let offers = match client_hello_offers_alpn(&self.hello) {
            Hello::Incomplete => return Ok(false),
            Hello::Offers(offers) => offers,
            // Not a hello this can read: let rustls say what is wrong with it.
            Hello::Unreadable => true,
        };
        let config = if offers {
            Arc::clone(&self.config.with)
        } else {
            Arc::clone(&self.config.without)
        };
        let mut inner = crypto::ServerConfig::start_session(config, self.version, &self.params);
        let hello = std::mem::take(&mut self.hello);
        let result = inner.read_handshake(&hello);
        self.inner = Some(inner);
        result
    }

    fn transport_parameters(
        &self,
    ) -> std::result::Result<Option<TransportParameters>, TransportError> {
        match &self.inner {
            Some(inner) => inner.transport_parameters(),
            None => Ok(None),
        }
    }

    fn write_handshake(&mut self, buf: &mut Vec<u8>) -> Option<Keys> {
        self.inner.as_mut()?.write_handshake(buf)
    }

    fn next_1rtt_keys(&mut self) -> Option<KeyPair<Box<dyn PacketKey>>> {
        self.inner.as_mut()?.next_1rtt_keys()
    }

    fn is_valid_retry(&self, orig_dst_cid: &ConnectionId, header: &[u8], payload: &[u8]) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|inner| inner.is_valid_retry(orig_dst_cid, header, payload))
    }

    fn export_keying_material(
        &self,
        output: &mut [u8],
        label: &[u8],
        context: &[u8],
    ) -> std::result::Result<(), ExportKeyingMaterialError> {
        match &self.inner {
            Some(inner) => inner.export_keying_material(output, label, context),
            None => Err(ExportKeyingMaterialError),
        }
    }
}

/// What the start of a CRYPTO stream says about ALPN.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Hello {
    /// More bytes are needed before the ClientHello is whole.
    Incomplete,
    /// A whole ClientHello; `true` if it carries an ALPN extension.
    Offers(bool),
    /// Not a ClientHello this parser can walk.
    Unreadable,
}

/// Find the ALPN extension (type 16) in a TLS 1.3 ClientHello handshake
/// message (RFC 8446 §4.1.2).
pub(crate) fn client_hello_offers_alpn(bytes: &[u8]) -> Hello {
    const CLIENT_HELLO: u8 = 1;
    const ALPN: u16 = 16;
    if bytes.len() < 4 {
        return Hello::Incomplete;
    }
    if bytes[0] != CLIENT_HELLO {
        return Hello::Unreadable;
    }
    let len = u32::from_be_bytes([0, bytes[1], bytes[2], bytes[3]]) as usize;
    let Some(body) = bytes.get(4..4 + len) else {
        return Hello::Incomplete;
    };
    match hello_has_extension(body, ALPN) {
        Some(offers) => Hello::Offers(offers),
        None => Hello::Unreadable,
    }
}

fn hello_has_extension(body: &[u8], wanted: u16) -> Option<bool> {
    let mut reader = Reader(body);
    reader.skip(2 + 32)?; // legacy_version, random
    let session_id = reader.u8()? as usize;
    reader.skip(session_id)?;
    let suites = reader.u16()? as usize;
    reader.skip(suites)?;
    let compression = reader.u8()? as usize;
    reader.skip(compression)?;
    let extensions = reader.u16()? as usize;
    let mut extensions = Reader(reader.take(extensions)?);
    while !extensions.0.is_empty() {
        let kind = extensions.u16()?;
        let len = extensions.u16()? as usize;
        extensions.skip(len)?;
        if kind == wanted {
            return Some(true);
        }
    }
    Some(false)
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        if self.0.len() < len {
            return None;
        }
        let (head, rest) = self.0.split_at(len);
        self.0 = rest;
        Some(head)
    }

    fn skip(&mut self, len: usize) -> Option<()> {
        self.take(len).map(|_| ())
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|bytes| bytes[0])
    }

    fn u16(&mut self) -> Option<u16> {
        self.take(2)
            .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]))
    }
}

#[cfg(test)]
mod tests;
