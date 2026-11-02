//! MQTT over WebSockets.
//!
//! [`accept`] reads the HTTP upgrade from a connection and, if it is a valid
//! request for the `mqtt` subprotocol from an allowed origin, returns a
//! [`WsStream`]: the connection as a plain byte stream, which
//! [`MqttService::serve_connection`](crate::MqttService::serve_connection)
//! serves like any other. TLS is the caller's, below this layer, so `ws` and
//! `wss` differ only in what the stream was before it got here.

mod frame;
mod handshake;
mod stream;
#[cfg(test)]
mod tests;

use tokio::io::{AsyncRead, AsyncWrite};

pub use frame::{Event, FrameDecoder, Violation};
pub use handshake::{AllowedOrigins, Refusal, Settings, parse};
pub use stream::WsStream;

/// Upgrade `stream` to a WebSocket carrying MQTT.
pub async fn accept<S>(mut stream: S, settings: &Settings) -> Result<WsStream<S>, Refusal>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let rest = handshake::accept(&mut stream, settings).await?;
    Ok(WsStream::new(stream, rest))
}
