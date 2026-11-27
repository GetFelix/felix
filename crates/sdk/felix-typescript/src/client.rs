//! `Client`, the connected cluster client every other handle comes from.

mod cache;
mod group;
mod publish;
mod stream;

use std::sync::Arc;

use felix_client::ClientConfig;
use napi::JsFunction;
use napi::JsObject;
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::args::{reconnect_policy, seeds};
use crate::errors::{classify, invalid};
use crate::stream_read::ConnectionCount;
use crate::tls;
use crate::token;
use crate::tuning::ClientTuning;

/// A connected Felix client.
#[napi]
pub struct Client {
    /// `None` once closed. A plain `std::sync::Mutex` and not the async one:
    /// the lock is only ever held long enough to clone the `Arc`, so callers
    /// sharing a client never queue behind each other's in-flight calls.
    inner: std::sync::Mutex<Option<Arc<felix_client::Client>>>,
}

impl Client {
    /// The cluster client, or an error if this one has been closed.
    pub(crate) fn cluster(&self) -> Result<Arc<felix_client::Client>> {
        self.inner
            .lock()
            .expect("the client lock is only ever held across a clone")
            .clone()
            .ok_or_else(|| invalid("this client is closed"))
    }
}

#[napi]
#[napi]
impl Client {
    /// Connect to a cluster.
    ///
    /// `addrs` is one `"host:port"` or several; any reachable one is enough
    /// and the client discovers the rest. A name is resolved again whenever
    /// no broker the client knows answers, so prefer one to an IP.
    /// `serverName` is the name the broker's certificate is expected to
    /// carry.
    ///
    /// TLS is not optional — QUIC has no unencrypted mode. Supply `caFile` to
    /// trust a specific CA (what a self-signed development broker needs); omit
    /// it to use the operating system's trust store.
    ///
    /// `offerAlpn` offers the `felix/1` ALPN. A broker with
    /// `FELIX_TLS_REQUIRE_ALPN=true` serves only clients that do; a broker
    /// older than ALPN support refuses them, which is why it is off by default.
    ///
    /// Pass `token`, a `tokenProvider`, or both. `tokenProvider` is called on
    /// the JS thread, lazily and one call at a time, and `token` is what the
    /// client starts with.
    ///
    /// `clientCertFile` and `clientKeyFile` are PEM files holding a client
    /// certificate (leaf first, then any intermediates) and its private key.
    /// Give both to connect to a broker started with `FELIX_TLS_CLIENT_CA`,
    /// which refuses a client without one. Either alone, an unreadable or
    /// non-PEM file, or a key that does not match is an `InvalidArg` error.
    ///
    /// `reconnectAttempts` (default 5, a whole number of at least 1) is how
    /// many times a call tries to reach a broker before it gives up.
    /// `reconnectDeadline` (milliseconds, default none) bounds one call across
    /// every attempt and wait. A deadline shorter than one attempt's own
    /// timeout leaves the call a single attempt. `reconnectBackoff` (default
    /// 200) is the ceiling of the first wait in milliseconds, and it doubles
    /// each attempt up to `reconnectMaxBackoff` (default 2000), which must not
    /// be below it. Each wait is a random fraction of its ceiling. All apply to
    /// `withIdentity` clients too.
    #[napi(
        ts_args_type = "addrs: string | string[], tenantId: string, token?: string | null, serverName?: string, caFile?: string, offerAlpn?: boolean, tokenProvider?: (signal: AbortSignal) => Promise<string>, reconnectAttempts?: number, reconnectDeadline?: number, reconnectBackoff?: number, reconnectMaxBackoff?: number, clientCertFile?: string, clientKeyFile?: string, tuning?: ClientTuning",
        ts_return_type = "Promise<Client>"
    )]
    pub fn connect(
        env: Env,
        addrs: Either<String, Vec<String>>,
        tenant_id: String,
        token: Option<String>,
        server_name: Option<String>,
        ca_file: Option<String>,
        offer_alpn: Option<bool>,
        token_provider: Option<JsFunction>,
        reconnect_attempts: Option<f64>,
        reconnect_deadline: Option<f64>,
        reconnect_backoff: Option<f64>,
        reconnect_max_backoff: Option<f64>,
        client_cert_file: Option<String>,
        client_key_file: Option<String>,
        tuning: Option<ClientTuning>,
    ) -> Result<JsObject> {
        let policy = reconnect_policy(
            reconnect_attempts,
            reconnect_deadline,
            reconnect_backoff,
            reconnect_max_backoff,
        )?;
        let addrs = match addrs {
            Either::A(one) => vec![one],
            Either::B(many) => many,
        };
        let seeds = seeds(addrs)?;
        if seeds.is_empty() {
            return Err(invalid("at least one broker address is required"));
        }
        let quinn = tls::client_config(
            ca_file.as_deref(),
            client_cert_file.as_deref(),
            client_key_file.as_deref(),
            offer_alpn.unwrap_or(false),
        )?;
        let mut config = ClientConfig::optimized_defaults(quinn);
        if let Some(tuning) = tuning {
            tuning.apply(&mut config)?;
        }
        config.auth_tenant_id = Some(tenant_id);
        match token_provider {
            Some(callback) => {
                let fetch = token::fetch_from(&env, &callback)?;
                config.token_provider = Some(token::provider(&fetch, token));
            }
            None => {
                config.auth_token = Some(
                    token
                        .filter(|token| !token.is_empty())
                        .ok_or_else(|| invalid("a token or a tokenProvider is required"))?,
                );
            }
        }

        let server_name = server_name.unwrap_or_else(|| "localhost".to_string());
        env.execute_tokio_future(
            async move {
                let inner =
                    felix_client::Client::connect_seeds(&seeds, &server_name, config, policy)
                        .await
                        .map_err(classify)?;
                Ok(inner)
            },
            |_, inner| {
                Ok(Client {
                    inner: std::sync::Mutex::new(Some(Arc::new(inner))),
                })
            },
        )
    }

    /// A client that acts as another user over this client's connections.
    ///
    /// Every stream it opens authenticates with `tenantId` and a token: the
    /// fixed `token`, or what `tokenProvider` returns (called as for
    /// `connect`; `token` then only seeds it). The broker checks its requests
    /// against that token's grants alone. It follows shards and failovers as
    /// this client does, reaching each broker over this client's connection to
    /// it. A refused token fails here.
    #[napi(
        ts_args_type = "tenantId: string, token?: string | null, tokenProvider?: (signal: AbortSignal) => Promise<string>",
        ts_return_type = "Promise<Client>"
    )]
    pub fn with_identity(
        &self,
        env: Env,
        tenant_id: String,
        token: Option<String>,
        token_provider: Option<JsFunction>,
    ) -> Result<JsObject> {
        let cluster = self.cluster()?;
        let tokens = match token_provider {
            Some(callback) => {
                let fetch = token::fetch_from(&env, &callback)?;
                Ok(token::provider(&fetch, token))
            }
            None => Err(token
                .filter(|token| !token.is_empty())
                .ok_or_else(|| invalid("a token or a tokenProvider is required"))?),
        };
        env.execute_tokio_future(
            async move {
                match tokens {
                    Ok(provider) => cluster.with_identity(tenant_id, provider).await,
                    Err(fixed) => cluster.with_identity_token(tenant_id, fixed).await,
                }
                .map_err(classify)
            },
            |_, identity| {
                Ok(Client {
                    inner: std::sync::Mutex::new(Some(Arc::new(identity))),
                })
            },
        )
    }

    /// Release the client. Idempotent.
    ///
    /// Later calls fail rather than quietly using a connection that was meant
    /// to be gone. Subscriptions already handed out hold their own connection
    /// and keep running — closing the client is not a way to stop them, and
    /// `close` on the subscription is.
    #[napi]
    pub fn close(&self) {
        self.inner
            .lock()
            .expect("the client lock is only ever held across a clone")
            .take();
    }

    #[napi(getter)]
    pub fn closed(&self) -> bool {
        self.inner
            .lock()
            .expect("the client lock is only ever held across a clone")
            .is_none()
    }

    /// Every broker this client would try, seeds included.
    #[napi]
    pub async fn endpoints(&self) -> Result<Vec<String>> {
        Ok(self
            .cluster()?
            .endpoints()
            .await
            .into_iter()
            .map(|addr| addr.to_string())
            .collect())
    }

    /// Ask the broker in use which brokers a client may connect to, and add
    /// them to the endpoints this client will try. Resolves with how many the
    /// cluster named, which may be zero. Rejects when the broker predates
    /// discovery.
    #[napi]
    pub async fn refresh_topology(&self) -> Result<u32> {
        let named = self.cluster()?.refresh_topology().await.map_err(classify)?;
        Ok(named as u32)
    }

    /// Live connections to each broker this client holds one for, as
    /// `[address, count]` pairs. A broker starts at one connection and gains
    /// more only when its streams are saturated.
    #[napi]
    pub async fn connections_per_node(&self) -> Result<Vec<ConnectionCount>> {
        Ok(self
            .cluster()?
            .connections_per_node()
            .await
            .into_iter()
            .map(|(addr, connections)| ConnectionCount {
                addr: addr.to_string(),
                connections: connections as u32,
            })
            .collect())
    }
}
