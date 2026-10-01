//! Orders/payments pipeline demo (pub/sub + cache last-known state).
//!
//! Demonstrates a multi-stage pipeline with idempotent workers, cache-backed
//! last-known state, and failure injection.
//!
//! # Failure injection
//! Use `--kill-worker=payments` to stop and restart the payments worker mid-run.
use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{FelixTokenIssuer, Jwks, TenantId, TenantKeyCache, TenantKeyMaterial};
use felix_broker::{Broker, CacheMetadata, StreamMetadata};
use felix_broker_service::serving::{auth::BrokerAuth, quic};
use felix_client::{Client, ClientConfig, Publisher};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::AckMode;
use jsonwebtoken::Algorithm;
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;

const DEMO_PRIVATE_KEY: [u8; 32] = [42u8; 32];
const ORDER_CACHE: &str = "order_state";
const ORDER_INDEX: &str = "order_index";

async fn run_demo(args: DemoArgs) -> Result<()> {
    println!("== Felix Demo: Orders/Payments Pipeline ==");
    println!("Goal: show pub/sub pipeline + cache last-known state + idempotency.");

    println!("Step 1/9: booting in-process broker + QUIC server.");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await?;
    broker
        .register_stream("t1", "default", "payments", StreamMetadata::default())
        .await?;
    broker
        .register_stream("t1", "default", "shipments", StreamMetadata::default())
        .await?;
    broker
        .register_cache("t1", "default", ORDER_CACHE, CacheMetadata::default())
        .await?;

    let config = felix_broker_service::config::BrokerConfig::from_env()?;
    let demo_auth = demo_auth_for_tenants(&["t1"], Duration::from_secs(900))?;
    let (server_config, cert) = build_server_config().context("build server config")?;
    let transport = TransportConfig::default();
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        transport,
    )?);
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(quic::serve(
        Arc::clone(&server),
        broker,
        config.clone(),
        demo_auth.auth,
    ));

    println!("Step 2/9: connecting pipeline client.");
    let client = Arc::new(build_client_for_tenant(&cert, addr, "t1", &demo_auth.tokens).await?);
    let publisher = client.publisher().await?;

    println!("Step 3/9: starting workers.");
    let shipments_done = Arc::new(AtomicUsize::new(0));
    let orders_expected = args.orders;
    let orders_worker = spawn_orders_worker(
        Arc::clone(&client),
        publisher.clone(),
        orders_expected,
        Arc::clone(&shipments_done),
    )
    .await?;
    let mut payments_worker = spawn_payments_worker(
        Arc::clone(&client),
        publisher.clone(),
        orders_expected,
        Arc::clone(&shipments_done),
    )
    .await?;
    let shipments_worker = spawn_shipments_worker(
        Arc::clone(&client),
        orders_expected,
        Arc::clone(&shipments_done),
    )
    .await?;

    println!("Step 4/9: publishing orders (with intentional duplicates).");
    let order_ids: Vec<String> = (1..=args.orders)
        .map(|idx| format!("order-{idx:02}"))
        .collect();
    update_order_index(&client, &order_ids).await?;

    for (idx, order_id) in order_ids.iter().enumerate() {
        let event = OrderEvent::new(order_id, Stage::OrderCreated, idx as u64);
        publish_event(&publisher, "orders", &event).await?;

        if args.duplicate_every > 0 && (idx + 1) % args.duplicate_every == 0 {
            println!("Injecting duplicate for {}", order_id);
            publish_event(&publisher, "orders", &event).await?;
        }

        if args.kill_payments && idx + 1 == args.orders / 2 {
            println!("Failure injection: stopping payments worker.");
            payments_worker.abort();
            tokio::time::sleep(Duration::from_millis(300)).await;
            println!("Restarting payments worker.");
            payments_worker = spawn_payments_worker(
                Arc::clone(&client),
                publisher.clone(),
                orders_expected,
                Arc::clone(&shipments_done),
            )
            .await?;
        }
    }

    println!("Step 5/9: waiting for pipeline completion.");
    wait_for_completion(&shipments_done, orders_expected).await;

    println!("Step 6/9: reading cache snapshots.");
    for order_id in &order_ids {
        let state = read_order_state(&client, order_id).await?;
        println!("{order_id} -> {}", state.status);
    }

    println!("Step 7/9: shutting down workers.");
    orders_worker.abort();
    payments_worker.abort();
    shipments_worker.abort();

    println!("Step 8/9: demo summary.");
    println!(
        "Orders processed: {} (expected {})",
        shipments_done.load(Ordering::Relaxed),
        orders_expected
    );
    println!("Step 9/9: complete.");

    server_task.abort();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    run_demo(DemoArgs::from_env()).await
}

#[derive(Debug)]
struct DemoArgs {
    orders: usize,
    duplicate_every: usize,
    kill_payments: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use std::time::Duration;

    #[tokio::test]
    async fn orders_demo_end_to_end() -> Result<()> {
        let args = DemoArgs {
            orders: 3,
            duplicate_every: 2,
            kill_payments: false,
        };
        tokio::time::timeout(Duration::from_secs(15), run_demo(args))
            .await
            .context("orders demo timeout")?
    }

    #[tokio::test]
    async fn orders_demo_kill_payments() -> Result<()> {
        let args = DemoArgs {
            orders: 4,
            duplicate_every: 2,
            kill_payments: true,
        };
        tokio::time::timeout(Duration::from_secs(15), run_demo(args))
            .await
            .context("orders demo kill payments timeout")?
    }

    #[test]
    fn orders_demo_args_parse_variants() {
        let parsed = DemoArgs::from_args(
            vec![
                "--orders=7".to_string(),
                "--duplicate-every=3".to_string(),
                "--kill-worker=payments".to_string(),
            ]
            .into_iter(),
        );
        assert_eq!(parsed.orders, 7);
        assert_eq!(parsed.duplicate_every, 3);
        assert!(parsed.kill_payments);

        let parsed = DemoArgs::from_args(
            vec!["--orders=x".to_string(), "--duplicate-every=y".to_string()].into_iter(),
        );
        assert_eq!(parsed.orders, 12);
        assert_eq!(parsed.duplicate_every, 5);
        assert!(!parsed.kill_payments);
    }

    #[tokio::test]
    async fn orders_demo_wait_for_completion_timeout_path() {
        let done = Arc::new(AtomicUsize::new(0));
        wait_for_completion(&done, 1).await;
        assert_eq!(done.load(Ordering::Relaxed), 0);
    }
}

impl DemoArgs {
    fn from_env() -> Self {
        Self::from_args(std::env::args().skip(1))
    }

    fn from_args<I>(args: I) -> Self
    where
        I: Iterator<Item = String>,
    {
        let mut orders = 12usize;
        let mut duplicate_every = 5usize;
        let mut kill_payments = false;
        for arg in args {
            if let Some(value) = arg.strip_prefix("--orders=") {
                orders = value.parse().unwrap_or(orders);
            } else if let Some(value) = arg.strip_prefix("--duplicate-every=") {
                duplicate_every = value.parse().unwrap_or(duplicate_every);
            } else if arg == "--kill-worker=payments" {
                kill_payments = true;
            }
        }
        Self {
            orders,
            duplicate_every,
            kill_payments,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderEvent {
    event_id: String,
    order_id: String,
    stage: Stage,
}

impl OrderEvent {
    fn new(order_id: &str, stage: Stage, seq: u64) -> Self {
        Self {
            event_id: format!("{order_id}:{stage}:{seq}"),
            order_id: order_id.to_string(),
            stage,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Stage {
    OrderCreated,
    PaymentCaptured,
    ShipmentPrepared,
}

impl std::fmt::Display for Stage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Stage::OrderCreated => write!(f, "order_created"),
            Stage::PaymentCaptured => write!(f, "payment_captured"),
            Stage::ShipmentPrepared => write!(f, "shipment_prepared"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderState {
    status: String,
    last_event_id: String,
}

struct DemoAuthBundle {
    auth: Arc<BrokerAuth>,
    tokens: HashMap<String, String>,
}

fn demo_auth_for_tenants(tenants: &[&str], ttl: Duration) -> Result<DemoAuthBundle> {
    let mut key_store: HashMap<String, TenantKeyMaterial> = HashMap::new();
    let mut tokens = HashMap::new();
    let mut jwks_per_tenant = HashMap::new();

    for tenant in tenants {
        let signing_key = Ed25519SigningKey::from_bytes(&DEMO_PRIVATE_KEY);
        let public_key = signing_key.verifying_key().to_bytes();
        let jwks = build_demo_jwks("demo-k1", &public_key)?;
        let key_material = TenantKeyMaterial {
            kid: "demo-k1".to_string(),
            alg: Algorithm::EdDSA,
            private_key: DEMO_PRIVATE_KEY,
            public_key,
            jwks: jwks.clone(),
        };
        key_store.insert((*tenant).to_string(), key_material);
        jwks_per_tenant.insert((*tenant).to_string(), jwks);
    }

    let issuer = FelixTokenIssuer::new("felix-auth", "felix-broker", ttl, Arc::new(key_store));
    for tenant in tenants {
        let perms = vec![
            format!("tenant.manage:tenant:{tenant}"),
            format!("ns.manage:namespace:{tenant}/*"),
            format!("stream.publish:stream:{tenant}/*/*"),
            format!("stream.subscribe:stream:{tenant}/*/*"),
            format!("cache.read:cache:{tenant}/*/*"),
            format!("cache.write:cache:{tenant}/*/*"),
        ];
        let token = issuer.mint(&TenantId::new(*tenant), "p:demo", perms)?;
        tokens.insert((*tenant).to_string(), token);
    }

    let key_store = Arc::new(
        felix_broker_service::serving::auth::ControlPlaneKeyStore::new(
            "http://127.0.0.1".to_string(),
            Arc::new(TenantKeyCache::default()),
        ),
    );
    for (tenant, jwks) in jwks_per_tenant {
        key_store.insert_jwks(&TenantId::new(&tenant), jwks);
    }
    let auth = Arc::new(BrokerAuth::with_key_store(key_store));

    Ok(DemoAuthBundle { auth, tokens })
}

fn build_demo_jwks(kid: &str, public_key: &[u8; 32]) -> Result<Jwks> {
    let x = URL_SAFE_NO_PAD.encode(public_key);
    Ok(Jwks {
        keys: vec![felix_authz::Jwk {
            kty: "OKP".to_string(),
            kid: kid.to_string(),
            alg: "EdDSA".to_string(),
            use_field: felix_authz::KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(x),
        }],
    })
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
            .context("build server config")?;
    Ok((server_config, cert_der))
}

fn build_client_config(cert: CertificateDer<'static>) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    let quinn = QuinnClientConfig::with_root_certificates(Arc::new(roots))?;
    ClientConfig::from_env_or_yaml(quinn, None)
}

async fn build_client_for_tenant(
    cert: &CertificateDer<'static>,
    addr: std::net::SocketAddr,
    tenant: &str,
    tokens: &HashMap<String, String>,
) -> Result<Client> {
    let mut config = build_client_config(cert.clone())?;
    config.auth_tenant_id = Some(tenant.to_string());
    config.auth_token = tokens.get(tenant).cloned();
    Client::connect(addr, "localhost", config).await
}

async fn publish_event(publisher: &Publisher, stream: &str, event: &OrderEvent) -> Result<()> {
    let payload = serde_json::to_vec(event)?;
    publisher
        .publish("t1", "default", stream, payload, AckMode::PerMessage)
        .await?;
    Ok(())
}

async fn spawn_orders_worker(
    client: Arc<Client>,
    publisher: Publisher,
    expected: usize,
    done: Arc<AtomicUsize>,
) -> Result<JoinHandle<()>> {
    let mut seen = HashSet::new();
    let mut subscription = client.subscribe("t1", "default", "orders").await?;
    Ok(tokio::spawn(async move {
        while seen.len() < expected {
            match tokio::time::timeout(Duration::from_secs(2), subscription.next_event()).await {
                Ok(Ok(Some(event))) => {
                    if let Ok(order_event) = serde_json::from_slice::<OrderEvent>(&event.payload) {
                        if !seen.insert(order_event.event_id.clone()) {
                            println!("orders worker dedupe: {}", order_event.event_id);
                            continue;
                        }
                        let _ =
                            update_order_state(&client, &order_event.order_id, &order_event.stage)
                                .await;
                        let next =
                            OrderEvent::new(&order_event.order_id, Stage::PaymentCaptured, 0);
                        let _ = publish_event(&publisher, "payments", &next).await;
                    }
                }
                _ => break,
            }
        }
        done.fetch_add(0, Ordering::Relaxed);
    }))
}

async fn spawn_payments_worker(
    client: Arc<Client>,
    publisher: Publisher,
    expected: usize,
    done: Arc<AtomicUsize>,
) -> Result<JoinHandle<()>> {
    let mut seen = HashSet::new();
    let mut subscription = client.subscribe("t1", "default", "payments").await?;
    Ok(tokio::spawn(async move {
        while seen.len() < expected {
            match tokio::time::timeout(Duration::from_secs(2), subscription.next_event()).await {
                Ok(Ok(Some(event))) => {
                    if let Ok(order_event) = serde_json::from_slice::<OrderEvent>(&event.payload) {
                        if !seen.insert(order_event.event_id.clone()) {
                            println!("payments worker dedupe: {}", order_event.event_id);
                            continue;
                        }
                        let _ =
                            update_order_state(&client, &order_event.order_id, &order_event.stage)
                                .await;
                        let next =
                            OrderEvent::new(&order_event.order_id, Stage::ShipmentPrepared, 0);
                        let _ = publish_event(&publisher, "shipments", &next).await;
                    }
                }
                _ => break,
            }
        }
        done.fetch_add(0, Ordering::Relaxed);
    }))
}

async fn spawn_shipments_worker(
    client: Arc<Client>,
    expected: usize,
    done: Arc<AtomicUsize>,
) -> Result<JoinHandle<()>> {
    let mut seen = HashSet::new();
    let mut subscription = client.subscribe("t1", "default", "shipments").await?;
    Ok(tokio::spawn(async move {
        while seen.len() < expected {
            match tokio::time::timeout(Duration::from_secs(2), subscription.next_event()).await {
                Ok(Ok(Some(event))) => {
                    if let Ok(order_event) = serde_json::from_slice::<OrderEvent>(&event.payload) {
                        if !seen.insert(order_event.event_id.clone()) {
                            println!("shipments worker dedupe: {}", order_event.event_id);
                            continue;
                        }
                        let _ =
                            update_order_state(&client, &order_event.order_id, &order_event.stage)
                                .await;
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                }
                _ => break,
            }
        }
    }))
}

async fn update_order_index(client: &Client, order_ids: &[String]) -> Result<()> {
    let payload = serde_json::to_vec(order_ids)?;
    client
        .cache_put(
            "t1",
            "default",
            ORDER_CACHE,
            ORDER_INDEX,
            payload.into(),
            None,
        )
        .await
}

async fn update_order_state(client: &Client, order_id: &str, stage: &Stage) -> Result<()> {
    let state = OrderState {
        status: stage.to_string(),
        last_event_id: format!("{order_id}:{stage}"),
    };
    let payload = serde_json::to_vec(&state)?;
    client
        .cache_put("t1", "default", ORDER_CACHE, order_id, payload.into(), None)
        .await
}

async fn read_order_state(client: &Client, order_id: &str) -> Result<OrderState> {
    let bytes = client
        .cache_get("t1", "default", ORDER_CACHE, order_id)
        .await?
        .context("missing order state")?;
    Ok(serde_json::from_slice(&bytes)?)
}

async fn wait_for_completion(done: &Arc<AtomicUsize>, expected: usize) {
    let start = std::time::Instant::now();
    loop {
        let current = done.load(Ordering::Relaxed);
        if current >= expected {
            break;
        }
        if start.elapsed() > Duration::from_secs(10) {
            println!("Timed out waiting for shipments: {current}/{expected}");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
