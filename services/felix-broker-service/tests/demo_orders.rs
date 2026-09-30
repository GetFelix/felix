use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

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
use tokio::task::JoinHandle;

const DEMO_PRIVATE_KEY: [u8; 32] = [42u8; 32];
const ORDER_CACHE: &str = "order_state";

#[tokio::test]
async fn orders_demo_smoke() -> Result<()> {
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
        config,
        demo_auth.auth,
    ));

    let client = Arc::new(build_client_for_tenant(&cert, addr, "t1", &demo_auth.tokens).await?);
    let publisher = client.publisher().await?;

    let done = Arc::new(AtomicUsize::new(0));
    let orders_expected = 3usize;
    let orders_worker = spawn_orders_worker(
        Arc::clone(&client),
        publisher.clone(),
        orders_expected,
        Arc::clone(&done),
    )
    .await?;
    let payments_worker = spawn_payments_worker(
        Arc::clone(&client),
        publisher.clone(),
        orders_expected,
        Arc::clone(&done),
    )
    .await?;
    let shipments_worker =
        spawn_shipments_worker(Arc::clone(&client), orders_expected, Arc::clone(&done)).await?;

    for idx in 1..=orders_expected {
        let order_id = format!("order-{idx}");
        let event = OrderEvent::new(&order_id, Stage::OrderCreated, idx as u64);
        publish_event(&publisher, "orders", &event).await?;
    }

    wait_for_completion(&done, orders_expected).await;

    for idx in 1..=orders_expected {
        let order_id = format!("order-{idx}");
        let state = read_order_state(&client, &order_id).await?;
        assert_eq!(state.status, "shipment_prepared");
    }

    orders_worker.abort();
    payments_worker.abort();
    shipments_worker.abort();
    server_task.abort();
    Ok(())
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
        if start.elapsed() > Duration::from_secs(5) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
