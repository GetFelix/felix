//! Multi-tenant real-time notifications demo (SaaS alerts).
//!
//! Demonstrates tenant isolation, fanout subscriptions, cache snapshots, and
//! failure injection for alert-style workloads.
//!
//! # Scenario
//! Two tenants publish alerts to `alerts` streams. Two subscribers per tenant
//! receive fanout. A per-tenant cache stores the last N alerts.
//!
//! # Failure injection
//! Use `--drop-subscriber` to drop one subscriber mid-run and restart it.
use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{FelixTokenIssuer, Jwks, TenantId, TenantKeyCache, TenantKeyMaterial};
use felix_broker::{Broker, CacheMetadata, StreamMetadata};
use felix_broker_service::serving::{auth::BrokerAuth, quic};
use felix_client::{Client, ClientConfig, Publisher, Subscription};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::AckMode;
use jsonwebtoken::Algorithm;
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;

const DEMO_PRIVATE_KEY: [u8; 32] = [42u8; 32];
const LAST_ALERTS_CACHE: &str = "last_alerts";

async fn run_demo(args: DemoArgs) -> Result<()> {
    println!("== Felix Demo: Multi-tenant Real-time Notifications ==");
    println!("Goal: show tenant isolation, fanout, cache snapshots, and failure handling.");

    println!("Step 1/8: booting in-process broker + QUIC server.");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    for tenant in ["t1", "t2"] {
        broker.register_tenant(tenant).await?;
        broker.register_namespace(tenant, "default").await?;
        broker
            .register_stream(tenant, "default", "alerts", StreamMetadata::default())
            .await?;
        broker
            .register_cache(
                tenant,
                "default",
                LAST_ALERTS_CACHE,
                CacheMetadata::default(),
            )
            .await?;
    }

    let config = felix_broker_service::config::BrokerConfig::from_env()?;
    let demo_auth = demo_auth_for_tenants(&["t1", "t2"], Duration::from_secs(900))?;
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

    println!("Step 2/8: connecting tenant clients.");
    let client_t1 = build_client_for_tenant(&cert, addr, "t1", &demo_auth.tokens).await?;
    let client_t2 = build_client_for_tenant(&cert, addr, "t2", &demo_auth.tokens).await?;

    println!("Step 3/8: opening fanout subscriptions.");
    let t1_sub_a = client_t1.subscribe("t1", "default", "alerts").await?;
    let t1_sub_b = client_t1.subscribe("t1", "default", "alerts").await?;
    let t2_sub_a = client_t2.subscribe("t2", "default", "alerts").await?;
    let t2_sub_b = client_t2.subscribe("t2", "default", "alerts").await?;

    println!("Step 4/8: verifying cross-tenant access is denied.");
    let cross = client_t1.subscribe("t2", "default", "alerts").await;
    if cross.is_err() {
        println!("Cross-tenant subscribe blocked as expected.");
    } else {
        println!("Unexpected cross-tenant subscribe success.");
    }

    println!(
        "Step 5/8: publishing alerts + updating cache (last N={}).",
        args.last_n
    );
    let publisher_t1 = client_t1.publisher().await?;
    let publisher_t2 = client_t2.publisher().await?;
    let mut cache_t1 = VecDeque::with_capacity(args.last_n);
    let mut cache_t2 = VecDeque::with_capacity(args.last_n);

    let t1_count = Arc::new(AtomicUsize::new(0));
    let t2_count = Arc::new(AtomicUsize::new(0));
    let t1_sub_a_task = spawn_subscriber("t1/sub-a", t1_sub_a, args.alerts, Arc::clone(&t1_count));
    let t1_sub_b_task = spawn_subscriber("t1/sub-b", t1_sub_b, args.alerts, Arc::clone(&t1_count));
    let t2_sub_a_task = spawn_subscriber("t2/sub-a", t2_sub_a, args.alerts, Arc::clone(&t2_count));
    let t2_sub_b_task = spawn_subscriber("t2/sub-b", t2_sub_b, args.alerts, Arc::clone(&t2_count));

    for idx in 1..=args.alerts {
        let alert_t1 = format!("t1 alert #{idx}");
        let alert_t2 = format!("t2 alert #{idx}");

        publish_alert(&publisher_t1, "t1", &alert_t1).await?;
        update_alert_cache(&client_t1, "t1", &mut cache_t1, &alert_t1, args.last_n).await?;

        publish_alert(&publisher_t2, "t2", &alert_t2).await?;
        update_alert_cache(&client_t2, "t2", &mut cache_t2, &alert_t2, args.last_n).await?;

        if args.drop_subscriber && idx == args.alerts / 2 {
            println!("Failure injection: dropping t1/sub-b mid-run.");
            t1_sub_b_task.abort();
        }
    }

    if args.drop_subscriber {
        println!("Restarting t1/sub-b to show resume behavior.");
        let t1_sub_b_restart = client_t1.subscribe("t1", "default", "alerts").await?;
        let remaining = args.alerts - args.alerts / 2;
        let _restart_task = spawn_subscriber(
            "t1/sub-b-restart",
            t1_sub_b_restart,
            remaining,
            Arc::clone(&t1_count),
        );
    }

    println!("Step 6/8: waiting for subscribers to drain.");
    join_with_timeout("t1/sub-a", t1_sub_a_task).await;
    join_with_timeout("t2/sub-a", t2_sub_a_task).await;
    join_with_timeout("t2/sub-b", t2_sub_b_task).await;

    println!("Step 7/8: reading cache snapshots.");
    print_cache_snapshot(&client_t1, "t1").await?;
    print_cache_snapshot(&client_t2, "t2").await?;

    println!("Step 8/8: summary.");
    println!(
        "Received counts (may be higher due to fanout): t1={} t2={}",
        t1_count.load(Ordering::Relaxed),
        t2_count.load(Ordering::Relaxed)
    );
    println!("Demo complete.");

    server_task.abort();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    run_demo(DemoArgs::from_env()).await
}

struct DemoArgs {
    alerts: usize,
    last_n: usize,
    drop_subscriber: bool,
}

impl DemoArgs {
    fn from_env() -> Self {
        Self::from_args(std::env::args().skip(1))
    }

    fn from_args<I>(args: I) -> Self
    where
        I: Iterator<Item = String>,
    {
        let mut alerts = 10usize;
        let mut last_n = 5usize;
        let mut drop_subscriber = false;
        for arg in args {
            if arg == "--drop-subscriber" {
                drop_subscriber = true;
            } else if let Some(value) = arg.strip_prefix("--alerts=") {
                alerts = value.parse().unwrap_or(alerts);
            } else if let Some(value) = arg.strip_prefix("--last-n=") {
                last_n = value.parse().unwrap_or(last_n);
            }
        }
        Self {
            alerts,
            last_n,
            drop_subscriber,
        }
    }
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

async fn publish_alert(publisher: &Publisher, tenant: &str, message: &str) -> Result<()> {
    publisher
        .publish(
            tenant,
            "default",
            "alerts",
            message.as_bytes().to_vec(),
            AckMode::PerMessage,
        )
        .await?;
    Ok(())
}

async fn update_alert_cache(
    client: &Client,
    tenant: &str,
    cache: &mut VecDeque<String>,
    message: &str,
    last_n: usize,
) -> Result<()> {
    if cache.len() == last_n {
        cache.pop_front();
    }
    cache.push_back(message.to_string());
    let payload = serde_json::to_vec(&cache.iter().collect::<Vec<_>>())?;
    client
        .cache_put(
            tenant,
            "default",
            LAST_ALERTS_CACHE,
            "alerts",
            payload.into(),
            None,
        )
        .await
}

fn spawn_subscriber(
    name: &'static str,
    mut subscription: Subscription,
    expected: usize,
    counter: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut received = 0usize;
        while received < expected {
            match tokio::time::timeout(Duration::from_secs(2), subscription.next_event()).await {
                Ok(Ok(Some(event))) => {
                    received += 1;
                    counter.fetch_add(1, Ordering::Relaxed);
                    println!(
                        "[{name}] event on {}: {}",
                        event.stream,
                        String::from_utf8_lossy(event.payload.as_ref())
                    );
                }
                Ok(Ok(None)) => break,
                Ok(Err(err)) => {
                    println!("[{name}] error: {err}");
                    break;
                }
                Err(_) => {
                    println!("[{name}] timed out waiting for event");
                    break;
                }
            }
        }
    })
}

async fn join_with_timeout(name: &str, handle: JoinHandle<()>) {
    let _ = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .map_err(|_| {
            println!("{name} did not finish before timeout");
        });
}

async fn print_cache_snapshot(client: &Client, tenant: &str) -> Result<()> {
    let snapshot = client
        .cache_get(tenant, "default", LAST_ALERTS_CACHE, "alerts")
        .await?;
    let Some(bytes) = snapshot else {
        println!("[{tenant}] cache snapshot missing");
        return Ok(());
    };
    let alerts: Vec<String> = serde_json::from_slice(&bytes)?;
    println!("[{tenant}] last alerts: {:?}", alerts);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use std::time::Duration;

    #[tokio::test]
    async fn notifications_demo_end_to_end() -> Result<()> {
        let args = DemoArgs {
            alerts: 2,
            last_n: 2,
            drop_subscriber: false,
        };
        tokio::time::timeout(Duration::from_secs(15), run_demo(args))
            .await
            .context("notifications demo timeout")?
    }

    #[tokio::test]
    async fn notifications_demo_with_drop() -> Result<()> {
        let args = DemoArgs {
            alerts: 4,
            last_n: 2,
            drop_subscriber: true,
        };
        tokio::time::timeout(Duration::from_secs(15), run_demo(args))
            .await
            .context("notifications demo drop timeout")?
    }

    #[test]
    fn notifications_demo_args_parse_variants() {
        let parsed = DemoArgs::from_args(
            vec![
                "--alerts=4".to_string(),
                "--last-n=2".to_string(),
                "--drop-subscriber".to_string(),
            ]
            .into_iter(),
        );
        assert_eq!(parsed.alerts, 4);
        assert_eq!(parsed.last_n, 2);
        assert!(parsed.drop_subscriber);

        let parsed = DemoArgs::from_args(
            vec!["--alerts=bad".to_string(), "--last-n=nope".to_string()].into_iter(),
        );
        assert_eq!(parsed.alerts, 10);
        assert_eq!(parsed.last_n, 5);
        assert!(!parsed.drop_subscriber);
    }

    #[tokio::test]
    async fn notifications_demo_join_with_timeout_times_out_cleanly() {
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        join_with_timeout("slow-subscriber", handle).await;
    }
}
