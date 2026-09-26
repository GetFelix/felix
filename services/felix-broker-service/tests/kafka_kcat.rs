//! The Kafka listener against a real Kafka client: `kcat` (librdkafka 1.8).
//!
//! kcat runs from the `edenhill/kcat:1.7.1` image, so these tests need Docker.
//! Without it each test says so and returns; CI has Docker. On Linux the
//! container shares the host network; elsewhere (Docker Desktop) it reaches
//! the host as `host.docker.internal`, which is also what the listener
//! advertises in Metadata.
//!
//! Run with `cargo test -p felix-broker-service --test kafka_kcat`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::process::Output;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Bytes;
use ed25519_dalek::SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
};
use felix_broker::{Broker, DurableStorage, StreamMetadata};
use felix_broker_service::config::KafkaListenerConfig;
use felix_broker_service::serving::auth::{BrokerAuth, ControlPlaneKeyStore};
use felix_broker_service::serving::kafka::{BrokerCluster, KafkaListener, tls_config};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig, LogRecord, RecordMark};
use jsonwebtoken::Algorithm;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use tempfile::TempDir;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const KCAT_IMAGE: &str = "edenhill/kcat:1.7.1";
const TENANT: &str = "t1";
const TOPIC: &str = "orders.created";
const SHARDS: u32 = 3;
/// Generous: the image may run under emulation, and each kcat is a container.
const KCAT_BOUND: Duration = Duration::from_secs(60);

struct Felix {
    broker: Arc<Broker>,
    token: String,
    /// `host:port` a kcat container dials.
    plain: String,
    tls: String,
    anonymous: String,
    certs: TempDir,
    _data: TempDir,
    shutdown: CancellationToken,
}

impl Drop for Felix {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl Felix {
    async fn publish(&self, shard: u32, values: &[&str]) {
        let payloads: Vec<Bytes> = values
            .iter()
            .map(|v| Bytes::copy_from_slice(v.as_bytes()))
            .collect();
        self.broker
            .publish_batch(TENANT, "orders", "created", shard, &payloads)
            .await
            .expect("publish");
    }

    /// Every record of one shard, from the broker's log.
    async fn log_records(&self, shard: u32) -> Vec<LogRecord> {
        let handle = self
            .broker
            .resolve_stream_handle(TENANT, "orders", "created", shard)
            .await
            .expect("handle");
        handle
            .log()
            .expect("durable")
            .read_from(0, usize::MAX)
            .await
            .expect("read")
    }

    /// kcat against the SASL/PLAIN listener, with this tenant's token.
    async fn kcat(&self, args: &[&str]) -> Output {
        self.kcat_with_input(args, "").await
    }

    /// [`Self::kcat`], with `input` on kcat's stdin: what `-P` produces.
    async fn kcat_with_input(&self, args: &[&str], input: &str) -> Output {
        let mut all = vec![
            "-b",
            self.plain.as_str(),
            "-X",
            "security.protocol=SASL_PLAINTEXT",
            "-X",
            "sasl.mechanisms=PLAIN",
            "-X",
            "sasl.username=t1",
        ];
        let password = format!("sasl.password={}", self.token);
        all.extend(["-X", password.as_str()]);
        all.extend_from_slice(args);
        kcat_fed(&all, None, input).await
    }
}

fn docker_host() -> &'static str {
    if cfg!(target_os = "linux") {
        "127.0.0.1"
    } else {
        "host.docker.internal"
    }
}

/// Whether Docker can run the kcat image. Pulls it if missing.
async fn kcat_available() -> bool {
    let ok = Command::new("docker")
        .args(["run", "--rm", KCAT_IMAGE, "-V"])
        .output()
        .await
        .is_ok_and(|out| out.status.success());
    if !ok {
        eprintln!("skipping: docker cannot run {KCAT_IMAGE}");
    }
    ok
}

async fn kcat(args: &[&str], certs: Option<&Path>) -> Output {
    kcat_fed(args, certs, "").await
}

/// Run kcat with `input` on its stdin, then closed.
async fn kcat_fed(args: &[&str], certs: Option<&Path>, input: &str) -> Output {
    use tokio::io::AsyncWriteExt;
    let mut command = Command::new("docker");
    command.args(["run", "--rm", "-i"]);
    if cfg!(target_os = "linux") {
        command.args(["--network", "host"]);
    }
    if let Some(certs) = certs {
        command.args(["-v", &format!("{}:/certs:ro", certs.display())]);
    }
    command
        .arg(KCAT_IMAGE)
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().expect("run docker");
    let mut stdin = child.stdin.take().expect("stdin");
    stdin.write_all(input.as_bytes()).await.expect("write");
    drop(stdin);
    tokio::time::timeout(KCAT_BOUND, child.wait_with_output())
        .await
        // No arguments in the message: they carry the SASL password.
        .unwrap_or_else(|_| panic!("kcat did not finish within {KCAT_BOUND:?}"))
        .expect("run docker")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn is_addr_in_use(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::AddrInUse)
    })
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("0.0.0.0:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

async fn start() -> Felix {
    let data = tempfile::tempdir().expect("dir");
    let storage = DurableStorage::open(
        data.path(),
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, "orders")
        .await
        .expect("ns");
    broker
        .register_stream(
            TENANT,
            "orders",
            "created",
            StreamMetadata {
                durable: true,
                shards: SHARDS,
                ..Default::default()
            },
        )
        .await
        .expect("stream");

    let (auth, token) = auth_and_token();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("cert");
    let certs = tempfile::tempdir().expect("certs");
    std::fs::write(certs.path().join("ca.pem"), cert.cert.pem()).expect("write cert");
    let tls = tls_config(
        cert.cert.der().clone(),
        PrivateKeyDer::from(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der())),
    )
    .expect("tls");

    let shutdown = CancellationToken::new();
    let connections = TaskTracker::new();
    let mut addrs = Vec::new();
    for (tls_on, anonymous) in [(false, None), (true, None), (false, Some(TENANT))] {
        // The advertised address has to name the port before the bind, so the
        // port comes from a probe socket that is closed again. Another test can
        // take it in between; pick a fresh one when that happens.
        let mut attempt = 0;
        let (listener, advertise) = loop {
            attempt += 1;
            let port = free_port();
            let advertise = format!("{}:{port}", docker_host());
            let config = KafkaListenerConfig {
                listen: SocketAddr::from(([0, 0, 0, 0], port)),
                advertise: advertise.clone(),
                tls: tls_on,
                anonymous_tenant: anonymous.map(str::to_string),
                default_namespace: None,
                max_connections: 64,
            };
            let cluster = BrokerCluster::new(Arc::clone(&auth), None, None, "felix", &advertise)
                .expect("cluster");
            match KafkaListener::bind(
                &config,
                Some(Arc::clone(&tls)),
                Arc::clone(&broker),
                Arc::new(cluster),
                "felix-test".to_string(),
            )
            .await
            {
                Ok(listener) => break (listener, advertise),
                Err(err) if attempt < 5 && is_addr_in_use(&err) => continue,
                Err(err) => panic!("bind: {err:?}"),
            }
        };
        tokio::spawn(listener.serve(shutdown.clone(), connections.clone()));
        addrs.push(advertise);
    }
    let anonymous = addrs.pop().expect("anonymous");
    let tls = addrs.pop().expect("tls");
    let plain = addrs.pop().expect("plain");
    Felix {
        broker,
        token,
        plain,
        tls,
        anonymous,
        certs,
        _data: data,
        shutdown,
    }
}

/// A broker auth that knows one tenant key, and a token signed by it that may
/// read and write the tenant's streams.
fn auth_and_token() -> (Arc<BrokerAuth>, String) {
    let private_key = [9u8; 32];
    let public_key = SigningKey::from_bytes(&private_key)
        .verifying_key()
        .to_bytes();
    let jwks = Jwks {
        keys: vec![Jwk {
            kty: "OKP".to_string(),
            kid: "k1".to_string(),
            alg: "EdDSA".to_string(),
            use_field: KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(URL_SAFE_NO_PAD.encode(public_key)),
        }],
    };
    let key_store = Arc::new(ControlPlaneKeyStore::new(
        "http://127.0.0.1:1".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    key_store.insert_jwks(&TenantId::new(TENANT), jwks.clone());
    let material = TenantKeyMaterial {
        kid: "k1".to_string(),
        alg: Algorithm::EdDSA,
        private_key,
        public_key,
        jwks,
    };
    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(900),
        Arc::new(HashMap::from([(TENANT.to_string(), material)])),
    );
    let token = issuer
        .mint(
            &TenantId::new(TENANT),
            "p:kafka-test",
            vec![
                "stream.subscribe:stream:t1/orders/*".to_string(),
                "stream.publish:stream:t1/orders/*".to_string(),
            ],
        )
        .expect("mint");
    (Arc::new(BrokerAuth::with_key_store(key_store)), token)
}

/// `partition:offset:value` lines, sorted.
fn records(output: &Output) -> Vec<String> {
    let mut lines: Vec<String> = stdout(output)
        .lines()
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect();
    lines.sort();
    lines
}

#[tokio::test]
async fn kcat_lists_the_topic_with_a_partition_per_shard() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    let output = felix.kcat(&["-L", "-t", TOPIC]).await;
    let text = stdout(&output);
    assert!(output.status.success(), "{text}\n{}", stderr(&output));
    assert!(
        text.contains(&format!("topic \"{TOPIC}\" with {SHARDS} partitions")),
        "{text}"
    );
    assert!(
        text.contains(&felix.plain),
        "the broker's advertised address: {text}"
    );
}

#[tokio::test]
async fn kcat_consumes_every_partition_from_the_beginning_and_from_an_offset() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    felix.publish(0, &["a0", "a1"]).await;
    felix.publish(1, &["b0", "b1", "b2"]).await;
    felix.publish(2, &["c0"]).await;

    let output = felix
        .kcat(&[
            "-C",
            "-t",
            TOPIC,
            "-o",
            "beginning",
            "-e",
            "-q",
            "-f",
            "%p:%o:%s\\n",
        ])
        .await;
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(
        records(&output),
        ["0:0:a0", "0:1:a1", "1:0:b0", "1:1:b1", "1:2:b2", "2:0:c0"]
    );

    let output = felix
        .kcat(&[
            "-C",
            "-t",
            TOPIC,
            "-p",
            "1",
            "-o",
            "1",
            "-e",
            "-q",
            "-f",
            "%p:%o:%s\\n",
        ])
        .await;
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(records(&output), ["1:1:b1", "1:2:b2"]);

    // -o end with -e: nothing new, so nothing printed, and kcat exits.
    let output = felix
        .kcat(&[
            "-C",
            "-t",
            TOPIC,
            "-o",
            "end",
            "-e",
            "-q",
            "-f",
            "%p:%o:%s\\n",
        ])
        .await;
    assert!(output.status.success(), "{}", stderr(&output));
    assert!(records(&output).is_empty(), "{:?}", records(&output));
}

#[tokio::test]
async fn kcat_queries_offsets() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    felix.publish(1, &["b0", "b1", "b2"]).await;
    let latest = format!("{TOPIC}:1:-1");
    let output = felix.kcat(&["-Q", "-t", latest.as_str()]).await;
    let text = stdout(&output);
    assert!(output.status.success(), "{text}\n{}", stderr(&output));
    assert!(text.contains(&format!("{TOPIC} [1] offset 3")), "{text}");
    let earliest = format!("{TOPIC}:1:-2");
    let output = felix.kcat(&["-Q", "-t", earliest.as_str()]).await;
    assert!(
        stdout(&output).contains(&format!("{TOPIC} [1] offset 0")),
        "{}",
        stdout(&output)
    );
}

#[tokio::test]
async fn kcat_receives_a_record_published_while_its_fetch_waits() {
    if !kcat_available().await {
        return;
    }
    let felix = Arc::new(start().await);
    felix.publish(0, &["old"]).await;
    let consumer = {
        let felix = Arc::clone(&felix);
        tokio::spawn(async move {
            // No -e: kcat keeps fetching until it has one record (-c 1). A
            // 30 s fetch wait means only the commit wake-up can deliver it in time.
            felix
                .kcat(&[
                    "-C",
                    "-t",
                    TOPIC,
                    "-p",
                    "0",
                    "-o",
                    "1",
                    "-c",
                    "1",
                    "-q",
                    "-f",
                    "%o:%s\\n",
                    "-X",
                    "fetch.wait.max.ms=30000",
                ])
                .await
        })
    };
    // Enough for the container to start and park a fetch at the tail.
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert!(!consumer.is_finished(), "kcat is waiting for a record");
    let published = Instant::now();
    felix.publish(0, &["new"]).await;
    let output = consumer.await.expect("task");
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(records(&output), ["1:new"]);
    assert!(published.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn kcat_reads_over_sasl_ssl_and_anonymously_when_allowed() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    felix.publish(2, &["c0"]).await;
    let password = format!("sasl.password={}", felix.token);
    let output = kcat(
        &[
            "-b",
            felix.tls.as_str(),
            "-X",
            "security.protocol=SASL_SSL",
            "-X",
            "ssl.ca.location=/certs/ca.pem",
            "-X",
            "sasl.mechanisms=PLAIN",
            "-X",
            "sasl.username=t1",
            "-X",
            password.as_str(),
            "-C",
            "-t",
            TOPIC,
            "-p",
            "2",
            "-o",
            "beginning",
            "-e",
            "-q",
            "-f",
            "%o:%s\\n",
        ],
        Some(felix.certs.path()),
    )
    .await;
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(records(&output), ["0:c0"]);

    let output = kcat(
        &[
            "-b",
            felix.anonymous.as_str(),
            "-C",
            "-t",
            TOPIC,
            "-p",
            "2",
            "-o",
            "beginning",
            "-e",
            "-q",
            "-f",
            "%o:%s\\n",
        ],
        None,
    )
    .await;
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(records(&output), ["0:c0"]);
}

#[tokio::test]
async fn kcat_with_a_bad_token_fails_with_the_reason() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    let output = kcat(
        &[
            "-b",
            felix.plain.as_str(),
            "-X",
            "security.protocol=SASL_PLAINTEXT",
            "-X",
            "sasl.mechanisms=PLAIN",
            "-X",
            "sasl.username=t1",
            "-X",
            "sasl.password=not-a-token",
            "-L",
            "-m",
            "10",
        ],
        None,
    )
    .await;
    let text = stderr(&output);
    assert!(!output.status.success());
    assert!(text.contains("token rejected"), "{text}");
}

#[tokio::test]
async fn kcat_joining_a_group_exits_with_a_readable_error() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    let started = Instant::now();
    let output = felix.kcat(&["-G", "kcat-group", TOPIC]).await;
    let text = format!("{}{}", stdout(&output), stderr(&output));
    eprintln!("kcat -G said:\n{text}");
    assert!(!output.status.success(), "{text}");
    assert!(
        text.contains("Felix has no Kafka consumer groups"),
        "{text}"
    );
    assert!(started.elapsed() < KCAT_BOUND);
}

/// What kcat -P needs beyond the SASL settings: the topic, and one partition
/// so offsets are easy to predict.
fn produce_args<'a>(partition: &'a str, extra: &[&'a str]) -> Vec<&'a str> {
    let mut args = vec!["-P", "-t", TOPIC, "-p", partition];
    args.extend_from_slice(extra);
    args
}

/// `offset:value` for every record in one shard, read with Felix's own log.
async fn logged(felix: &Felix, shard: u32) -> Vec<String> {
    felix
        .log_records(shard)
        .await
        .into_iter()
        .map(|record| {
            format!(
                "{}:{}",
                record.offset,
                String::from_utf8_lossy(&record.payload)
            )
        })
        .collect()
}

async fn consume(felix: &Felix, partition: &str) -> Vec<String> {
    let output = felix
        .kcat(&[
            "-C",
            "-t",
            TOPIC,
            "-p",
            partition,
            "-o",
            "beginning",
            "-e",
            "-q",
            "-f",
            "%o:%s\\n",
        ])
        .await;
    assert!(output.status.success(), "{}", stderr(&output));
    records(&output)
}

/// **What kcat produces, Felix and kcat read back at the same offsets.** A
/// record already published through Felix keeps offset 0; the produced ones
/// follow it.
#[tokio::test]
async fn kcat_produces_and_felix_and_kcat_read_the_same_records() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    felix.publish(0, &["felix"]).await;
    let output = felix
        .kcat_with_input(&produce_args("0", &[]), "a\nb\nc\n")
        .await;
    assert!(output.status.success(), "{}", stderr(&output));
    let expected = ["0:felix", "1:a", "2:b", "3:c"];
    assert_eq!(logged(&felix, 0).await, expected);
    assert_eq!(consume(&felix, "0").await, expected);
}

#[tokio::test]
async fn kcat_produces_with_every_compression_codec() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    let mut expected = Vec::new();
    for codec in ["gzip", "snappy", "lz4", "zstd"] {
        let setting = format!("compression.codec={codec}");
        let input: String = (0..20).map(|i| format!("{codec}-{i}\n")).collect();
        let output = felix
            .kcat_with_input(&produce_args("1", &["-X", &setting]), &input)
            .await;
        assert!(output.status.success(), "{codec}: {}", stderr(&output));
        expected.extend((0..20).map(|i| format!("{codec}-{i}")));
    }
    let values: Vec<String> = logged(&felix, 1)
        .await
        .into_iter()
        .map(|line| line.split_once(':').expect("offset:value").1.to_string())
        .collect();
    assert_eq!(values, expected);
}

/// acks=0, 1 and all each land their records; only 1 and all were answered,
/// and kcat does not care either way.
#[tokio::test]
async fn kcat_produces_with_acks_zero_one_and_all() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    for acks in ["0", "1", "all"] {
        let setting = format!("acks={acks}");
        let output = felix
            .kcat_with_input(
                &produce_args("2", &["-X", &setting]),
                &format!("acks-{acks}\n"),
            )
            .await;
        assert!(output.status.success(), "acks={acks}: {}", stderr(&output));
    }
    assert_eq!(
        logged(&felix, 2).await,
        ["0:acks-0", "1:acks-1", "2:acks-all"]
    );
}

/// An idempotent kcat gets a producer id and its records go through the
/// log's producer sequences: each record carries the id and the next
/// sequence, which is what lets a re-send be recognised on any leader.
#[tokio::test]
async fn kcat_produces_idempotently() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    let input: String = (0..50).map(|i| format!("r{i}\n")).collect();
    let output = felix
        .kcat_with_input(
            &produce_args("0", &["-X", "enable.idempotence=true"]),
            &input,
        )
        .await;
    assert!(output.status.success(), "{}", stderr(&output));
    let stored = felix.log_records(0).await;
    assert_eq!(stored.len(), 50, "each record once");
    let mut producer = None;
    for (sequence, record) in stored.iter().enumerate() {
        assert_eq!(record.payload, format!("r{sequence}").as_bytes());
        let RecordMark::Opens(batch) = record.mark else {
            panic!("record {} carries no producer mark", record.offset);
        };
        assert_eq!(batch.sequence, sequence as u64);
        assert_eq!(
            *producer.get_or_insert(batch.producer_id),
            batch.producer_id
        );
    }
}

/// kcat -K splits each line into a key and a value. The key picked the
/// partition (here -p does) and is not stored: Felix records have no key.
#[tokio::test]
async fn kcat_produces_keyed_records_and_the_value_is_kept() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    let output = felix
        .kcat_with_input(&produce_args("1", &["-K", ":"]), "k1:v1\nk2:v2\n")
        .await;
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(logged(&felix, 1).await, ["0:v1", "1:v2"]);
    let output = felix
        .kcat(&[
            "-C",
            "-t",
            TOPIC,
            "-p",
            "1",
            "-o",
            "beginning",
            "-e",
            "-q",
            "-f",
            "%k|%s\\n",
        ])
        .await;
    assert_eq!(records(&output), ["|v1", "|v2"]);
}

/// A transactional producer fails fast, and says why.
#[tokio::test]
async fn kcat_transactional_producer_is_refused_readably() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    let started = Instant::now();
    let output = felix
        .kcat_with_input(
            &produce_args("0", &["-X", "transactional.id=kcat-tx"]),
            "never\n",
        )
        .await;
    let text = format!("{}{}", stdout(&output), stderr(&output));
    eprintln!("kcat -P with transactional.id said:\n{text}");
    assert!(!output.status.success(), "{text}");
    assert!(
        text.contains("Felix does not support Kafka transactions"),
        "{text}"
    );
    assert!(started.elapsed() < KCAT_BOUND);
    assert!(logged(&felix, 0).await.is_empty(), "a transaction wrote");
}
