//! The Kafka listener against a real Kafka client: `kcat` (librdkafka 1.8).
//!
//! kcat runs from the `edenhill/kcat:1.7.1` image, so these tests need Docker
//! or Podman: `CONTAINER_ENGINE`, else whichever of the two answers. Without
//! one each test says so and returns; CI has Docker. On Linux the container
//! shares the host network; elsewhere (Docker Desktop, `podman machine`) it
//! reaches the host as `host.docker.internal` or `host.containers.internal`,
//! which is also what the listener advertises in Metadata.
//!
//! Run with `cargo test -p felix-broker-service --test kafka_kcat`.

#[path = "kafka_kcat/fixture.rs"]
mod fixture;
#[path = "support/raft_shard.rs"]
mod raft_shard;

use std::path::Path;
use std::process::Output;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fixture::{Felix, Fixture, Listener};
use serial_test::serial;
use tokio::process::Command;

// Fully qualified: Podman may refuse a short name it has to guess at.
const KCAT_IMAGE: &str = "docker.io/edenhill/kcat:1.7.1";
const TENANT: &str = "t1";
const TOPIC: &str = "orders.created";
const SHARDS: u32 = 3;
/// Generous: the image may run under emulation, and each kcat is a container.
const KCAT_BOUND: Duration = Duration::from_secs(60);

impl Felix {
    /// kcat against the SASL/PLAIN listener, with this tenant's token.
    async fn kcat(&self, args: &[&str]) -> Output {
        self.kcat_with_input(args, "").await
    }

    /// [`Self::kcat`], with `input` on kcat's stdin: what `-P` produces.
    async fn kcat_with_input(&self, args: &[&str], input: &str) -> Output {
        let username = format!("sasl.username={TENANT}");
        let password = format!("sasl.password={}", self.token);
        let mut all = vec![
            "-b",
            self.plain(),
            "-X",
            "security.protocol=SASL_PLAINTEXT",
            "-X",
            "sasl.mechanisms=PLAIN",
            "-X",
            username.as_str(),
            "-X",
            password.as_str(),
        ];
        all.extend_from_slice(args);
        kcat_fed(&all, None, input).await
    }
}

/// Run a test body against both fixtures: `<name>::on_a_local_broker` and
/// `<name>::on_a_raft_shard`. The Raft runs each start a cluster, so they take
/// turns.
macro_rules! on_both {
    ($($name:ident),* $(,)?) => {$(
        mod $name {
            use super::*;

            #[tokio::test]
            async fn on_a_local_broker() {
                super::$name(Fixture::Local).await;
            }

            #[serial(raft_fixture)]
            #[tokio::test]
            async fn on_a_raft_shard() {
                super::$name(Fixture::RaftShard).await;
            }
        }
    )*};
}

on_both!(
    kcat_lists_the_topic_with_a_partition_per_shard,
    kcat_consumes_every_partition_from_the_beginning_and_from_an_offset,
    kcat_queries_offsets,
    kcat_receives_a_record_published_while_its_fetch_waits,
    kcat_reads_over_sasl_ssl_and_anonymously_when_allowed,
    kcat_with_a_bad_token_fails_with_the_reason,
    kcat_joining_a_group_exits_with_a_readable_error,
    kcat_produces_and_felix_and_kcat_read_the_same_records,
    kcat_produces_with_every_compression_codec,
    kcat_produces_with_acks_zero_one_and_all,
    kcat_produces_idempotently,
    kcat_produces_keyed_records_and_the_value_is_kept,
    kcat_keys_and_headers_are_kept_when_the_listener_opts_in,
    kcat_transactional_producer_is_refused_readably,
);

/// The same choice as `felix_cluster::container::engine`, which this crate
/// cannot depend on.
fn engine() -> &'static str {
    static ENGINE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ENGINE.get_or_init(|| {
        if let Ok(chosen) = std::env::var("CONTAINER_ENGINE")
            && !chosen.is_empty()
        {
            return chosen;
        }
        ["docker", "podman"]
            .into_iter()
            .find(|engine| {
                std::process::Command::new(engine)
                    .arg("version")
                    .output()
                    .is_ok_and(|out| out.status.success())
            })
            .unwrap_or("docker")
            .to_string()
    })
}

fn container_host() -> &'static str {
    if cfg!(target_os = "linux") {
        "127.0.0.1"
    } else if engine().ends_with("podman") {
        "host.containers.internal"
    } else {
        "host.docker.internal"
    }
}

/// Whether the container engine can run the kcat image. Pulls it if missing.
async fn kcat_available() -> bool {
    let ok = Command::new(engine())
        .args(["run", "--rm", KCAT_IMAGE, "-V"])
        .output()
        .await
        .is_ok_and(|out| out.status.success());
    if !ok {
        eprintln!("skipping: {} cannot run {KCAT_IMAGE}", engine());
    }
    ok
}

async fn kcat(args: &[&str], certs: Option<&Path>) -> Output {
    kcat_fed(args, certs, "").await
}

/// Run kcat with `input` on its stdin, then closed.
async fn kcat_fed(args: &[&str], certs: Option<&Path>, input: &str) -> Output {
    use tokio::io::AsyncWriteExt;
    let mut command = Command::new(engine());
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
    let mut child = command.spawn().expect("run the container engine");
    let mut stdin = child.stdin.take().expect("stdin");
    stdin.write_all(input.as_bytes()).await.expect("write");
    drop(stdin);
    tokio::time::timeout(KCAT_BOUND, child.wait_with_output())
        .await
        // No arguments in the message: they carry the SASL password.
        .unwrap_or_else(|_| panic!("kcat did not finish within {KCAT_BOUND:?}"))
        .expect("run the container engine")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
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

async fn kcat_lists_the_topic_with_a_partition_per_shard(fixture: Fixture) {
    if !kcat_available().await {
        return;
    }
    let felix = Felix::start(fixture).await;
    let output = felix.kcat(&["-L", "-t", TOPIC]).await;
    let text = stdout(&output);
    assert!(output.status.success(), "{text}\n{}", stderr(&output));
    assert!(
        text.contains(&format!("topic \"{TOPIC}\" with {SHARDS} partitions")),
        "{text}"
    );
    assert!(
        text.contains(felix.plain()),
        "the broker's advertised address: {text}"
    );
}

async fn kcat_consumes_every_partition_from_the_beginning_and_from_an_offset(fixture: Fixture) {
    if !kcat_available().await {
        return;
    }
    let felix = Felix::start(fixture).await;
    let a = felix.publish(0, &["a0", "a1"]).await;
    let b = felix.publish(1, &["b0", "b1", "b2"]).await;
    let c = felix.publish(2, &["c0"]).await;

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
        [
            format!("0:{a}:a0"),
            format!("0:{}:a1", a + 1),
            format!("1:{b}:b0"),
            format!("1:{}:b1", b + 1),
            format!("1:{}:b2", b + 2),
            format!("2:{c}:c0"),
        ]
    );

    let output = felix
        .kcat(&[
            "-C",
            "-t",
            TOPIC,
            "-p",
            "1",
            "-o",
            &(b + 1).to_string(),
            "-e",
            "-q",
            "-f",
            "%p:%o:%s\\n",
        ])
        .await;
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(
        records(&output),
        [format!("1:{}:b1", b + 1), format!("1:{}:b2", b + 2)]
    );

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

async fn kcat_queries_offsets(fixture: Fixture) {
    if !kcat_available().await {
        return;
    }
    let felix = Felix::start(fixture).await;
    let first = felix.publish(1, &["b0", "b1", "b2"]).await;
    // Latest is one past the last record. Earliest is at or below the first:
    // a Raft log's first entries hold no record (docs/per-shard-raft.md), and
    // a fetch from earliest still starts at the first record.
    let latest = format!("{TOPIC}:1:-1");
    let output = felix.kcat(&["-Q", "-t", latest.as_str()]).await;
    let text = stdout(&output);
    assert!(output.status.success(), "{text}\n{}", stderr(&output));
    assert_eq!(queried_offset(&text), first + 3, "{text}");
    let earliest = format!("{TOPIC}:1:-2");
    let output = felix.kcat(&["-Q", "-t", earliest.as_str()]).await;
    let text = stdout(&output);
    assert!(output.status.success(), "{text}\n{}", stderr(&output));
    assert!(queried_offset(&text) <= first, "{text}");
}

/// The offset in kcat -Q's `<topic> [<partition>] offset <n>` line.
fn queried_offset(text: &str) -> u64 {
    let prefix = format!("{TOPIC} [1] offset ");
    text.lines()
        .find_map(|line| line.trim().strip_prefix(&prefix))
        .and_then(|offset| offset.trim().parse().ok())
        .unwrap_or_else(|| panic!("no offset for partition 1 in {text:?}"))
}

async fn kcat_receives_a_record_published_while_its_fetch_waits(fixture: Fixture) {
    if !kcat_available().await {
        return;
    }
    let felix = Arc::new(Felix::start(fixture).await);
    let next = felix.publish(0, &["old"]).await + 1;
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
                    &next.to_string(),
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
    assert_eq!(felix.publish(0, &["new"]).await, next);
    let output = consumer.await.expect("task");
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(records(&output), [format!("{next}:new")]);
    assert!(published.elapsed() < Duration::from_secs(3));
}

async fn kcat_reads_over_sasl_ssl_and_anonymously_when_allowed(fixture: Fixture) {
    if !kcat_available().await {
        return;
    }
    let mut felix = Felix::start(fixture).await;
    let c0 = format!("{}:c0", felix.publish(2, &["c0"]).await);
    let username = format!("sasl.username={TENANT}");
    let password = format!("sasl.password={}", felix.token);
    let tls = felix.listener(Listener::Tls).await;
    let output = kcat(
        &[
            "-b",
            tls.as_str(),
            "-X",
            "security.protocol=SASL_SSL",
            "-X",
            "ssl.ca.location=/certs/ca.pem",
            "-X",
            "sasl.mechanisms=PLAIN",
            "-X",
            username.as_str(),
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
        Some(felix.certs()),
    )
    .await;
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(records(&output), std::slice::from_ref(&c0));

    let anonymous = felix.listener(Listener::Anonymous).await;

    let output = kcat(
        &[
            "-b",
            anonymous.as_str(),
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
    assert_eq!(records(&output), std::slice::from_ref(&c0));
}

async fn kcat_with_a_bad_token_fails_with_the_reason(fixture: Fixture) {
    if !kcat_available().await {
        return;
    }
    let felix = Felix::start(fixture).await;
    let output = kcat(
        &[
            "-b",
            felix.plain(),
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

async fn kcat_joining_a_group_exits_with_a_readable_error(fixture: Fixture) {
    if !kcat_available().await {
        return;
    }
    let felix = Felix::start(fixture).await;
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

/// `offset:value` for every record in one shard, as Felix stored it.
async fn logged(felix: &Felix, shard: u32) -> Vec<String> {
    felix
        .stored(shard)
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

/// `offset:value` for `values` at consecutive offsets from `first`.
fn at(first: u64, values: &[&str]) -> Vec<String> {
    values
        .iter()
        .zip(first..)
        .map(|(value, offset)| format!("{offset}:{value}"))
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
/// record already published through Felix keeps its offset, 0 on a local
/// broker; the produced ones follow it.
async fn kcat_produces_and_felix_and_kcat_read_the_same_records(fixture: Fixture) {
    if !kcat_available().await {
        return;
    }
    let felix = Felix::start(fixture).await;
    let first = felix.publish(0, &["felix"]).await;
    let output = felix
        .kcat_with_input(&produce_args("0", &[]), "a\nb\nc\n")
        .await;
    assert!(output.status.success(), "{}", stderr(&output));
    let expected = at(first, &["felix", "a", "b", "c"]);
    assert_eq!(logged(&felix, 0).await, expected);
    assert_eq!(consume(&felix, "0").await, expected);
}

async fn kcat_produces_with_every_compression_codec(fixture: Fixture) {
    if !kcat_available().await {
        return;
    }
    let felix = Felix::start(fixture).await;
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
async fn kcat_produces_with_acks_zero_one_and_all(fixture: Fixture) {
    if !kcat_available().await {
        return;
    }
    let felix = Felix::start(fixture).await;
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
    let first = felix.first_offset(2).await;
    assert_eq!(
        logged(&felix, 2).await,
        at(first, &["acks-0", "acks-1", "acks-all"])
    );
}

/// An idempotent kcat gets a producer id and its records go through the
/// log's producer sequences: each record carries the id and the next
/// sequence, which is what lets a re-send be recognised on any leader.
async fn kcat_produces_idempotently(fixture: Fixture) {
    if !kcat_available().await {
        return;
    }
    let felix = Felix::start(fixture).await;
    let input: String = (0..50).map(|i| format!("r{i}\n")).collect();
    let output = felix
        .kcat_with_input(
            &produce_args("0", &["-X", "enable.idempotence=true"]),
            &input,
        )
        .await;
    assert!(output.status.success(), "{}", stderr(&output));
    let stored = felix.stored(0).await;
    assert_eq!(stored.len(), 50, "each record once");
    for (sequence, record) in stored.iter().enumerate() {
        assert_eq!(record.payload, format!("r{sequence}").as_bytes());
    }
    // No client is shown producer marks, so on a cluster only the records
    // are checked here. A Raft shard's sequences are checked by
    // `an_idempotent_produce_is_one_proposal` (src/serving/kafka/tests.rs).
    let Some(marks) = felix.producer_marks(0).await else {
        return;
    };
    let mut producer = None;
    for (sequence, (record, mark)) in stored.iter().zip(marks).enumerate() {
        let Some((producer_id, batch_sequence)) = mark else {
            panic!("record {} carries no producer mark", record.offset);
        };
        assert_eq!(batch_sequence, sequence as u64);
        assert_eq!(*producer.get_or_insert(producer_id), producer_id);
    }
}

/// kcat -K splits each line into a key and a value. The listener has not opted
/// in to keys and headers, so the key is dropped and the value kept.
async fn kcat_produces_keyed_records_and_the_value_is_kept(fixture: Fixture) {
    if !kcat_available().await {
        return;
    }
    let felix = Felix::start(fixture).await;
    let output = felix
        .kcat_with_input(&produce_args("1", &["-K", ":"]), "k1:v1\nk2:v2\n")
        .await;
    assert!(output.status.success(), "{}", stderr(&output));
    let first = felix.first_offset(1).await;
    assert_eq!(logged(&felix, 1).await, at(first, &["v1", "v2"]));
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

/// **With the listener opted in, a key and headers survive kcat's round
/// trip**: -K, -H on the way in, %k and %h on the way out, and a header with no
/// value comes back empty.
async fn kcat_keys_and_headers_are_kept_when_the_listener_opts_in(fixture: Fixture) {
    if !kcat_available().await {
        return;
    }
    let felix = Felix::start_with(fixture, true).await;
    let output = felix
        .kcat_with_input(
            &produce_args("1", &["-K", ":", "-H", "trace=t-1", "-H", "nil"]),
            "k1:v1\nk2:v2\n",
        )
        .await;
    assert!(output.status.success(), "{}", stderr(&output));
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
            "%k|%s|%h\\n",
        ])
        .await;
    assert!(output.status.success(), "{}", stderr(&output));
    let lines = records(&output);
    assert_eq!(lines.len(), 2, "{lines:?}");
    for (line, (key, value)) in lines.iter().zip([("k1", "v1"), ("k2", "v2")]) {
        let prefix = format!("{key}|{value}|trace=t-1,nil=");
        assert!(line.starts_with(&prefix), "{line:?}");
    }
}

/// A transactional producer fails fast, and says why.
async fn kcat_transactional_producer_is_refused_readably(fixture: Fixture) {
    if !kcat_available().await {
        return;
    }
    let felix = Felix::start(fixture).await;
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
