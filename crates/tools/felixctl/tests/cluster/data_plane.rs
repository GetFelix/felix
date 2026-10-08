//! `pub`, `sub`, `cache`, `topology`, `group` and `counter`.

use felix_cluster::{CacheSpec, Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

use crate::Env;

#[tokio::test]
#[serial]
async fn publish_subscribe_cache_and_topology_against_a_cluster() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new("orders", 1), StreamSpec::new("wide", 3)],
        caches: vec![CacheSpec::new("sessions", 2)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let env = Env::new(&cluster);

    // Publish from an argument, from stdin lines, without acks, and once.
    let run = env
        .felixctl(&cluster, &["pub", "orders", "one", "--json"])
        .await
        .ok();
    assert_eq!(run.json()["published"], 1);

    let mut args: Vec<String> = vec!["pub".into(), "orders".into(), "--json".into()];
    args.extend(env.flags(&cluster));
    let run = env.run(&args, &[], Some(b"two\nthree\n")).await.ok();
    assert_eq!(run.json()["published"], 2);

    env.felixctl(&cluster, &["pub", "orders", "four", "--ack", "none"])
        .await
        .ok();
    env.felixctl(&cluster, &["pub", "orders", "five", "--idempotent"])
        .await
        .ok();

    let run = env
        .felixctl(
            &cluster,
            &["pub", "wide", "x", "--key", "k", "--count", "3", "--json"],
        )
        .await
        .ok();
    assert_eq!(run.json()["published"], 3);

    // Read it all back, in order, with offsets.
    let first = env.first_after_probes(&cluster, "orders").await;
    let first_arg = first.to_string();
    let run = env
        .felixctl(
            &cluster,
            &[
                "sub", "orders", "--from", &first_arg, "--count", "5", "--json",
            ],
        )
        .await
        .ok();
    let events = run.json_lines();
    let payloads: Vec<&str> = events
        .iter()
        .map(|e| e["payload"].as_str().unwrap())
        .collect();
    assert_eq!(payloads, ["one", "two", "three", "four", "five"]);
    let offsets: Vec<u64> = events
        .iter()
        .map(|e| e["offset"].as_u64().unwrap())
        .collect();
    assert!(offsets.windows(2).all(|w| w[0] < w[1]), "{offsets:?}");

    let run = env
        .felixctl(
            &cluster,
            &["sub", "orders", "--from", &first_arg, "--count", "1"],
        )
        .await
        .ok();
    assert_eq!(run.stdout, "one\n");

    // `earliest` starts at offset 0, whichever record that is.
    let run = env
        .felixctl(
            &cluster,
            &[
                "sub", "orders", "--from", "earliest", "--count", "1", "--json",
            ],
        )
        .await
        .ok();
    assert_eq!(run.json()["offset"], 0, "{}", run.stdout);

    let run = env
        .felixctl(
            &cluster,
            &[
                "sub", "orders", "--shard", "0", "--from", &first_arg, "--count", "2", "--format",
                "offsets",
            ],
        )
        .await
        .ok();
    let lines: Vec<&str> = run.stdout.lines().collect();
    assert_eq!(lines.len(), 2, "{}", run.stdout);
    assert!(lines[0] == format!("0\t{first}\tone"), "{}", run.stdout);

    // Every shard of a wide stream, merged.
    let run = env
        .felixctl(
            &cluster,
            &[
                "sub", "wide", "--from", "earliest", "--count", "3", "--json",
            ],
        )
        .await
        .ok();
    assert_eq!(run.json_lines().len(), 3);

    // A stream the brokers do not know.
    let run = env
        .felixctl(&cluster, &["sub", "missing", "--count", "1"])
        .await;
    assert_eq!(run.code, 5, "{}", run.stderr);
    let run = env.felixctl(&cluster, &["pub", "missing", "x"]).await;
    assert_eq!(
        run.code, 4,
        "a refused publish is a server error: {}",
        run.stderr
    );

    // Cache keys.
    env.felixctl(
        &cluster,
        &[
            "cache", "put", "sessions", "user-1", "in", "--ttl-ms", "600000",
        ],
    )
    .await
    .ok();
    let run = env
        .felixctl(&cluster, &["cache", "get", "sessions", "user-1"])
        .await
        .ok();
    assert_eq!(run.stdout, "in\n");
    let run = env
        .felixctl(&cluster, &["cache", "get", "sessions", "user-1", "--json"])
        .await
        .ok();
    assert_eq!(run.json()["value"], "in");

    let mut args: Vec<String> = vec![
        "cache".into(),
        "put".into(),
        "sessions".into(),
        "user-2".into(),
    ];
    args.extend(env.flags(&cluster));
    env.run(&args, &[], Some(b"from stdin")).await.ok();

    let run = env
        .felixctl(
            &cluster,
            &[
                "cache",
                "watch",
                "sessions",
                "--key",
                "user-2",
                "--retained",
                "--count",
                "1",
                "--json",
            ],
        )
        .await
        .ok();
    assert_eq!(run.json()["value"], "from stdin");
    let run = env
        .felixctl(
            &cluster,
            &[
                "cache",
                "watch",
                "sessions",
                "--prefix",
                "user-",
                "--retained",
                "--count",
                "2",
                "--json",
            ],
        )
        .await
        .ok();
    let mut keys: Vec<String> = run
        .json_lines()
        .iter()
        .map(|change| change["key"].as_str().unwrap().to_string())
        .collect();
    keys.sort();
    assert_eq!(keys, ["user-1", "user-2"]);

    let run = env
        .felixctl(&cluster, &["cache", "del", "sessions", "user-1", "--json"])
        .await
        .ok();
    assert_eq!(run.json()["deleted"], true);
    let run = env
        .felixctl(&cluster, &["cache", "get", "sessions", "user-1"])
        .await;
    assert_eq!(run.code, 5, "a missing key exits 5: {}", run.stderr);

    // Where things live.
    let run = env
        .felixctl(&cluster, &["topology", "orders", "--json"])
        .await
        .ok();
    let topology = run.json();
    assert_eq!(topology["shards"], 1);
    assert_eq!(topology["owners_known"], true);
    let leader = topology["shard_owners"][0]["leader"]
        .as_str()
        .expect("a leader");
    assert!(
        cluster.node_ids().iter().any(|id| id == leader),
        "{topology}"
    );

    let run = env
        .felixctl(&cluster, &["topology", "sessions", "--cache", "--json"])
        .await
        .ok();
    assert_eq!(run.json()["shards"], 2);
    assert_eq!(run.json()["shard_owners"].as_array().unwrap().len(), 2);

    let run = env.felixctl(&cluster, &["topology", "wide"]).await.ok();
    assert!(run.stdout.contains("3 shard(s)"), "{}", run.stdout);
}

#[tokio::test]
#[serial]
async fn groups_and_counters_against_a_cluster() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new("jobs", 2)],
        caches: vec![CacheSpec::new("stats", 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let env = Env::new(&cluster);
    let operator = cluster.group_operator_token();
    let op = |args: &[&str]| -> Vec<String> {
        let mut all: Vec<String> = args.iter().map(|a| a.to_string()).collect();
        all.extend(["--token".to_string(), operator.clone()]);
        all
    };

    // Start after the harness's probes, then publish three records. Unkeyed
    // records go to shard 0; idempotent publishes are answered after the
    // write, so a poll right after sees them.
    let args = op(&["group", "create", "jobs", "billing", "--json"]);
    let run = env.felixctl(&cluster, &strs(&args)).await.ok();
    let shards = run.json()["shards"].as_array().unwrap().clone();
    assert_eq!(shards.len(), 2, "{}", run.stdout);
    assert!(
        shards.iter().all(|s| s["created"] == true),
        "{}",
        run.stdout
    );
    for payload in ["a", "b", "c"] {
        env.felixctl(&cluster, &["pub", "jobs", payload, "--idempotent"])
            .await
            .ok();
    }

    let mut polled = Vec::new();
    for _ in 0..20 {
        let run = env
            .felixctl(
                &cluster,
                &[
                    "group",
                    "poll",
                    "jobs",
                    "billing",
                    "--max",
                    "3",
                    "--wait-ms",
                    "500",
                    "--json",
                ],
            )
            .await
            .ok();
        polled.extend(run.json_lines());
        if polled.len() >= 3 {
            break;
        }
    }
    let payloads: Vec<&str> = polled
        .iter()
        .map(|r| r["payload"].as_str().unwrap())
        .collect();
    assert_eq!(payloads, ["a", "b", "c"]);
    let claim = |i: usize| polled[i]["claim"].as_str().unwrap().to_string();
    assert!(
        claim(0).starts_with("0:") && claim(0).ends_with(":1"),
        "{}",
        claim(0)
    );

    // Finish one, hand one back, keep one longer and then give up on it.
    env.felixctl(&cluster, &["group", "ack", "jobs", "billing", &claim(0)])
        .await
        .ok();
    let run = env
        .felixctl(
            &cluster,
            &[
                "group",
                "extend",
                "jobs",
                "billing",
                &claim(2),
                "--for-ms",
                "60000",
                "--json",
            ],
        )
        .await
        .ok();
    assert!(
        run.json()["visible_ms"].as_u64().unwrap() > 0,
        "{}",
        run.stdout
    );
    env.felixctl(&cluster, &["group", "nack", "jobs", "billing", &claim(1)])
        .await
        .ok();
    env.felixctl(
        &cluster,
        &["group", "dead-letters", "add", "jobs", "billing", &claim(2)],
    )
    .await
    .ok();
    let run = env
        .felixctl(
            &cluster,
            &["group", "dead-letters", "ls", "jobs", "billing", "--json"],
        )
        .await
        .ok();
    let dead = run.json()["dead_letters"].as_array().unwrap().clone();
    assert_eq!(dead.len(), 1, "{}", run.stdout);
    let dead_claim = dead[0]["claim"].as_str().unwrap().to_string();
    assert_eq!(dead[0]["offset"], polled[2]["offset"]);

    // The nacked record comes back as a second attempt.
    let run = env
        .felixctl(
            &cluster,
            &[
                "group",
                "poll",
                "jobs",
                "billing",
                "--shard",
                "0",
                "--wait-ms",
                "2000",
                "--json",
            ],
        )
        .await
        .ok();
    let again = run.json_lines();
    assert_eq!(again.len(), 1, "{}", run.stdout);
    assert_eq!(again[0]["payload"], "b");
    assert_eq!(again[0]["attempts"], 2);
    let retry = again[0]["claim"].as_str().unwrap().to_string();
    env.felixctl(&cluster, &["group", "ack", "jobs", "billing", &retry])
        .await
        .ok();

    // Discarding asks first, and nothing can answer here.
    let run = env
        .felixctl(
            &cluster,
            &strs(&op(&[
                "group",
                "dead-letters",
                "discard",
                "jobs",
                "billing",
                &dead_claim,
            ])),
        )
        .await;
    assert_eq!(run.code, 2, "{}", run.stderr);
    let args = op(&[
        "group",
        "dead-letters",
        "discard",
        "jobs",
        "billing",
        &dead_claim,
        "--yes",
    ]);
    env.felixctl(&cluster, &strs(&args)).await.ok();

    let run = env
        .felixctl(
            &cluster,
            &["group", "describe", "jobs", "billing", "--json"],
        )
        .await
        .ok();
    let shard0 = &run.json()["shards"][0];
    assert_eq!(shard0["lag"], 0, "{}", run.stdout);
    assert_eq!(shard0["in_flight"], 0, "{}", run.stdout);
    assert_eq!(shard0["dead_letters"], 0, "{}", run.stdout);
    let run = env
        .felixctl(&cluster, &["group", "describe", "jobs", "billing"])
        .await
        .ok();
    assert!(run.stdout.starts_with("SHARD"), "{}", run.stdout);

    // Back to the start replays finished records, so it needs --yes.
    let args = op(&[
        "group", "seek", "jobs", "billing", "earliest", "--shard", "0",
    ]);
    let run = env.felixctl(&cluster, &strs(&args)).await;
    assert_eq!(run.code, 2, "{}", run.stderr);
    let args = op(&[
        "group", "seek", "jobs", "billing", "earliest", "--shard", "0", "--yes",
    ]);
    env.felixctl(&cluster, &strs(&args)).await.ok();
    // An offset means nothing across two shards.
    let args = op(&["group", "seek", "jobs", "billing", "0", "--yes"]);
    assert_eq!(env.felixctl(&cluster, &strs(&args)).await.code, 2);

    let run = env
        .felixctl(&cluster, &strs(&op(&["group", "rm", "jobs", "billing"])))
        .await;
    assert_eq!(run.code, 2, "{}", run.stderr);
    let args = op(&["group", "rm", "jobs", "billing", "--yes", "--json"]);
    let run = env.felixctl(&cluster, &strs(&args)).await.ok();
    assert_eq!(run.json()["deleted"], true);

    // Counters.
    let run = env
        .felixctl(&cluster, &["counter", "add", "stats", "views", "5"])
        .await
        .ok();
    assert_eq!(run.stdout, "5\n");
    env.felixctl(&cluster, &["counter", "add", "stats", "views", "-2"])
        .await
        .ok();
    let run = env
        .felixctl(&cluster, &["counter", "get", "stats", "views", "--json"])
        .await
        .ok();
    assert_eq!(run.json()["value"], 3);
    let run = env
        .felixctl(&cluster, &["counter", "get", "stats", "never"])
        .await;
    assert_eq!(run.code, 5, "{}", run.stderr);
}

fn strs(args: &[String]) -> Vec<&str> {
    args.iter().map(String::as_str).collect()
}
