//! `pub`, `sub`, `cache` and `topology`.

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
