//! The `felix-cluster` commands the quickstart tells a newcomer to run, run
//! as a newcomer would: the built binary, its exit status and what it prints.
//!
//! The library suites cover what the commands are built from. They do not
//! cover the commands, so a demo that waits on something only one shard
//! engine reports can go on failing for everyone who follows the docs.
//!
//! Run with `cargo build -p felix-broker-service --bin felix-broker &&
//! cargo test -p felix-cluster --test quickstart_commands`.
use std::process::Stdio;
use std::time::Duration;

use felix_cluster::wait;
use serial_test::serial;
use tokio::process::Command;

/// Long enough for a three-broker cluster to start, run a demo with a
/// failover in it, and stop, on a loaded machine.
const COMMAND_FINISHES_WITHIN: Duration = Duration::from_secs(180);

fn felix_cluster() -> Command {
    Command::new(env!("CARGO_BIN_EXE_felix-cluster"))
}

/// Run one self-contained command to its end and return what it printed.
/// Panics with both streams when it fails or never finishes.
async fn run(args: &[&str]) -> String {
    let child = felix_cluster()
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .output();
    let output = tokio::time::timeout(wait::budget(COMMAND_FINISHES_WITHIN), child)
        .await
        .unwrap_or_else(|_| panic!("felix-cluster {args:?} did not finish"))
        .expect("run felix-cluster");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "felix-cluster {args:?} failed ({}):\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status,
    );
    stdout
}

#[tokio::test]
#[serial]
async fn status_prints_who_owns_each_shard() {
    let printed = run(&["status"]).await;
    assert!(printed.contains("orders/0 -> broker-"), "{printed}");
}

#[tokio::test]
#[serial]
async fn smoke_crosses_a_broker() {
    let printed = run(&["smoke"]).await;
    assert!(printed.contains("smoke: ok"), "{printed}");
}

#[tokio::test]
#[serial]
async fn demo_runs_to_its_end() {
    let printed = run(&["demo", "--pace", "0"]).await;
    assert!(
        printed.contains("Every record reached the subscriber"),
        "{printed}"
    );
}

#[tokio::test]
#[serial]
async fn failover_keeps_what_it_acknowledged() {
    let printed = run(&["failover", "--pace", "0"]).await;
    assert!(
        printed.contains("Every record acknowledged before the kill is still here"),
        "{printed}"
    );
}

#[tokio::test]
#[serial]
async fn consistency_runs_to_its_end() {
    // The hybrid engine's version of this demo fails at its replay. The
    // hybrid is frozen, so that path is left to retire with it.
    if !felix_cluster::suite_runs_on_raft() {
        eprintln!("skipped on the hybrid: retires with it");
        return;
    }
    let printed = run(&["consistency", "--pace", "0"]).await;
    if let Err(why) = consistency_showed_the_trade(&printed) {
        panic!("{why}:\n{printed}");
    }
}

/// The "N acknowledged, M missing afterwards" the demo's summary gives for
/// one consistency level.
fn summary(printed: &str, level: &str) -> Option<(usize, usize)> {
    let line = printed
        .lines()
        .find(|line| line.trim_start().starts_with(level) && line.contains("missing afterwards"))?;
    let mut numbers = line
        .split(|c: char| !c.is_ascii_digit())
        .filter_map(|word| word.parse::<usize>().ok());
    Some((numbers.next()?, numbers.next()?))
}

/// Whether a `consistency` run that exited 0 showed anything. The heading
/// alone does not: a run whose publisher was acknowledged nothing loses
/// nothing either.
fn consistency_showed_the_trade(printed: &str) -> Result<(), String> {
    let (quorum, quorum_missing) =
        summary(printed, "Quorum").ok_or("no summary line for Quorum")?;
    let (leader, _) = summary(printed, "Leader").ok_or("no summary line for Leader")?;
    if quorum == 0 || leader == 0 {
        return Err(format!(
            "nothing was acknowledged (Quorum {quorum}, Leader {leader}), so nothing was shown"
        ));
    }
    if quorum_missing != 0 {
        return Err(format!(
            "Quorum is missing {quorum_missing} acknowledged record(s)"
        ));
    }
    Ok(())
}

#[test]
fn a_consistency_run_that_acknowledged_nothing_is_not_a_pass() {
    let run = |quorum: &str, leader: &str| {
        format!(
            "==> The trade, stated plainly\n\n    Quorum   {quorum} missing afterwards.\n    Leader   {leader} missing afterwards.\n"
        )
    };
    let shown = run("24 acknowledged, 0", "31 acknowledged, 7");
    assert_eq!(consistency_showed_the_trade(&shown), Ok(()));
    for hollow in [
        run("0 acknowledged, 0", "31 acknowledged, 7"),
        run("24 acknowledged, 0", "0 acknowledged, 0"),
        run("24 acknowledged, 2", "31 acknowledged, 7"),
        "==> The trade, stated plainly\n".to_string(),
    ] {
        assert!(consistency_showed_the_trade(&hollow).is_err(), "{hollow}");
    }
}

#[cfg(unix)]
mod up {
    use std::path::Path;
    use std::time::Instant;

    use felix_cluster::session::Session;
    use tokio::io::{AsyncBufReadExt, BufReader};

    use super::*;

    /// A held `felix-cluster up`. Stopped with SIGTERM, which runs its
    /// teardown: killing it outright would orphan its brokers.
    struct Held(tokio::process::Child);

    impl Drop for Held {
        fn drop(&mut self) {
            let Some(pid) = self.0.id() else { return };
            // SAFETY: `pid` is this test's own child, not yet reaped.
            unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
            let deadline = Instant::now() + Duration::from_secs(30);
            while Instant::now() < deadline {
                if matches!(self.0.try_wait(), Ok(Some(_))) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            let _ = self.0.start_kill();
        }
    }

    /// An attach command against the cluster whose session file is in `tmp`.
    fn attached(tmp: &Path, args: &[&str]) -> Command {
        let mut command = felix_cluster();
        command
            .args(args)
            .env("TMPDIR", tmp)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        command
    }

    async fn run_attached(tmp: &Path, args: &[&str]) -> String {
        let output = tokio::time::timeout(
            wait::budget(Duration::from_secs(60)),
            attached(tmp, args).output(),
        )
        .await
        .unwrap_or_else(|_| panic!("felix-cluster {args:?} did not finish"))
        .expect("run felix-cluster");
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        assert!(
            output.status.success(),
            "felix-cluster {args:?} failed ({}):\nstdout:\n{stdout}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr),
        );
        stdout
    }

    /// How many Raft shards the cluster's brokers lead between them.
    async fn raft_shards_led(session: &Session) -> f64 {
        let mut led = 0.0;
        for node in &session.nodes {
            let Ok(response) = reqwest::get(format!("http://{}/metrics", node.metrics_addr)).await
            else {
                continue;
            };
            let body = response.text().await.unwrap_or_default();
            led += body
                .lines()
                .filter(|line| line.starts_with("felix_broker_raft_shards_led "))
                .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
                .sum::<f64>();
        }
        led
    }

    /// The quickstart's three windows: `up`, `subscribe`, `publish`. With no
    /// engine named anywhere, the cluster `up` holds runs its shards on Raft.
    #[tokio::test]
    #[serial]
    async fn up_runs_on_raft_and_the_other_windows_reach_it() {
        // The session file goes to the temp directory, so a directory of its
        // own keeps this cluster apart from one a developer is holding.
        let tmp = tempfile::tempdir().expect("temp dir");
        let session_file = tmp.path().join("felix-cluster.json");
        let up = felix_cluster()
            .args(["up", "--nodes", "3"])
            .env("TMPDIR", tmp.path())
            // As a newcomer runs it: no engine chosen, here or by the suite.
            .env_remove("FELIX_TEST_SHARD_ENGINE")
            .env_remove("FELIX_SHARD_ENGINE")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start felix-cluster up");
        let mut up = Held(up);

        // `up` writes the session file once the cluster serves.
        wait::until(COMMAND_FINISHES_WITHIN, "the session file", || async {
            session_file.is_file()
        })
        .await
        .unwrap_or_else(|err| panic!("{err:#}; up exited: {:?}", up.0.try_wait()));
        let session = Session::read(&session_file).expect("read the session file");

        wait::until(
            Duration::from_secs(30),
            "a broker to lead a Raft shard",
            || async { raft_shards_led(&session).await >= 1.0 },
        )
        .await
        .expect("felix-cluster up, with no engine named, runs its shards on Raft");

        let owners = run_attached(tmp.path(), &["owners"]).await;
        assert!(owners.contains("orders/0 -> broker-"), "{owners}");

        let mut subscriber = attached(tmp.path(), &["subscribe", "orders"])
            .spawn()
            .expect("start felix-cluster subscribe");
        let mut lines = BufReader::new(subscriber.stdout.take().expect("stdout")).lines();

        // `subscribe` says it is waiting before its subscription is open, so
        // publish until a record arrives rather than once and hope.
        let deadline = Instant::now() + wait::budget(Duration::from_secs(60));
        let mut seen = String::new();
        let delivered = loop {
            let published = run_attached(tmp.path(), &["publish", "orders", "hello"]).await;
            assert!(
                published.contains("forwarded to") && published.contains("acknowledged"),
                "a publish through a broker that does not own the shard is forwarded:\n{published}"
            );
            let window = tokio::time::sleep(Duration::from_millis(500));
            tokio::pin!(window);
            let arrived = loop {
                tokio::select! {
                    _ = &mut window => break false,
                    line = lines.next_line() => match line {
                        Ok(Some(line)) => {
                            let hit = line.contains("hello");
                            seen.push_str(&line);
                            seen.push('\n');
                            if hit {
                                break true;
                            }
                        }
                        _ => break false,
                    },
                }
            };
            if arrived || Instant::now() >= deadline {
                break arrived;
            }
        };
        assert!(
            delivered,
            "the subscriber never printed a published record:\n{seen}"
        );
    }
}
