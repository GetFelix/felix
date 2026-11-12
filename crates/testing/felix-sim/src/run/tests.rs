use super::*;

fn small() -> SimConfig {
    SimConfig {
        brokers: 3,
        shards: 6,
        cache_shards: 2,
        clients: 3,
        cache_keys: 4,
        run_for: Duration::from_secs(30),
        fault_window: Duration::from_secs(15),
        settle_within: Duration::from_secs(20),
        faults: false,
        ..SimConfig::for_seed(1)
    }
}

#[test]
fn a_run_without_faults_passes() {
    let outcome = run_seed(1, small());
    println!("{outcome}");
    assert!(outcome.passed());
    assert!(outcome.history.acknowledged() > 20, "{}", outcome.summary);
}

/// A key the search could not decide is neither proven nor refuted, so the
/// seed does not pass.
#[test]
fn an_undecided_cache_key_fails_the_seed() {
    let mut outcome = failed_start(1, small(), String::new(), Duration::ZERO);
    outcome.findings.clear();
    assert!(outcome.passed());
    outcome
        .report
        .undecided
        .push("cache key k: not decided after 20000000 steps".into());
    assert!(!outcome.passed());
    assert!(outcome.to_string().contains("cache key k"), "{outcome}");
}

#[test]
fn first_difference_names_the_first_event_that_differs() {
    let lines = |items: &[&str]| items.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let a = lines(&["fault x", "op 1", "op 2"]);
    assert_eq!(first_difference(&a, &a), None);
    let found = first_difference(&a, &lines(&["fault x", "op 1", "op 3"])).unwrap();
    assert!(
        found.contains("event 2") && found.contains("op 3"),
        "{found}"
    );
    let found = first_difference(&a, &lines(&["fault x"])).unwrap();
    assert!(
        found.contains("event 1") && found.contains("ends here"),
        "{found}"
    );
}

/// A replay must build with the seeded `select!`; without it the run under
/// the same faults can differ.
#[test]
fn the_replay_command_builds_with_a_seeded_select() {
    let outcome = failed_start(7, small(), String::new(), Duration::ZERO);
    assert_eq!(outcome.replay_command(), "FELIX_SIM_SEED=7 task sim:replay");
}

#[test]
fn the_replay_command_keeps_placement_faults_on() {
    let config = small().with_placement_faults();
    let outcome = failed_start(7, config, String::new(), Duration::ZERO);
    assert_eq!(
        outcome.replay_command(),
        "FELIX_SIM_PLACEMENT=1 FELIX_SIM_SEED=7 task sim:replay"
    );
}

#[test]
fn the_replay_command_keeps_power_loss_on() {
    let config = small().with_power_loss();
    let outcome = failed_start(7, config, String::new(), Duration::ZERO);
    assert_eq!(
        outcome.replay_command(),
        "FELIX_SIM_POWER_LOSS=1 FELIX_SIM_SEED=7 task sim:replay"
    );
}

#[test]
fn a_deadline_fires_only_when_it_is_not_dropped_in_time() {
    let (fired, seen) = std::sync::mpsc::channel();
    let kept = Deadline::start(Duration::from_millis(50), {
        let fired = fired.clone();
        move || fired.send("kept").unwrap()
    });
    let dropped = Deadline::start(Duration::from_millis(50), move || {
        fired.send("dropped").unwrap()
    });
    drop(dropped);
    assert_eq!(seen.recv_timeout(Duration::from_secs(10)), Ok("kept"));
    drop(kept);
    assert!(seen.recv_timeout(Duration::from_millis(200)).is_err());
}

/// Holds the simulation thread and never gives it back to the runtime.
struct HoldsTheThread;

#[async_trait::async_trait]
impl Checker for HoldsTheThread {
    fn name(&self) -> &'static str {
        "holds-the-thread"
    }

    async fn check(&mut self, _ctx: &CheckContext) -> Vec<Finding> {
        std::thread::sleep(Duration::from_secs(3600));
        Vec::new()
    }
}

const HOLDING_CHILD: &str = "FELIX_SIM_TEST_HOLDING_CHILD";

/// A run whose thread never returns to the runtime cannot see the stall
/// watch's cancel. It must end the process naming its seed, not hang it.
#[test]
fn a_run_that_holds_its_thread_ends_the_process_with_its_seed() {
    if std::env::var_os(HOLDING_CHILD).is_some() {
        // Long enough that a slow start (coverage, a loaded runner) reaches
        // the checker before the stall watch would end the run normally.
        let config = SimConfig {
            real_time_limit: Duration::from_secs(40),
            ..small()
        };
        run_seed_with(7, config, vec![Box::new(HoldsTheThread)]);
        return;
    }
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "run::tests::a_run_that_holds_its_thread_ends_the_process_with_its_seed",
            "--nocapture",
        ])
        .env(HOLDING_CHILD, "1")
        .env_remove("RUST_LOG")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("start the child run");
    let mut stderr = child.stderr.take().unwrap();
    let reader = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = std::io::Read::read_to_string(&mut stderr, &mut text);
        text
    });
    let begun = std::time::Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if begun.elapsed() > Duration::from_secs(240) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the run held its thread and the process did not end");
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let stderr = reader.join().unwrap();
    assert!(!status.success(), "{stderr}");
    assert!(stderr.contains("seed 7 ran past"), "{stderr}");
}
