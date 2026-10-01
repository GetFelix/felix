//! Reading a broker's log back, for a failing test to quote.

use super::BrokerNode;

/// Lines per broker on a failure.
///
/// Enough to cover a shard reassignment and the replication pass after it,
/// which is the sequence most of these tests wait on; small enough that a suite
/// failing several cases is still readable.
pub(crate) const FAILURE_LOG_LINES: usize = 40;

/// What is worth printing when a cluster test fails.
///
/// The same set the redirect diagnostics already use, plus replication and
/// quorum: these tests wait on ownership moving and records arriving, so those
/// are the lines that say why the wait ended the way it did. Everything else a
/// broker logs at the end of a test is the harness shutting it down.
const FAILURE_LOG_TOPICS: &[&str] = &[
    "shard",
    "route",
    "assignment",
    "watch",
    "forward",
    "lease",
    "open",
    "replicat",
    "quorum",
    "halted",
];

impl BrokerNode {
    /// What this broker was doing, for a failing test to print.
    ///
    /// Filtered, not tailed. A raw tail is teardown: the last lines of every
    /// broker log are "connection lost" and a page-wide `ConnectionStats` dump
    /// from the harness killing it, which is noise in front of the answer.
    /// `log_lines_matching` falls back to a plain tail when none of the topics
    /// appear, so nothing is hidden — the absence of routing lines is itself
    /// evidence about what the broker was doing.
    pub(crate) fn failure_log(&self, take: usize) -> String {
        let label = &self.node_id;
        let lines = self.log_lines_matching(FAILURE_LOG_TOPICS, take);
        if lines.is_empty() {
            return format!("[{label}] no log");
        }
        format!("[{label}]\n  {}", lines.replace('\n', "\n  "))
    }

    /// The tail of this broker's log, for a start-up failure to quote.
    ///
    /// An exit status alone cannot distinguish a lost port from a refused
    /// credential, and those need opposite responses.
    pub(crate) fn failure_reason(&self) -> String {
        let Ok(log) = std::fs::read_to_string(self.data_dir.join("broker.log")) else {
            return String::new();
        };
        let tail: Vec<&str> = log
            .lines()
            .filter(|line| !line.trim().is_empty())
            .rev()
            .take(5)
            .collect();
        if tail.is_empty() {
            return String::new();
        }
        let mut lines = tail;
        lines.reverse();
        format!(":\n  {}", lines.join("\n  "))
    }

    /// Whether this broker exited because a port it was handed was taken.
    ///
    /// Ports are probed and released before the broker binds them, so another
    /// process can take one in between. That is the harness losing a race, not
    /// the broker failing to start.
    pub(crate) fn lost_port_race(&self) -> bool {
        std::fs::read_to_string(self.data_dir.join("broker.log"))
            .is_ok_and(|log| log.contains("Address already in use"))
    }

    /// The last log lines touching any of `topics`, for a failure to quote.
    ///
    /// Routing failures are diagnosed from what the brokers believed, and by
    /// the time a test fails the harness's tempdir is about to take the logs
    /// with it.
    pub(crate) fn log_lines_matching(&self, topics: &[&str], take: usize) -> String {
        let Ok(log) = std::fs::read_to_string(self.data_dir.join("broker.log")) else {
            return String::new();
        };
        let matching = |filtered: bool| -> Vec<&str> {
            let mut lines: Vec<&str> = log
                .lines()
                .filter(|line| !line.trim().is_empty())
                .filter(|line| {
                    if !filtered {
                        return true;
                    }
                    let lower = line.to_lowercase();
                    topics.iter().any(|topic| lower.contains(topic))
                })
                .rev()
                .take(take)
                .collect();
            lines.reverse();
            lines
        };
        // A tail with none of the topics still beats silence: the absence of
        // routing lines is itself evidence about what the broker was doing.
        let lines = match matching(true) {
            hits if !hits.is_empty() => hits,
            _ => matching(false),
        };
        lines.join("\n")
    }
}
