//! What a seed's run is bounded by, and how two runs of it are compared.

#[cfg(feature = "raft")]
use std::sync::mpsc;
#[cfg(feature = "raft")]
use std::time::Duration;

/// Where two traces first differ, or `None` when they are the same.
pub fn first_difference(a: &[String], b: &[String]) -> Option<String> {
    let at = a
        .iter()
        .zip(b)
        .position(|(a, b)| a != b)
        .or((a.len() != b.len()).then(|| a.len().min(b.len())))?;
    let missing = "<the trace ends here>".to_string();
    Some(format!(
        "the traces first differ at event {at} of {} and {}:\n  first:  {}\n  second: {}",
        a.len(),
        b.len(),
        a.get(at).unwrap_or(&missing),
        b.get(at).unwrap_or(&missing),
    ))
}

/// Calls `expired` on a thread of its own unless it is dropped within `limit`
/// of real time.
#[cfg(feature = "raft")]
pub(crate) struct Deadline {
    _finished: mpsc::Sender<()>,
}

#[cfg(feature = "raft")]
impl Deadline {
    /// Ends the process with `message` unless dropped within `limit`.
    pub(crate) fn exits(limit: Duration, message: String) -> Self {
        Self::start(limit, move || {
            // Output that nobody drains blocks every writer, this message
            // too, so the exit does not wait on it.
            std::thread::spawn(|| {
                std::thread::sleep(Duration::from_secs(10));
                std::process::exit(1);
            });
            eprintln!("{message}");
            std::process::exit(1);
        })
    }

    fn start(limit: Duration, expired: impl FnOnce() + Send + 'static) -> Self {
        let (finished, wait) = mpsc::channel::<()>();
        std::thread::Builder::new()
            .name("sim-deadline".into())
            .spawn(move || {
                // A dropped sender disconnects the channel and ends the wait.
                if let Err(mpsc::RecvTimeoutError::Timeout) = wait.recv_timeout(limit) {
                    expired();
                }
            })
            .expect("start the run's deadline");
        Self {
            _finished: finished,
        }
    }
}

#[cfg(test)]
mod tests;
