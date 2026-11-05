//! Log lines written to stdout from a thread of their own.
//!
//! When stdout is a file, writing a line can wait on the filesystem journal
//! for a second or more under heavy writeback. Written on a runtime worker,
//! that wait stalls every task queued behind it, the lease heartbeat
//! included. Here the worker hands the line to a bounded queue and goes on.
//!
//! No line is dropped: a full queue blocks the logging thread until the
//! writer catches up. [`flush`] waits for every line queued before it, and
//! runs on a clean exit and from the panic hook. Only a process killed
//! outright (SIGKILL, OOM kill) loses the lines still queued.

use std::io::Write;
use std::sync::OnceLock;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::ThreadId;
use std::time::Duration;

/// Lines held before a logging thread waits for the writer.
const QUEUED_LINES: usize = 16_384;

/// How long the panic hook waits for the queue to drain. A writer stuck on a
/// dead disk must not keep a panicking process from exiting.
const PANIC_FLUSH: Duration = Duration::from_secs(5);

enum Message {
    Line(Vec<u8>),
    Flush(SyncSender<()>),
}

/// The process's writer, set once by [`install`].
static STDOUT: OnceLock<LogWriter> = OnceLock::new();

/// A `MakeWriter` whose lines reach the sink on the writer's own thread.
#[derive(Clone)]
pub(crate) struct LogWriter {
    queue: SyncSender<Message>,
    thread: ThreadId,
}

impl LogWriter {
    /// Start a writer thread draining into `sink`.
    pub(crate) fn spawn(sink: impl Write + Send + 'static) -> std::io::Result<Self> {
        let (queue, lines) = sync_channel(QUEUED_LINES);
        let thread = std::thread::Builder::new()
            .name("felix-log-writer".into())
            .spawn(move || drain(lines, sink))?
            .thread()
            .id();
        Ok(Self { queue, thread })
    }

    /// Wait until every line queued before this call is in the sink and
    /// flushed, or `limit` passes. Returns whether it finished. A no-op on
    /// the writer's own thread, which would otherwise wait on itself.
    pub(crate) fn flush(&self, limit: Option<Duration>) -> bool {
        if std::thread::current().id() == self.thread {
            return false;
        }
        let (done, flushed) = sync_channel(1);
        if self.queue.send(Message::Flush(done)).is_err() {
            return false;
        }
        match limit {
            Some(limit) => flushed.recv_timeout(limit).is_ok(),
            None => flushed.recv().is_ok(),
        }
    }
}

impl Write for LogWriter {
    fn write(&mut self, line: &[u8]) -> std::io::Result<usize> {
        self.queue
            .send(Message::Line(line.to_vec()))
            .map_err(|_| std::io::Error::other("the log writer has stopped"))?;
        Ok(line.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogWriter {
    type Writer = LogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Start the process's stdout writer, and a panic hook that flushes it
/// before the panic is reported. Call once.
pub(crate) fn install() -> LogWriter {
    let writer = STDOUT
        .get_or_init(|| LogWriter::spawn(std::io::stdout()).expect("start the log writer thread"));
    std::panic::set_hook(flushing_hook(writer.clone(), std::panic::take_hook()));
    writer.clone()
}

type PanicHook = Box<dyn Fn(&std::panic::PanicHookInfo<'_>) + Send + Sync + 'static>;

/// `previous`, run once `writer` has written out what was logged before the
/// panic, so the report follows the lines that led to it.
fn flushing_hook(writer: LogWriter, previous: PanicHook) -> PanicHook {
    Box::new(move |info| {
        writer.flush(Some(PANIC_FLUSH));
        previous(info);
    })
}

/// Write out every log line queued so far. Call before the process exits:
/// the writer thread does not outlive `main`.
pub fn flush() {
    if let Some(writer) = STDOUT.get() {
        writer.flush(None);
    }
}

/// The writer thread: write each line, and flush the sink whenever the queue
/// runs dry so a line shows up as soon as nothing is behind it.
fn drain(lines: Receiver<Message>, sink: impl Write) {
    let mut sink = std::io::BufWriter::new(sink);
    while let Ok(message) = lines.recv() {
        let mut next = Some(message);
        while let Some(message) = next.take() {
            match message {
                // A failed write has nowhere to be reported but the log.
                Message::Line(line) => {
                    let _ = sink.write_all(&line);
                }
                Message::Flush(done) => {
                    let _ = sink.flush();
                    let _ = done.send(());
                }
            }
            next = lines.try_recv().ok();
        }
        let _ = sink.flush();
    }
    let _ = sink.flush();
}

#[cfg(test)]
mod tests;
