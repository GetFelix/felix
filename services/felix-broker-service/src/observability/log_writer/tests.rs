use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::*;

/// A sink that takes `delay` per write and keeps what it was given.
#[derive(Clone, Default)]
struct SlowSink {
    written: Arc<Mutex<Vec<u8>>>,
    delay: Duration,
}

impl Write for SlowSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        std::thread::sleep(self.delay);
        self.written.lock().expect("sink").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn line(n: usize) -> String {
    format!("line {n}\n")
}

/// **Logging does not wait for the disk**, and once flushed every line is
/// there, in order. The sink here takes as long per write as a stalled
/// journal; the lines are queued in far less.
#[test]
fn lines_are_queued_without_waiting_and_all_arrive_in_order() {
    let sink = SlowSink {
        delay: Duration::from_millis(200),
        ..SlowSink::default()
    };
    let mut writer = LogWriter::spawn(sink.clone()).expect("writer");

    let started = Instant::now();
    for n in 0..1_000 {
        writer.write_all(line(n).as_bytes()).expect("write");
    }
    let queued = started.elapsed();
    assert!(
        queued < Duration::from_millis(100),
        "logging waited {queued:?} on the sink"
    );

    assert!(writer.flush(None), "flush finished");
    let expected: String = (0..1_000).map(line).collect();
    let written = sink.written.lock().expect("sink").clone();
    assert_eq!(String::from_utf8(written).expect("utf8"), expected);
}

/// **A full queue holds the logger rather than dropping a line.**
#[test]
fn a_full_queue_waits_instead_of_dropping() {
    let sink = SlowSink {
        delay: Duration::from_micros(20),
        ..SlowSink::default()
    };
    let mut writer = LogWriter::spawn(sink.clone()).expect("writer");
    let lines = QUEUED_LINES * 3;
    for n in 0..lines {
        writer.write_all(line(n).as_bytes()).expect("write");
    }
    assert!(writer.flush(None), "flush finished");
    let written = sink.written.lock().expect("sink").clone();
    let written = String::from_utf8(written).expect("utf8");
    assert_eq!(written.lines().count(), lines);
    assert_eq!(written.lines().last(), Some(line(lines - 1).trim_end()));
}

/// **A panic report comes after the lines logged before it.** The hook
/// flushes before the previous hook prints the panic, so a broker that panics
/// leaves its last lines in its log.
#[test]
fn a_panic_flushes_the_queue_before_it_is_reported() {
    let sink = SlowSink {
        delay: Duration::from_millis(20),
        ..SlowSink::default()
    };
    let writer = LogWriter::spawn(sink.clone()).expect("writer");
    let reported = Arc::new(Mutex::new(None));
    let report: PanicHook = Box::new({
        let sink = sink.clone();
        let reported = Arc::clone(&reported);
        move |_| {
            // Only this test's panic: the hook is process-wide.
            if std::thread::current().name() == Some("panics-after-logging") {
                *reported.lock().expect("reported") =
                    Some(sink.written.lock().expect("sink").len());
            }
        }
    });
    let previous = std::panic::take_hook();
    std::panic::set_hook(flushing_hook(writer.clone(), report));
    let mut logging = writer.clone();
    let panicked = std::thread::Builder::new()
        .name("panics-after-logging".into())
        .spawn(move || {
            for n in 0..10 {
                logging.write_all(line(n).as_bytes()).expect("write");
            }
            panic!("boom");
        })
        .expect("spawn")
        .join();
    std::panic::set_hook(previous);

    assert!(panicked.is_err(), "the thread panicked");
    let expected: usize = (0..10).map(|n| line(n).len()).sum();
    assert_eq!(*reported.lock().expect("reported"), Some(expected));
}

/// The writer's own thread cannot wait on itself.
#[test]
fn flushing_from_the_writer_thread_returns() {
    struct FlushingSink(Arc<Mutex<Option<LogWriter>>>, Arc<Mutex<Option<bool>>>);
    impl Write for FlushingSink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if let Some(writer) = self.0.lock().expect("writer").as_ref() {
                *self.1.lock().expect("result") = Some(writer.flush(None));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let slot = Arc::new(Mutex::new(None));
    let result = Arc::new(Mutex::new(None));
    let mut writer =
        LogWriter::spawn(FlushingSink(Arc::clone(&slot), Arc::clone(&result))).expect("writer");
    *slot.lock().expect("slot") = Some(writer.clone());
    writer.write_all(b"x\n").expect("write");
    assert!(writer.flush(Some(Duration::from_secs(5))), "flush finished");
    assert_eq!(*result.lock().expect("result"), Some(false));
}

/// **A panic flush gives up on time behind a stuck sink**, even when the
/// queue is full and the flush request itself cannot be queued.
#[test]
fn a_bounded_flush_returns_when_the_queue_is_full_behind_a_stuck_sink() {
    let (release, stuck) = std::sync::mpsc::channel::<()>();
    struct StuckSink(Mutex<std::sync::mpsc::Receiver<()>>);
    impl Write for StuckSink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let _ = self.0.lock().expect("stuck").recv();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = LogWriter::spawn_queuing(StuckSink(Mutex::new(stuck)), 1).expect("writer");
    // A line past the sink's buffer reaches the stuck write; the next fills
    // the queue.
    writer.write_all(&[b'x'; 64 * 1024]).expect("write");
    writer.write_all(line(1).as_bytes()).expect("write");
    let (finished, returned) = std::sync::mpsc::channel();
    std::thread::spawn({
        let writer = writer.clone();
        move || {
            let _ = finished.send(writer.flush(Some(Duration::from_millis(100))));
        }
    });
    let flushed = returned.recv_timeout(Duration::from_secs(5));
    // Unstick the sink either way, so the writer thread can finish.
    drop(release);
    assert_eq!(flushed, Ok(false), "the flush waited past its limit");
}
