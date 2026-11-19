//! The services' default test log filter keeps openraft's chatter out.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::{EnvFilter, Layer};

struct Count(Arc<AtomicUsize>);

impl<S: tracing::Subscriber> Layer<S> for Count {
    fn on_event(&self, _: &tracing::Event<'_>, _: Context<'_, S>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn quiet_test_log_drops_openraft_ticks_and_heartbeats() {
    let seen = Arc::new(AtomicUsize::new(0));
    let subscriber = tracing_subscriber::registry().with(
        Count(seen.clone()).with_filter(EnvFilter::new(felix_common::log_filter::QUIET_TEST_LOG)),
    );
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(target: "openraft::core::raft_core", "tick");
        tracing::warn!(target: "openraft::core::heartbeat::worker", "heartbeat");
        tracing::info!(target: "felix_broker_service", "chatty");
        assert_eq!(seen.load(Ordering::SeqCst), 0);
        tracing::warn!(target: "felix_broker_service", "real warning");
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    });
}
