use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use base64::Engine;

use super::*;

fn jwt(exp: i64) -> String {
    let encode = |value: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value);
    format!(
        "{}.{}.sig",
        encode(r#"{"alg":"EdDSA"}"#),
        encode(&format!(r#"{{"exp":{exp},"tid":"t1"}}"#))
    )
}

/// A provider whose fetches are numbered, so a test can tell them apart.
fn counting(
    make: impl Fn(usize) -> Result<String> + Send + Sync + 'static,
) -> (RefreshingToken, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let make = Arc::new(make);
    let provider = RefreshingToken::new(move || {
        let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
        let make = Arc::clone(&make);
        async move { make(n) }
    });
    (provider, calls)
}

#[test]
fn exp_is_read_from_a_jwt_and_nothing_else() {
    assert_eq!(read_exp(&jwt(1_900_000_000)), Some(1_900_000_000));
    assert_eq!(read_exp("demo-token"), None);
    assert_eq!(read_exp("a.b"), None);
    assert_eq!(read_exp("a.b.c.d"), None);
}

#[test]
fn refresh_is_due_two_thirds_through_the_remaining_life() {
    assert_eq!(refresh_at(1_000, 1_900), 1_600);
    // Already expired: due now.
    assert_eq!(refresh_at(1_000, 900), 1_000);
}

/// A token that is still valid but past the point where it should be
/// replaced.
fn due(token: String) -> Held {
    Held {
        token,
        expires_at: Some(now_secs() + 60),
        refresh_at: Some(now_secs() - 1),
    }
}

#[tokio::test]
async fn a_token_with_life_left_is_reused() -> Result<()> {
    let (provider, calls) = counting(|_| Ok(jwt(now_secs() + 900)));
    let first = provider.token().await?;
    let second = provider.token().await?;
    assert_eq!(first, second);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn a_token_past_two_thirds_of_its_life_is_replaced() -> Result<()> {
    let (provider, calls) = counting(|n| Ok(format!("{}-{n}", jwt(now_secs() + 900))));
    *provider.state.lock().unwrap() = Some(due(jwt(now_secs() + 60)));
    let token = provider.token().await?;
    assert!(
        token.ends_with("-1"),
        "expected a fetched token, got {token}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn a_failed_fetch_falls_back_to_a_token_that_is_still_valid() -> Result<()> {
    let (provider, calls) = counting(|_| Err(anyhow::anyhow!("control plane down")));
    let held = jwt(now_secs() + 60);
    *provider.state.lock().unwrap() = Some(due(held.clone()));
    assert_eq!(provider.token().await?, held);
    // And does not fetch again on the very next call.
    assert_eq!(provider.token().await?, held);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn a_failed_fetch_with_nothing_valid_held_is_an_error() {
    let (provider, _) = counting(|_| Err(anyhow::anyhow!("control plane down")));
    *provider.state.lock().unwrap() = Some(Held::new(jwt(now_secs() - 1)));
    assert!(provider.token().await.is_err());
}

#[tokio::test]
async fn invalidate_drops_only_the_token_it_names() -> Result<()> {
    let (provider, calls) = counting(|n| Ok(format!("opaque-{n}")));
    let first = provider.token().await?;
    provider.invalidate(&first);
    let second = provider.token().await?;
    assert_ne!(first, second);
    // A late report about the first token must not discard the second.
    provider.invalidate(&first);
    assert_eq!(provider.token().await?, second);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn an_initial_token_is_used_before_any_fetch() -> Result<()> {
    let provider =
        RefreshingToken::with_initial("seed", || async { anyhow::bail!("should not be called") });
    assert_eq!(provider.token().await?, "seed");
    Ok(())
}

/// A fetch that never answers fails after the timeout instead of hanging, and
/// a token that is still valid is kept.
#[tokio::test(start_paused = true)]
async fn a_fetch_that_never_answers_times_out_and_keeps_a_valid_token() -> Result<()> {
    let provider = RefreshingToken::new(std::future::pending::<Result<String>>);
    let held = jwt(now_secs() + 600);
    *provider.state.lock().unwrap() = Some(due(held.clone()));
    assert_eq!(provider.token().await?, held);

    let provider = RefreshingToken::new(std::future::pending::<Result<String>>);
    let err = provider.token().await.unwrap_err();
    assert!(format!("{err:#}").contains("took longer"), "{err:#}");
    Ok(())
}

/// A provider that records how many calls started and how many ran at once,
/// and answers only when told to.
struct Gated {
    calls: Arc<AtomicUsize>,
    running: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    open: Arc<tokio::sync::Notify>,
    cancels: Arc<Mutex<Vec<CancellationToken>>>,
    provider: RefreshingToken,
}

fn gated(answer: String) -> Gated {
    let calls = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let open = Arc::new(tokio::sync::Notify::new());
    let cancels = Arc::new(Mutex::new(Vec::new()));
    let (c, r, p, o, k) = (
        Arc::clone(&calls),
        Arc::clone(&running),
        Arc::clone(&peak),
        Arc::clone(&open),
        Arc::clone(&cancels),
    );
    let provider = RefreshingToken::new_cancellable(move |cancel| {
        c.fetch_add(1, Ordering::SeqCst);
        k.lock().unwrap().push(cancel);
        let (r, p, o, answer) = (
            Arc::clone(&r),
            Arc::clone(&p),
            Arc::clone(&o),
            answer.clone(),
        );
        async move {
            let now = r.fetch_add(1, Ordering::SeqCst) + 1;
            p.fetch_max(now, Ordering::SeqCst);
            o.notified().await;
            r.fetch_sub(1, Ordering::SeqCst);
            Ok(answer)
        }
    });
    Gated {
        calls,
        running,
        peak,
        open,
        cancels,
        provider,
    }
}

/// A call that outlives the timeout fails its waiter at the timeout, and the
/// next refresh joins it rather than calling the provider again.
#[tokio::test(start_paused = true)]
async fn a_hung_fetch_fails_the_waiter_and_is_joined_not_repeated() {
    let g = gated("late".into());
    let started = tokio::time::Instant::now();
    let err = g.provider.token().await.unwrap_err();
    assert!(format!("{err:#}").contains("took longer"), "{err:#}");
    assert_eq!(started.elapsed(), FETCH_TIMEOUT);

    let err = g.provider.token().await.unwrap_err();
    assert!(format!("{err:#}").contains("took longer"), "{err:#}");
    assert_eq!(started.elapsed(), FETCH_TIMEOUT * 2);
    assert_eq!(g.calls.load(Ordering::SeqCst), 1);
    assert_eq!(g.peak.load(Ordering::SeqCst), 1);
    assert_eq!(g.running.load(Ordering::SeqCst), 1);
}

/// The answer to a call whose waiters timed out is used, with no second call.
#[tokio::test(start_paused = true)]
async fn a_late_answer_is_used_without_a_second_call() -> Result<()> {
    let g = gated("late-token".into());
    assert!(g.provider.token().await.is_err());
    g.open.notify_one();
    tokio::task::yield_now().await;
    assert_eq!(g.provider.token().await?, "late-token");
    assert_eq!(g.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

/// A late answer is used even when nobody is waiting when it lands.
#[tokio::test(start_paused = true)]
async fn a_late_answer_joined_by_a_second_waiter_is_returned() -> Result<()> {
    let g = gated("joined".into());
    assert!(g.provider.token().await.is_err());
    let open = Arc::clone(&g.open);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        open.notify_one();
    });
    assert_eq!(g.provider.token().await?, "joined");
    assert_eq!(g.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn the_cancellation_token_fires_on_timeout() {
    let g = gated("x".into());
    assert!(g.provider.token().await.is_err());
    assert!(g.cancels.lock().unwrap()[0].is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn the_cancellation_token_fires_on_drop() {
    let g = gated("x".into());
    let waiter = tokio::spawn({
        let provider = Arc::new(g.provider);
        let held = Arc::clone(&provider);
        async move {
            let _ = held.token().await;
        }
    });
    tokio::task::yield_now().await;
    let cancel = g.cancels.lock().unwrap()[0].clone();
    assert!(!cancel.is_cancelled());
    waiter.abort();
    let _ = waiter.await;
    assert!(cancel.is_cancelled());
}
