//! Where openraft's random draws come from.
//!
//! openraft picks each election timeout from `AsyncRuntime::thread_rng()`.
//! Tokio's stock runtime answers with `rand::rng()`, which nothing can seed,
//! so two simulation runs of one seed would elect at different times.
//! [`ShardRuntime`] is that runtime with one change: while a thread holds a
//! [`TimerSeed`], its draws come from a stream derived from the seed.

use std::cell::Cell;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::time::Duration;

use openraft::rt::{AsyncRuntime, OptionalSend, Select2, Select3, Select4};
use openraft_rt_tokio::TokioRuntime;
use rand::SeedableRng;
use rand::rngs::SmallRng;

#[cfg(test)]
mod tests;

thread_local! {
    static STREAM: Cell<Option<u64>> = const { Cell::new(None) };
}

/// Makes every openraft draw on this thread follow `seed` until it drops.
///
/// For the simulator: its run is one thread, so the draws come in the
/// same order each time and one stream fixes them all. A broker never
/// holds one.
#[must_use = "the draws follow the seed only while this is held"]
pub struct TimerSeed {
    before: Option<u64>,
    // The stream is the thread's, so the guard must not move off it.
    _thread: PhantomData<*const ()>,
}

/// Seed openraft's draws on this thread. See [`TimerSeed`].
pub fn seed_timers(seed: u64) -> TimerSeed {
    TimerSeed {
        before: STREAM.replace(Some(seed)),
        _thread: PhantomData,
    }
}

impl Drop for TimerSeed {
    fn drop(&mut self) {
        STREAM.set(self.before);
    }
}

/// SplitMix64: each draw gets a well-mixed seed of its own.
fn next(state: u64) -> u64 {
    let z = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Tokio's runtime for openraft, with draws a simulation can seed.
#[derive(Debug)]
pub(crate) struct ShardRuntime(TokioRuntime);

impl AsyncRuntime for ShardRuntime {
    type JoinError = <TokioRuntime as AsyncRuntime>::JoinError;
    type JoinHandle<T: OptionalSend + 'static> = <TokioRuntime as AsyncRuntime>::JoinHandle<T>;
    type Sleep = <TokioRuntime as AsyncRuntime>::Sleep;
    type Instant = <TokioRuntime as AsyncRuntime>::Instant;
    type TimeoutError = <TokioRuntime as AsyncRuntime>::TimeoutError;
    type Timeout<R, T: Future<Output = R> + OptionalSend> =
        <TokioRuntime as AsyncRuntime>::Timeout<R, T>;
    type ThreadLocalRng = SmallRng;
    type Mpsc = <TokioRuntime as AsyncRuntime>::Mpsc;
    type Watch = <TokioRuntime as AsyncRuntime>::Watch;
    type Oneshot = <TokioRuntime as AsyncRuntime>::Oneshot;
    type Mutex<T: OptionalSend + 'static> = <TokioRuntime as AsyncRuntime>::Mutex<T>;

    fn spawn<T>(future: T) -> Self::JoinHandle<T::Output>
    where
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static,
    {
        TokioRuntime::spawn(future)
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        TokioRuntime::sleep(duration)
    }

    fn sleep_until(deadline: Self::Instant) -> Self::Sleep {
        TokioRuntime::sleep_until(deadline)
    }

    fn timeout<R, F>(duration: Duration, future: F) -> Self::Timeout<R, F>
    where
        F: Future<Output = R> + OptionalSend,
    {
        TokioRuntime::timeout(duration, future)
    }

    fn timeout_at<R, F>(deadline: Self::Instant, future: F) -> Self::Timeout<R, F>
    where
        F: Future<Output = R> + OptionalSend,
    {
        TokioRuntime::timeout_at(deadline, future)
    }

    fn select2<A, B>(
        a: A,
        b: B,
    ) -> impl Future<Output = Select2<A::Output, B::Output>> + OptionalSend
    where
        A: Future + OptionalSend,
        B: Future + OptionalSend,
    {
        TokioRuntime::select2(a, b)
    }

    fn select3<A, B, C>(
        a: A,
        b: B,
        c: C,
    ) -> impl Future<Output = Select3<A::Output, B::Output, C::Output>> + OptionalSend
    where
        A: Future + OptionalSend,
        B: Future + OptionalSend,
        C: Future + OptionalSend,
    {
        TokioRuntime::select3(a, b, c)
    }

    fn select4<A, B, C, D>(
        a: A,
        b: B,
        c: C,
        d: D,
    ) -> impl Future<Output = Select4<A::Output, B::Output, C::Output, D::Output>> + OptionalSend
    where
        A: Future + OptionalSend,
        B: Future + OptionalSend,
        C: Future + OptionalSend,
        D: Future + OptionalSend,
    {
        TokioRuntime::select4(a, b, c, d)
    }

    fn is_panic(join_error: &Self::JoinError) -> bool {
        TokioRuntime::is_panic(join_error)
    }

    fn thread_rng() -> Self::ThreadLocalRng {
        let seed = STREAM.with(|stream| {
            let state = stream.get()?;
            stream.set(Some(next(state)));
            Some(state)
        });
        SmallRng::seed_from_u64(seed.unwrap_or_else(rand::random))
    }

    fn new(threads: usize) -> Self {
        Self(TokioRuntime::new(threads))
    }

    fn block_on<F, T>(&mut self, future: F) -> T
    where
        F: Future<Output = T>,
    {
        self.0.block_on(future)
    }

    fn spawn_blocking<F, T>(f: F) -> impl Future<Output = Result<T, io::Error>> + Send
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        TokioRuntime::spawn_blocking(f)
    }
}
