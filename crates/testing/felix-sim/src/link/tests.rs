use super::*;

#[tokio::test(start_paused = true)]
async fn a_call_passes_while_the_link_is_up() {
    let cut = Cut::new();
    let began = Instant::now();
    let answer = cut.through(Duration::from_secs(5), async { Ok(7) }).await;
    assert_eq!(answer.unwrap(), 7);
    assert_eq!(began.elapsed(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn a_cut_call_fails_when_its_timeout_runs_out() {
    let cut = Cut::new();
    cut.set(true);
    let began = Instant::now();
    let answer = cut
        .through(Duration::from_secs(3), async { Ok::<_, anyhow::Error>(7) })
        .await;
    assert!(answer.unwrap_err().to_string().contains("timed out"));
    assert_eq!(began.elapsed(), Duration::from_secs(3));
}

#[tokio::test(start_paused = true)]
async fn a_call_in_flight_when_the_link_is_cut_is_lost() {
    let cut = Cut::new();
    let began = Instant::now();
    let flipper = cut.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        flipper.set(true);
    });
    let answer = cut
        .through(Duration::from_secs(4), async {
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok::<_, anyhow::Error>(7)
        })
        .await;
    assert!(answer.is_err());
    assert_eq!(began.elapsed(), Duration::from_secs(4));
}

#[tokio::test(start_paused = true)]
async fn a_call_slower_than_its_timeout_fails_even_when_the_link_is_up() {
    let cut = Cut::new();
    let answer = cut
        .through(Duration::from_secs(2), async {
            tokio::time::sleep(Duration::from_secs(9)).await;
            Ok::<_, anyhow::Error>(7)
        })
        .await;
    assert!(answer.is_err());
}
