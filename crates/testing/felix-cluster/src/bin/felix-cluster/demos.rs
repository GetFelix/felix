//! The narrated demos: `demo`, `failover` and `consistency`.
//!
//! Each prints its story one step at a time, pausing `--pace` seconds between
//! steps so it can be read, or narrated over, as it runs.

mod consistency;
mod failover;
mod replay;
mod tour;

pub(crate) use consistency::consistency;
pub(crate) use failover::failover;
pub(crate) use tour::demo;

use std::time::Duration;

fn step(title: &str) {
    println!("\n\x1b[1m── {title}\x1b[0m");
}

async fn beat(pace: Duration) {
    if !pace.is_zero() {
        tokio::time::sleep(pace).await;
    }
}
