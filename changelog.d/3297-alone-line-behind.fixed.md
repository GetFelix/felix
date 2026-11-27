- Raft: an RF 1 shard re-forming itself after a restore, on a standalone broker or in a
  cluster, now waits until its time line clears the copy's last index, the offset floor and
  every earlier epoch's first index by 2^32, about four and a half minutes of clock. Before,
  it took the line from the broker's clock with no check, so a clock stepped back, or a
  directory restored onto a host whose clock was behind, could start the epoch below offsets
  an earlier epoch had acknowledged. The shard starts by itself once the wait is over, and
  the log says how long that is. Re-forms after a relaxed crash do not wait. `felixctl raft
  reform plan` now refuses an `--offset-gap` above 2^40 before sending, and a restore plan
  blocked for being too soon says how long to wait and what a short gap adds. (#3297)
