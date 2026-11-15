- A Raft group simulation seed now replays its run event for event when built
  with `--cfg tokio_unstable` (#2449). openraft's election timeouts come from
  a stream the simulation seeds, the node ticker visits groups in shard order,
  and storage's blocking work runs on the simulation thread. A broker still
  draws its timeouts at random.
