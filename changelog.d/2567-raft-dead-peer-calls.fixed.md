- A Raft group with a member on a dead broker goes idle again: retrying the entry that member missed no longer keeps its leader heartbeating every 200 ms, and a stream shard's retries wait up to 10 s while the broker is down (#2567).
  With one broker of three down, the calls to it drop from about five a second per group to about 0.6. The first answer from the broker still ends every wait.
