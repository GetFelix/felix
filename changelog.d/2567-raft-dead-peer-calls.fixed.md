- A Raft group with a member on a dead broker goes idle again: retrying the entry that member missed no longer keeps its leader heartbeating every 200 ms, and the retries wait up to 10 s while the broker is down (#2567).
  With 1,500 groups and one broker of three down, the calls to it drop from about 7,500 a second to about 900. The first answer from the broker still ends every wait.
