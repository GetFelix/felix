- A second failover soon after the first no longer leaves the shard closed
  (#1574). A leader promoted by a failover that dies before it has reached a
  follower left that follower named by no storage identity, so the next
  promoted leader could not count its fence answer and waited for good when it
  needed it for a majority. A report now carries, for each follower its leader
  has not reached, the identity the leader's own promotion named it by, and
  the next failover names the follower by that. A Postgres control plane runs
  migration `0037` on start; under Raft the field waits for metadata level 20.
