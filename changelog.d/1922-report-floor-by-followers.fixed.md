A `Quorum` leader acknowledging by its followers (`majority_ack`, and `fenced_caches` for caches) no longer reports a follower as a promotion candidate before it holds the log the leader inherited. After the replica set changed, the next promotion could otherwise pick a broker that never held an acknowledged write and open without it. The same floor now applies to a cache's counter log, counted from the leader's current stint (#1922).

When the leader is lost and no live follower holds its log, placement now promotes a caught-up follower on a draining broker instead of leaving the shard without a leader; the drain then moves it off as usual.

A leader whose replica set changes now keeps naming a follower it has not reached yet by the storage identity it knew it by. Before, that follower was named with none, so after the leader died its fence answer never counted and a failover that needed it stayed closed.
