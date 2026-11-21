A formed Raft group's set now changes only by steps its reported voters keep
a majority of. A chain of one-member swaps that the group never applied
could end on brokers it never reached, which then formed a second group for
the shard. A leader's floor report carries the group's committed voters, and
the control plane refuses a set change those voters do not hold a majority
of. A voter counts only under the copy id it was reported with, so a voter
whose copy was wiped counts for nothing, and the same check holds the new
set's target. Voters count only from a leader in the stored target. A
replication factor 1 Raft shard's set does not move: to put it on another
broker, delete and recreate the stream (a hand-off is #2916). A shard can be
re-formed once until #2250. Needs metadata level 46. Until the finalize to level 46, formed Raft groups' sets do not move, so
placement replacements and drains of Raft shards wait for it.
