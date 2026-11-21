A formed Raft group's set now changes only by steps its reported voters keep
a majority of. A chain of one-member swaps that the group never applied
could end on brokers it never reached, which then formed a second group for
the shard. A leader's floor report carries the group's committed voters, and
the control plane refuses a set change those voters do not hold a majority
of. A replication factor 1 move goes through a two-broker set. Needs metadata
level 46.
