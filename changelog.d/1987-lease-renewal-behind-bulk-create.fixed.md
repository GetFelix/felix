A lease renewal no longer waits seconds for its replication pass while a broker
creates shards in bulk. A shard with a fresh append now starts ahead of new
shards' first passes, and an append wake no longer reads the whole routing
table, which had taken about half of the replication task under load.
