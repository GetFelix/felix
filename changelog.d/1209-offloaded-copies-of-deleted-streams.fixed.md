- Deleting a stream now deletes its offloaded copies (#1209). With
  `FELIX_DURABLE_OFFLOAD_DIR` set, a broker retiring a deleted stream's shards
  removes every copy their manifests record, and anything else under the
  shard's prefix for that node, before it moves the local directories out of
  service. Copies left by a delete from an earlier release stay.
