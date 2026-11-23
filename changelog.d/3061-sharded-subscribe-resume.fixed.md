- A sharded subscription that loses a shard's feed before the shard delivered
  anything now resumes at the offset it started from, not at the tail. Records
  written while the shard was being re-established are no longer skipped (#3061).
