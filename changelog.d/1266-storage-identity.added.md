- Each broker data directory now has a storage identity (#1266): 16 random
  bytes kept in `<FELIX_DURABLE_STORAGE_DIR>/storage-id`, created on the first
  start, kept across restarts, and replaced when the file is missing or does
  not decode. A broker sends it as `storage_id` with its registration; a
  broker without durable storage sends a new one each start.
  `felix-broker restore-point` writes a new one.
