- Placement fails a `Quorum` stream over on its followers' word only once
  `generation_start`, `majority_ack` and `lease_free_reads` are all finalized,
  which is what brokers need to acknowledge and read without the lease
  (#1247). Before, a fleet with the last two but not `generation_start` could
  have a live leader replaced while it still served reads on its lease. The
  control plane now refuses with 409 to finalize `majority_ack` before
  `generation_start`, and `lease_free_reads` or `fenced_caches` before both;
  the finalize dry run lists what is missing as `requires`. A fleet that
  already finalized them out of order fails over on the lease until
  `generation_start` is finalized.
