- A broker cut off from the control plane before it heard the fleet finalize
  `lease_free_reads` no longer confirms a `Quorum` read on its lease alone
  (#2161). Its replicas could elect a successor without waiting out that
  lease, so the read could miss the successor's writes. Until a broker hears
  of the finalize, a read there needs both the lease and a round of fences.
