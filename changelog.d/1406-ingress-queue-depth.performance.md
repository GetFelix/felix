- The default `FELIX_BROKER_PUB_QUEUE_DEPTH` is now 1024 (was 64), so the publish queue holds
  4096 jobs with the default executors instead of 256 (#1406). With many pipelining publishers the
  queue filled first, and a full queue stops the publishing connection from being read. On an
  8 vCPU broker with 64 publisher connections, 256 B spread keys: 83 to 166 MB/s unbatched and
  1884 to 2674 MB/s at batch 64; with plain keys 143 to 150 and 2531 to 2646. Set the variable to
  go back to a shallower queue.
