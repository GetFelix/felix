- The default `FELIX_BROKER_PUB_QUEUE_DEPTH` is now 512 (was 64), so the publish queue holds
  2048 jobs with the default executors instead of 256 (#1406). With many pipelining publishers the
  queue filled first, and a full queue stops the publishing connection from being read. With 64
  publisher connections and spread keys, 256 B unbatched acked publishes went from 95 to 163 MB/s
  (p50 12.6 to 7.5 ms), and 4 KiB batch-64 p99 fell from about 460-600 ms to about 360-380 ms;
  throughput at large batches is unchanged (about 4.7 GB/s). Set the variable to go back to a
  shallower queue.
