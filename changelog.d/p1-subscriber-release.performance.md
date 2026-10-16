- A subscription's feeder gives a batch back to `FELIX_SUB_QUEUE_MAX_BYTES` once
  its frame is in the writer lanes, not before encoding it. The earlier release
  sat on the delivery path and cost single-publisher streams 1-2% throughput
  against 0.6.0-preview.7.
