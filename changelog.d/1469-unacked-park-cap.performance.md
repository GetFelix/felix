- Fire-and-forget publishes of small batches under `pub_ingress_wait` are back to their v0.6.0
  campaign throughput (#1469). A control stream now parks at most 16 unacked publishes while it
  waits for queue room; past that it waits inline. With the acked cap of 1024, a busy broker's
  streams parked until the process-wide byte budget ran out, and every publish then waited on
  that budget: 256 B batch-64 fire-and-forget lost 13%.
  The trade-off: a fire-and-forget stream to one full shard stops reading after 16 frames
  instead of after the byte budget runs out, and with the default stream window it can then
  hold the connection's flow-control credit and stall the connection's other streams.
