- The development stack no longer logs a "shard has no eligible leader" warning
  every five seconds for every shard (#1601). While no node has ever
  registered, as with a broker run without `FELIX_NODE_ID`, the control plane
  reports it at debug; it still warns when nodes exist but none can take a shard.
