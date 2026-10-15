# Tools

Command-line tools for people working with a Felix cluster.

- [`felixctl`](felixctl) publishes, subscribes, reads and watches caches, shows
  where shards live, lists what the control plane knows, and runs benchmarks.
  Its data-plane commands use only `felix-client`'s public API, so it doubles
  as a check that the API is enough to build tools on.
