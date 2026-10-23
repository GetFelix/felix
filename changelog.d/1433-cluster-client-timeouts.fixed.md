- `finish()` and broker connects no longer hang a client forever when a broker stalls (#1433).
  `Publisher::finish` and `ClusterClient::finish` give up after `finish_timeout`
  (`FELIX_FINISH_TIMEOUT_MS`, default 60 s) and abort the streams, and each connect, including a
  cluster reconnect, fails after `connect_timeout` (`FELIX_CONNECT_TIMEOUT_MS`, default 10 s).
  A cluster reconnect now dials outside the client lock and within three times `connect_timeout`,
  `ClusterClient::finish` finishes brokers concurrently, and the topology, shard-count and subscribe
  exchanges give up after `connect_timeout`.
