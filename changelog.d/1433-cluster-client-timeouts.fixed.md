- `finish()` and broker connects no longer hang a client forever when a broker stalls (#1433).
  `Publisher::finish` and `ClusterClient::finish` give up after `finish_timeout`
  (`FELIX_FINISH_TIMEOUT_MS`, default 60 s) and abort the streams, and each connect, including a
  cluster reconnect, fails after `connect_timeout` (`FELIX_CONNECT_TIMEOUT_MS`, default 10 s).
