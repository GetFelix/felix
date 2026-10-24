- The C ABI (`crates/sdk/felix-capi`) runs atomic commits, conditional publishes and state reads (#1493):
  `felix_client_commit`, `_commit_on_shard`, `_commit_if` and `_commit_if_on_shard`, `felix_client_publish_if` and
  `_publish_if_on_shard`, and `felix_client_state_get` and `_state_get_on_shard`. A commit that does not hold exactly
  one event on one stream is refused at the call. A new status, `FELIX_STATUS_UNSUPPORTED`, reports a broker that
  does not serve commits.
