- A leader that crashed under `periodic` or `none` and gave its shard up is
  replaced by a follower that holds at least what it kept, and named leader
  again only when every follower holds less (#1314). Its probe now reports a
  follower level with it as caught up, so the outcome no longer depends on
  whether the control plane sees the leader's new storage id or its probe
  first. A follower counts from the probe only when measured under the
  identity and incarnation it is registered at now.
