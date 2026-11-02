- The broker-wide byte budgets (`FELIX_SUB_QUEUE_MAX_BYTES_TOTAL` and the
  replay ring and cache watch totals) stay off by default (#1338). Measured on a
  CI runner they cost at most a few percent of throughput, but no single size
  suits every machine, so the configuration reference now says how to size one.
  The manual `perf-env-ab.yml` workflow runs the fast benchmark subsets with
  and without any environment setting, to price a default like this one.
