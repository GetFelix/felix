- A new cluster finalizes `stream_incarnations` and `cache_incarnations` at its first broker
  registration, so a stream or cache deleted and created again there always starts empty on every
  broker (#1447). The control plane treats a cluster as new when no stream or cache was ever
  created and no broker has registered twice, and finalizes through the same checks as an
  operator's finalize, so a broker without the features keeps them off or is refused afterwards.
  A cluster upgraded from an older release still waits for the operator to finalize.
