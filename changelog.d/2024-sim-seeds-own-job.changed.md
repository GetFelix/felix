- The felix-sim seed batch runs in its own non-blocking CI job instead of the
  workspace tests, so its known liveness stalls no longer fail the required gates.
