- The felix-sim seed batch runs in its own non-blocking CI job instead of the
  workspace tests, so a long seed batch does not hold up the required gates.
