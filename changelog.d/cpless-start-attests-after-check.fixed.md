- With the unified fence on, a broker started from its control-plane view while the control plane was down now
  attests its copies once the control plane answers and the restore check passes. Before, it attested nothing for
  the rest of the process, so a shard whose leader died could not elect a new one without it.
