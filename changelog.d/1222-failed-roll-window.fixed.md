- Truncation, reset and restore are refused on a log that has failed (#1222). A
  flush that failed to sync a retired segment marked the roll failed while its
  seal was still running, and a truncation could then cut that segment and
  report success before the seal padded it back with zeros.
