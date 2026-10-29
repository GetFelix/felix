- A client stream the broker has stopped reading no longer stalls the other streams on its
  connection (#1478). `pub_stream_recv_window` now defaults to a quarter of the connection
  window (4 MiB with the defaults) instead of the whole of it, so a stopped stream holds only
  its share of the connection's flow-control credit. The connection window still follows the
  publish budget (#923). A stream window below `max_frame_bytes` is now accepted, since the
  broker reads a frame as it arrives. Set `FELIX_BROKER_PUB_STREAM_RECV_WINDOW` to restore
  the old value.
