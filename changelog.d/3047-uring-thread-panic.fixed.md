- A flush through io_uring (`FELIX_STORAGE_IO_URING=1`) now poisons its log
  if the ring's thread dies before answering it (#3047). Before, the log's
  next flush could fall back to a plain fsync that succeeds after the lost
  one failed, and mark records durable that the kernel had dropped.
