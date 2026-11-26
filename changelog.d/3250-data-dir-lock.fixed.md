- A broker now locks its data directory for as long as it runs, before it
  reads or writes anything there. A second broker pointed at the same
  directory stops at startup, before it is ready, with an error naming the
  directory, instead of writing its run record and possibly rotating the
  storage id under the first. `felix-broker restore-point` and
  `felix-broker rotate-storage-id` take the same lock, so they refuse to run
  while a broker holds the directory. The broker lets the lock go only once
  it has stopped every task that writes there and shut storage down, after
  the drain or after a start that failed once storage was open, such as one
  whose listener port was taken. The kernel lets it go when the process
  dies, so a crash leaves nothing to clean up and a broker restarted in the
  same process starts (#3250).
