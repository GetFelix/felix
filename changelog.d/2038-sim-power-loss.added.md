- felix-sim can cut the power (#2038). With `FELIX_SIM_POWER_LOSS=1` (or
  `SimConfig::with_power_loss`) the brokers run with `FsyncMode::OnCommit` and
  a crash that would be a kill cuts the power to the broker instead. Off by
  default; a seed's faults stay where they were. The cut lands inside an append, which never comes
  back whole (its size and sectors reach the disk only in part), and every other
  unsynced byte may be lost or torn. The broker restarts on what the device
  held, and a new checker, `TornTailRepaired`, fails the seed unless recovery
  repaired the segment the cut tore. `felix_storage::sim_fs` gains
  `arm_power_cut`, `cut_power`, `reboot` and `torn_tails_repaired` for it.
