- felix-sim cuts the power (#2038). One seed in three runs its brokers with
  `FsyncMode::OnCommit`, and in those runs a crash that would be a kill cuts the
  power to the broker instead. The cut lands inside an append, which never comes
  back whole (its size and sectors reach the disk only in part), and every other
  unsynced byte may be lost or torn. The broker restarts on what the device
  held, and a new checker, `TornTailRepaired`, fails the seed unless recovery
  repaired the segment the cut tore. `felix_storage::sim_fs` gains
  `arm_power_cut`, `cut_power`, `reboot` and `torn_tails_repaired` for it.
