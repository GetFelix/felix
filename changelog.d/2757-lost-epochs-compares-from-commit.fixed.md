- A follower whose `epochs` file was missing, short or corrupt no longer keeps
  records past its commit offset uncompared when a newer leader takes over. It
  sends the leader back to the commit offset, as it does when the history
  names an older generation. Records that agree cost one resend; a divergent
  one stops replication on the shard for an operator instead of staying under
  the new leader's log (#2757).
