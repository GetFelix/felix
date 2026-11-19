- A follower whose `epochs` file was missing, short or corrupt no longer keeps
  records past its commit offset uncompared. It sends the leader back to the
  commit offset, whatever the leader's generation, as it does when the history
  names an older generation. Records that agree cost one resend; a divergent
  one is refused, and the leader's rebuild of the follower (when its rebuild
  limits allow) drops it instead of leaving it under the leader's log. How far a resend has got is now
  tracked per log, so progress on a stream's group logs, or a cache's counters,
  no longer lets a batch skip the other log's uncompared records (#2757).
