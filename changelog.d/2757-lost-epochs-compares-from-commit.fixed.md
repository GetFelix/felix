- A follower whose `epochs` file was missing, short or corrupt no longer keeps
  records past its commit offset uncompared. It sends the leader back to the
  commit offset, whatever the leader's generation, as it does when the history
  names an older generation. Records that agree cost one resend; a divergent
  one is dropped when the leader rebuilds the follower from the commit offset,
  instead of staying under the leader's log. How far a resend has got is now
  tracked per log, so progress on a stream's group logs no longer lets a batch
  skip the stream's own uncompared records (#2757).
