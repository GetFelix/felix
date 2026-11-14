- Frames the connection writer drops for a subscriber that is not keeping up now
  count as that subscriber's drops (#2531). They moved only
  `felix_sub_queue_dropped_total`, so `felix_subscribe_dropped_total`, the
  subscriber's dropped count in the subscriptions listing and the offset it
  resumes from after a lag all missed them. Each record is counted, not each frame.
