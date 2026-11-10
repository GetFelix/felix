- A broker no longer holds every histogram sample until its metrics endpoint is
  scraped (#2152). The recorder kept one value per `felix_sub_conn_queue_depth`
  record, so resident memory climbed with each connection's traffic and the weekly
  soak failed its peak-RSS check. The broker now folds samples into summaries every
  five seconds, scraped or not.
  The control plane's recorder gets the same timer.
