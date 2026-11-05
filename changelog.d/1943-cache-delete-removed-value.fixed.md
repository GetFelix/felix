- A cache delete racing a put to the same key now reports the value it
  actually removed (#1943). It read the key before the put had applied, so it
  could answer with the value from before the put while the log, and every
  replica, deleted the put's value. No data was lost; only the delete's answer
  was wrong.
