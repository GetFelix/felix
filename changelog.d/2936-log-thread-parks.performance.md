- A log's append thread no longer polls for 20 µs after every append on a
  quiet log: it polls only while appends keep arriving within that window,
  and a flush with no append queued no longer wakes the thread at all. This
  cuts the CPU a lone publisher's append costs on both engines (#2936).
