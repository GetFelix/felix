- openraft moves to upstream commit `a8d0092bc8`, which adds conditional batch writes
  (`client_write_many` takes a `WriteOption`, openraft#2321). Felix passes the default option, so
  behaviour is unchanged.
