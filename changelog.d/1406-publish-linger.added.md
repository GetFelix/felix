- A busy publish stream can wait up to `FELIX_PUB_LINGER_US` microseconds for more publishes before
  it writes, gathering up to `FELIX_PUB_LINGER_BYTES`. Off by default; an idle stream still writes
  at once (#1406).
