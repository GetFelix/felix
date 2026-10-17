- The one-command `compose.yaml` stack now runs its broker with `FELIX_DURABLE_STORAGE_DIR`, so
  `orders` is a durable stream, consumer groups work, and cache watches work. It also creates the
  caches `config` and `stats`, and its development tokens can use caches, counters and groups.
  CI exercises these commands through the stack.
