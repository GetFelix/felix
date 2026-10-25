/* Prepended to each fragment on docs-site/src/content/docs/clients/c.md that
   has no #include of its own: the headers the page says fragments leave out,
   and the minimal example's on_connect, which fragments reuse. */
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "felix.h"

void on_connect(void *user_data, FelixStatus status, const char *message, FelixClient *client);
