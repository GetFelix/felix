/* Built against a release archive's felix.h and linked both ways by
 * scripts/package_libfelix.sh. Proves the library loads and its symbols
 * resolve; no broker is involved. */
#include <stdio.h>

#include "felix.h"

int main(void) {
  /* A null callback is refused before any connection is attempted. */
  FelixStatus status = felix_client_connect("127.0.0.1:1", "tenant", "token", NULL, NULL, NULL, NULL, NULL);
  if (status != FELIX_STATUS_INVALID_ARGUMENT) {
    fprintf(stderr, "expected FELIX_STATUS_INVALID_ARGUMENT, got %d\n", (int)status);
    return 1;
  }
  const char *message = felix_last_error_message();
  if (message == NULL) {
    fprintf(stderr, "no error message after a refused connect\n");
    return 1;
  }
  printf("libfelix loaded: %s\n", message);
  return 0;
}
