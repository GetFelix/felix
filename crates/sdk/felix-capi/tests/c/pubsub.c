/*
 * Drives the C ABI the way a C, Go or C# caller would: through the header and
 * a linked library, with no Rust in sight.
 *
 *   pubsub offline
 *     Argument checking, the last-error channel and a failed connect. Needs
 *     no broker.
 *
 *   pubsub fixture ADDRS TENANT NAMESPACE TOKEN CA_FILE STREAM MISSING UNAUTHORIZED_TOKEN
 *     The publish and subscribe scenarios, against `felix-cluster
 *     client-fixture`. tests/c/run.sh starts the fixture and passes its
 *     fields in.
 */
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "felix.h"

#define WAIT_MS 20000

static int failures = 0;

#define CHECK(cond, ...)                                                       \
  do {                                                                         \
    if (!(cond)) {                                                             \
      fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__);                     \
      fprintf(stderr, __VA_ARGS__);                                            \
      const char *last = felix_last_error_message();                           \
      fprintf(stderr, " (last error: %s)\n", last ? last : "none");            \
      failures++;                                                              \
    }                                                                          \
  } while (0)

/* A status check that stops the scenario: the rest would only cascade. */
#define REQUIRE_OK(call)                                                       \
  do {                                                                         \
    FelixStatus status_ = (call);                                              \
    if (status_ != FELIX_STATUS_OK) {                                          \
      CHECK(0, "%s returned %d", #call, (int)status_);                         \
      return;                                                                  \
    }                                                                          \
  } while (0)

struct fixture {
  const char *addrs, *tenant, *ns, *token, *ca_file, *stream, *missing,
      *unauthorized_token;
};

static void offline(void) {
  FelixClient *client = (FelixClient *)0x1;

  /* Null arguments are refused, named, and leave the out-pointer alone. */
  FelixStatus status =
      felix_client_connect(NULL, "t1", "token", NULL, NULL, &client);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null addrs gave %d",
        (int)status);
  const char *message = felix_last_error_message();
  CHECK(message && strstr(message, "addrs"), "message does not name addrs");
  CHECK(client == (FelixClient *)0x1, "out_client written on failure");

  status = felix_client_connect("127.0.0.1:1", "t1", "token", NULL, NULL, NULL);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null out_client gave %d",
        (int)status);

  status = felix_client_connect("", "t1", "token", NULL, NULL, &client);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "empty addrs gave %d",
        (int)status);

  status = felix_client_connect("127.0.0.1:1", "t1", "token", NULL,
                                "/nonexistent/ca.pem", &client);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "missing ca_file gave %d",
        (int)status);

  status = felix_client_publish(NULL, "t", "n", "s", NULL, 0,
                                FELIX_ACK_PER_MESSAGE, NULL, NULL);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null client gave %d",
        (int)status);

  FelixEvent *event = NULL;
  status = felix_subscription_next_event(NULL, 0, &event);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT && event == NULL,
        "null subscription gave %d", (int)status);

  /* Every free takes null. */
  felix_client_free(NULL);
  felix_subscription_free(NULL);
  felix_event_free(NULL);

  /* Nothing listens on port 1: a connection failure, not a crash. The
     platform trust store is enough, since no handshake starts. */
  status = felix_client_connect("127.0.0.1:1", "t1", "token", NULL, NULL,
                                &client);
  CHECK(status != FELIX_STATUS_OK && status != FELIX_STATUS_PANIC,
        "connecting to a closed port gave %d", (int)status);
  CHECK(felix_last_error_message() != NULL, "no message for a failed connect");
}

static FelixStatus publish(FelixClient *client, const struct fixture *f,
                           const char *stream, const char *payload,
                           uint64_t *offset, bool *has_offset) {
  return felix_client_publish(client, f->tenant, f->ns, stream,
                              (const uint8_t *)payload, strlen(payload),
                              FELIX_ACK_PER_MESSAGE, offset, has_offset);
}

/* Poll until an event with exactly `expected` arrives. Records from other
   runs may share the stream, so others are skipped. */
static int expect_payload(FelixSubscription *sub, const char *expected,
                          uint64_t *offset) {
  for (;;) {
    FelixEvent *event = NULL;
    FelixStatus status = felix_subscription_next_event(sub, WAIT_MS, &event);
    if (status != FELIX_STATUS_OK) {
      CHECK(0, "waiting for %s: status %d", expected, (int)status);
      return 0;
    }
    const uint8_t *data = NULL;
    size_t len = 0;
    bool has_offset = false;
    felix_event_payload(event, &data, &len);
    felix_event_offset(event, offset, &has_offset);
    int match = len == strlen(expected) && memcmp(data, expected, len) == 0;
    felix_event_free(event);
    if (match) {
      CHECK(has_offset, "a durable stream delivered %s without an offset",
            expected);
      return 1;
    }
  }
}

static void pubsub(const struct fixture *f, const char *run) {
  FelixClient *client = NULL;
  REQUIRE_OK(felix_client_connect(f->addrs, f->tenant, f->token, NULL,
                                  f->ca_file, &client));

  /* pubsub.roundtrip, pubsub.order, pubsub.offsets_are_contiguous */
  FelixSubscription *sub = NULL;
  FelixStatus status = felix_client_subscribe(
      client, f->tenant, f->ns, f->stream, FELIX_START_LATEST, 0, &sub);
  if (status != FELIX_STATUS_OK) {
    CHECK(0, "subscribe gave %d", (int)status);
    felix_client_free(client);
    return;
  }

  /* Nothing has been published since the subscription started. */
  FelixEvent *event = NULL;
  status = felix_subscription_next_event(sub, 200, &event);
  CHECK(status == FELIX_STATUS_TIMEOUT && event == NULL,
        "an idle poll gave %d", (int)status);

  enum { COUNT = 10 };
  char payloads[COUNT][64];
  for (int i = 0; i < COUNT; i++) {
    snprintf(payloads[i], sizeof payloads[i], "c-%s-%d", run, i);
    uint64_t offset = 0;
    bool has_offset = false;
    status = publish(client, f, f->stream, payloads[i], &offset, &has_offset);
    CHECK(status == FELIX_STATUS_OK, "publish %d gave %d", i, (int)status);
  }

  uint64_t previous = 0, last = 0;
  for (int i = 0; i < COUNT; i++) {
    uint64_t offset = 0;
    if (!expect_payload(sub, payloads[i], &offset)) {
      break;
    }
    if (i > 0) {
      CHECK(offset == previous + 1, "offsets jumped: %" PRIu64 " then %" PRIu64,
            previous, offset);
    }
    previous = last = offset;
  }
  felix_subscription_free(sub);

  /* pubsub.resume_from_offset: published while nothing is subscribed, then
     read by resuming at the offset after the last one handled. */
  char resumed[64];
  snprintf(resumed, sizeof resumed, "c-%s-resumed", run);
  status = publish(client, f, f->stream, resumed, NULL, NULL);
  CHECK(status == FELIX_STATUS_OK, "publish before resume gave %d",
        (int)status);
  sub = NULL;
  status = felix_client_subscribe(client, f->tenant, f->ns, f->stream,
                                  FELIX_START_OFFSET, last + 1, &sub);
  CHECK(status == FELIX_STATUS_OK, "resume subscribe gave %d", (int)status);
  if (sub) {
    uint64_t offset = 0;
    if (expect_payload(sub, resumed, &offset)) {
      CHECK(offset == last + 1, "resume read %" PRIu64 ", expected %" PRIu64,
            offset, last + 1);
    }
    felix_subscription_free(sub);
  }

  /* error.unknown_stream_is_typed: never a connection failure, since a
     retry cannot make the stream exist. A broker with no assignment for the
     shard says it is unavailable, which is typed too. */
  status = publish(client, f, f->missing, "nowhere", NULL, NULL);
  CHECK(status == FELIX_STATUS_NOT_FOUND ||
            status == FELIX_STATUS_SHARD_UNAVAILABLE,
        "an unknown stream gave %d", (int)status);
  CHECK(felix_last_error_message() != NULL, "no message for an unknown stream");

  felix_client_free(client);

  /* error.unauthorized_is_typed */
  FelixClient *restricted = NULL;
  REQUIRE_OK(felix_client_connect(f->addrs, f->tenant, f->unauthorized_token,
                                  NULL, f->ca_file, &restricted));
  status = publish(restricted, f, f->stream, "not allowed", NULL, NULL);
  CHECK(status == FELIX_STATUS_AUTH, "an unauthorized publish gave %d",
        (int)status);
  felix_client_free(restricted);
}

int main(int argc, char **argv) {
  if (argc == 2 && strcmp(argv[1], "offline") == 0) {
    offline();
  } else if (argc == 10 && strcmp(argv[1], "fixture") == 0) {
    struct fixture f = {argv[2], argv[3], argv[4], argv[5], argv[6],
                        argv[7], argv[8], argv[9]};
    char run[32];
    snprintf(run, sizeof run, "%ld", (long)time(NULL));
    pubsub(&f, run);
  } else {
    fprintf(stderr, "usage: %s offline | fixture ADDRS TENANT NAMESPACE TOKEN "
                    "CA_FILE STREAM MISSING UNAUTHORIZED_TOKEN\n",
            argv[0]);
    return 2;
  }
  if (failures) {
    fprintf(stderr, "%d check(s) failed\n", failures);
    return 1;
  }
  printf("ok\n");
  return 0;
}
