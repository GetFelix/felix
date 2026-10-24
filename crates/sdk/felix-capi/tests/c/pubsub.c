/*
 * Drives the C ABI the way a C, Go or C# caller would: through the header and
 * a linked library, with no Rust in sight.
 *
 *   pubsub offline
 *     Argument checking, the last-error channel, and connects that fail or
 *     are cancelled. Needs no broker.
 *
 *   pubsub fixture ADDRS TENANT NAMESPACE TOKEN CA_FILE STREAM MISSING UNAUTHORIZED_TOKEN
 *     The publish and subscribe scenarios, against `felix-cluster
 *     client-fixture`. tests/c/run.sh starts the fixture and passes its
 *     fields in.
 *
 * Every call is asynchronous. `struct waiter` turns one into a blocking call
 * with a mutex and a condition variable, which is all a blocking caller needs.
 */
#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
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

struct waiter {
  pthread_mutex_t mu;
  pthread_cond_t cv;
  int calls;
  FelixStatus status;
  char message[512];
  void *handle;
  bool has_offset;
  uint64_t offset;
};

static void waiter_init(struct waiter *w) {
  memset(w, 0, sizeof *w);
  pthread_mutex_init(&w->mu, NULL);
  pthread_cond_init(&w->cv, NULL);
}

static void waiter_destroy(struct waiter *w) {
  pthread_mutex_destroy(&w->mu);
  pthread_cond_destroy(&w->cv);
}

/* Records one callback. The message is copied: it is only valid while the
   callback runs. */
static void finish(struct waiter *w, FelixStatus status, const char *message,
                   void *handle) {
  pthread_mutex_lock(&w->mu);
  w->calls++;
  w->status = status;
  w->handle = handle;
  snprintf(w->message, sizeof w->message, "%s", message ? message : "");
  pthread_cond_broadcast(&w->cv);
  pthread_mutex_unlock(&w->mu);
}

static void on_connect(void *user_data, FelixStatus status,
                       const char *message, FelixClient *client) {
  finish(user_data, status, message, client);
}

static void on_publish(void *user_data, FelixStatus status,
                       const char *message, bool has_offset, uint64_t offset) {
  struct waiter *w = user_data;
  w->has_offset = has_offset;
  w->offset = offset;
  finish(w, status, message, NULL);
}

static void on_subscribe(void *user_data, FelixStatus status,
                         const char *message, FelixSubscription *sub) {
  finish(user_data, status, message, sub);
}

static void on_event(void *user_data, FelixStatus status, const char *message,
                     FelixEvent *event) {
  finish(user_data, status, message, event);
}

/* The callback's status. Exits the test if it does not arrive in time. */
static FelixStatus wait_for(struct waiter *w, int ms) {
  struct timespec deadline;
  clock_gettime(CLOCK_REALTIME, &deadline);
  deadline.tv_sec += ms / 1000;
  deadline.tv_nsec += (long)(ms % 1000) * 1000000L;
  if (deadline.tv_nsec >= 1000000000L) {
    deadline.tv_sec++;
    deadline.tv_nsec -= 1000000000L;
  }
  pthread_mutex_lock(&w->mu);
  int rc = 0;
  while (w->calls == 0 && rc != ETIMEDOUT) {
    rc = pthread_cond_timedwait(&w->cv, &w->mu, &deadline);
  }
  int calls = w->calls;
  FelixStatus status = w->status;
  pthread_mutex_unlock(&w->mu);
  if (!calls) {
    /* The call is still pending and would call back into this waiter, which
       lives on the caller's stack. Stop here rather than let it. */
    fprintf(stderr, "FAIL: no callback within %d ms\n", ms);
    exit(1);
  }
  return status;
}

static void pause_ms(long ms) {
  struct timespec t = {ms / 1000, (ms % 1000) * 1000000L};
  nanosleep(&t, NULL);
}

static FelixStatus connect_sync(const char *addrs, const char *tenant,
                                const char *token, const char *ca_file,
                                FelixClient **out) {
  struct waiter w;
  waiter_init(&w);
  FelixStatus status = felix_client_connect(addrs, tenant, token, NULL, ca_file,
                                            NULL, on_connect, &w);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, WAIT_MS);
    *out = w.handle;
    if (status != FELIX_STATUS_OK) {
      fprintf(stderr, "connect: %s\n", w.message);
    }
  }
  waiter_destroy(&w);
  return status;
}

struct fixture {
  const char *addrs, *tenant, *ns, *token, *ca_file, *stream, *missing,
      *unauthorized_token;
};

static void offline(void) {
  struct waiter w;
  waiter_init(&w);

  /* Refused arguments are named, and the callback never runs. */
  FelixStatus status = felix_client_connect(NULL, "t1", "token", NULL, NULL,
                                            NULL, on_connect, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null addrs gave %d",
        (int)status);
  const char *message = felix_last_error_message();
  CHECK(message && strstr(message, "addrs"), "message does not name addrs");

  status = felix_client_connect("127.0.0.1:1", "t1", "token", NULL, NULL, NULL,
                                NULL, NULL);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null callback gave %d",
        (int)status);

  status = felix_client_connect("", "t1", "token", NULL, NULL, NULL,
                                on_connect, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "empty addrs gave %d",
        (int)status);

  status = felix_client_connect("127.0.0.1:1", "t1", "token", NULL,
                                "/nonexistent/ca.pem", NULL, on_connect, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "missing ca_file gave %d",
        (int)status);

  status = felix_client_publish(NULL, "t", "n", "s", NULL, 0,
                                FELIX_ACK_PER_MESSAGE, NULL, on_publish, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null client gave %d",
        (int)status);

  status = felix_subscription_next(NULL, 0, NULL, on_event, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null subscription gave %d",
        (int)status);

  pause_ms(100);
  CHECK(w.calls == 0, "a refused call ran its callback %d time(s)", w.calls);
  waiter_destroy(&w);

  /* Every free takes null, and so does cancel. */
  felix_client_free(NULL);
  felix_subscription_free(NULL);
  felix_event_free(NULL);
  felix_cancel_token_cancel(NULL);
  felix_cancel_token_free(NULL);

  /* A name that does not resolve is accepted, then reported as the caller's
     mistake, with its own message. */
  waiter_init(&w);
  status = felix_client_connect("felix.invalid:5000", "t1", "token", NULL,
                                NULL, NULL, on_connect, &w);
  CHECK(status == FELIX_STATUS_OK, "an unresolvable name was refused: %d",
        (int)status);
  status = wait_for(&w, WAIT_MS);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "an unresolvable name gave %d",
        (int)status);
  CHECK(strstr(w.message, "resolve") != NULL, "message was %s", w.message);
  CHECK(w.handle == NULL, "a failed connect handed out a client");
  waiter_destroy(&w);

  /* Nothing listens on port 1: a connection failure, not a crash. The
     platform trust store is enough, since no handshake starts. */
  waiter_init(&w);
  status = felix_client_connect("127.0.0.1:1", "t1", "token", NULL, NULL, NULL,
                                on_connect, &w);
  CHECK(status == FELIX_STATUS_OK, "connect was refused: %d", (int)status);
  status = wait_for(&w, WAIT_MS);
  CHECK(status != FELIX_STATUS_OK && status != FELIX_STATUS_PANIC,
        "connecting to a closed port gave %d", (int)status);
  CHECK(w.message[0] != '\0', "no message for a failed connect");
  waiter_destroy(&w);

  /* A cancelled connect reports it and hands out nothing. */
  waiter_init(&w);
  FelixCancelToken *cancel = felix_cancel_token_new();
  felix_cancel_token_cancel(cancel);
  status = felix_client_connect("127.0.0.1:1", "t1", "token", NULL, NULL,
                                cancel, on_connect, &w);
  felix_cancel_token_free(cancel);
  CHECK(status == FELIX_STATUS_OK, "connect was refused: %d", (int)status);
  status = wait_for(&w, WAIT_MS);
  CHECK(status == FELIX_STATUS_CANCELLED, "a cancelled connect gave %d",
        (int)status);
  CHECK(w.handle == NULL, "a cancelled connect handed out a client");
  pause_ms(100);
  CHECK(w.calls == 1, "the callback ran %d times", w.calls);
  waiter_destroy(&w);
}

static FelixStatus publish(FelixClient *client, const struct fixture *f,
                           const char *stream, const char *payload,
                           uint64_t *offset, char *message, size_t cap) {
  struct waiter w;
  waiter_init(&w);
  FelixStatus status = felix_client_publish(
      client, f->tenant, f->ns, stream, (const uint8_t *)payload,
      strlen(payload), FELIX_ACK_PER_MESSAGE, NULL, on_publish, &w);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, WAIT_MS);
    if (offset) {
      *offset = w.offset;
    }
    if (message) {
      snprintf(message, cap, "%s", w.message);
    }
  }
  waiter_destroy(&w);
  return status;
}

static FelixStatus subscribe(FelixClient *client, const struct fixture *f,
                             FelixStartKind start, uint64_t offset,
                             FelixSubscription **out) {
  struct waiter w;
  waiter_init(&w);
  FelixStatus status =
      felix_client_subscribe(client, f->tenant, f->ns, f->stream, start, offset,
                             NULL, on_subscribe, &w);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, WAIT_MS);
    *out = w.handle;
  }
  waiter_destroy(&w);
  return status;
}

/* Reads until `count` expected payloads arrive, starting each read from the
   previous one's callback. Records from other runs may share the stream, so
   others are skipped. */
struct reader {
  struct waiter done;
  FelixSubscription *sub;
  const char *const *expected;
  int count, next;
  uint64_t offsets[16];
  bool offsets_present;
};

static void on_read(void *user_data, FelixStatus status, const char *message,
                    FelixEvent *event) {
  struct reader *r = user_data;
  if (status != FELIX_STATUS_OK) {
    finish(&r->done, status, message, NULL);
    return;
  }
  const uint8_t *data = NULL;
  size_t len = 0;
  uint64_t offset = 0;
  bool has_offset = false;
  felix_event_payload(event, &data, &len);
  felix_event_offset(event, &offset, &has_offset);
  const char *want = r->expected[r->next];
  if (len == strlen(want) && memcmp(data, want, len) == 0) {
    r->offsets[r->next] = offset;
    r->offsets_present = r->offsets_present && has_offset;
    r->next++;
  }
  felix_event_free(event);
  if (r->next == r->count) {
    finish(&r->done, FELIX_STATUS_OK, NULL, NULL);
    return;
  }
  status = felix_subscription_next(r->sub, WAIT_MS, NULL, on_read, r);
  if (status != FELIX_STATUS_OK) {
    finish(&r->done, status, "re-arming the read was refused", NULL);
  }
}

/* The offsets of `count` expected payloads, read in order, or 0 on failure. */
static int read_expected(FelixSubscription *sub, const char *const *expected,
                         int count, uint64_t *offsets) {
  struct reader r = {.sub = sub,
                     .expected = expected,
                     .count = count,
                     .offsets_present = true};
  waiter_init(&r.done);
  FelixStatus status = felix_subscription_next(sub, WAIT_MS, NULL, on_read, &r);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&r.done, WAIT_MS * 2);
  }
  CHECK(status == FELIX_STATUS_OK, "reading %d events: status %d (%s)", count,
        (int)status, r.done.message);
  CHECK(status != FELIX_STATUS_OK || r.offsets_present,
        "a durable stream delivered an event without an offset");
  memcpy(offsets, r.offsets, sizeof(uint64_t) * (size_t)count);
  waiter_destroy(&r.done);
  return status == FELIX_STATUS_OK;
}

static void pubsub(const struct fixture *f, const char *run) {
  FelixClient *client = NULL;
  REQUIRE_OK(connect_sync(f->addrs, f->tenant, f->token, f->ca_file, &client));

  /* pubsub.roundtrip, pubsub.order, pubsub.offsets_are_contiguous */
  FelixSubscription *sub = NULL;
  FelixStatus status = subscribe(client, f, FELIX_START_LATEST, 0, &sub);
  if (status != FELIX_STATUS_OK) {
    CHECK(0, "subscribe gave %d", (int)status);
    felix_client_free(client);
    return;
  }

  /* Nothing has been published since the subscription started. */
  struct waiter w;
  waiter_init(&w);
  status = felix_subscription_next(sub, 200, NULL, on_event, &w);
  CHECK(status == FELIX_STATUS_OK, "an idle read was refused: %d", (int)status);
  status = wait_for(&w, WAIT_MS);
  CHECK(status == FELIX_STATUS_TIMEOUT && w.handle == NULL,
        "an idle read gave %d", (int)status);
  waiter_destroy(&w);

  /* A second read while one is pending is refused. A record published while
     a read is pending, which is then cancelled, is not lost: either that read
     got it, or the next one does. */
  char racing[64];
  snprintf(racing, sizeof racing, "c-%s-racing", run);
  waiter_init(&w);
  FelixCancelToken *cancel = felix_cancel_token_new();
  status = felix_subscription_next(sub, -1, cancel, on_event, &w);
  CHECK(status == FELIX_STATUS_OK, "a read was refused: %d", (int)status);
  status = felix_subscription_next(sub, 0, NULL, on_event, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "a second read gave %d",
        (int)status);
  status = publish(client, f, f->stream, racing, NULL, NULL, 0);
  CHECK(status == FELIX_STATUS_OK, "the racing publish gave %d", (int)status);
  felix_cancel_token_cancel(cancel);
  felix_cancel_token_free(cancel);
  status = wait_for(&w, WAIT_MS);
  CHECK(status == FELIX_STATUS_OK || status == FELIX_STATUS_CANCELLED,
        "a cancelled read gave %d", (int)status);
  bool got = false;
  if (status == FELIX_STATUS_OK) {
    const uint8_t *data = NULL;
    size_t len = 0;
    felix_event_payload(w.handle, &data, &len);
    got = len == strlen(racing) && memcmp(data, racing, len) == 0;
    felix_event_free(w.handle);
  }
  waiter_destroy(&w);
  if (!got) {
    const char *one[1] = {racing};
    uint64_t offset[1] = {0};
    read_expected(sub, one, 1, offset);
  }

  enum { COUNT = 10 };
  char payloads[COUNT][64];
  const char *expected[COUNT];
  for (int i = 0; i < COUNT; i++) {
    snprintf(payloads[i], sizeof payloads[i], "c-%s-%d", run, i);
    expected[i] = payloads[i];
    status = publish(client, f, f->stream, payloads[i], NULL, NULL, 0);
    CHECK(status == FELIX_STATUS_OK, "publish %d gave %d", i, (int)status);
  }

  uint64_t offsets[COUNT] = {0};
  uint64_t last = 0;
  if (read_expected(sub, expected, COUNT, offsets)) {
    for (int i = 1; i < COUNT; i++) {
      CHECK(offsets[i] == offsets[i - 1] + 1,
            "offsets jumped: %" PRIu64 " then %" PRIu64, offsets[i - 1],
            offsets[i]);
    }
    last = offsets[COUNT - 1];
  }

  /* Freeing a subscription with a read pending still calls back, once. */
  waiter_init(&w);
  status = felix_subscription_next(sub, -1, NULL, on_event, &w);
  CHECK(status == FELIX_STATUS_OK, "a read was refused: %d", (int)status);
  felix_subscription_free(sub);
  status = wait_for(&w, WAIT_MS);
  CHECK(status == FELIX_STATUS_CANCELLED, "a read pending on free gave %d",
        (int)status);
  pause_ms(100);
  CHECK(w.calls == 1, "the callback ran %d times", w.calls);
  waiter_destroy(&w);

  /* pubsub.resume_from_offset: published while nothing is subscribed, then
     read by resuming at the offset after the last one handled. */
  char resumed[64];
  snprintf(resumed, sizeof resumed, "c-%s-resumed", run);
  status = publish(client, f, f->stream, resumed, NULL, NULL, 0);
  CHECK(status == FELIX_STATUS_OK, "publish before resume gave %d",
        (int)status);
  sub = NULL;
  status = subscribe(client, f, FELIX_START_OFFSET, last + 1, &sub);
  CHECK(status == FELIX_STATUS_OK, "resume subscribe gave %d", (int)status);
  if (sub) {
    const char *one[1] = {resumed};
    uint64_t offset[1] = {0};
    if (read_expected(sub, one, 1, offset)) {
      CHECK(offset[0] == last + 1, "resume read %" PRIu64 ", expected %" PRIu64,
            offset[0], last + 1);
    }
    felix_subscription_free(sub);
  }

  /* error.unknown_stream_is_typed: never a connection failure, since a
     retry cannot make the stream exist. A broker with no assignment for the
     shard says it is unavailable, which is typed too. */
  char message[512] = "";
  status = publish(client, f, f->missing, "nowhere", NULL, message,
                   sizeof message);
  CHECK(status == FELIX_STATUS_NOT_FOUND ||
            status == FELIX_STATUS_SHARD_UNAVAILABLE,
        "an unknown stream gave %d", (int)status);
  CHECK(message[0] != '\0', "no message for an unknown stream");

  felix_client_free(client);

  /* error.unauthorized_is_typed */
  FelixClient *restricted = NULL;
  REQUIRE_OK(connect_sync(f->addrs, f->tenant, f->unauthorized_token,
                          f->ca_file, &restricted));
  status = publish(restricted, f, f->stream, "not allowed", NULL, NULL, 0);
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
