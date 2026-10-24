/*
 * Drives the C ABI the way a C, Go or C# caller would: through the header and
 * a linked library, with no Rust in sight.
 *
 *   pubsub offline
 *     Argument checking, the last-error channel, and connects that fail or
 *     are cancelled. Needs no broker.
 *
 *   pubsub fixture ADDRS TENANT NAMESPACE TOKEN CA_FILE STREAM MISSING UNAUTHORIZED_TOKEN GROUP_OPERATOR_TOKEN
 *     The publish, batch, subscribe, subscription and group scenarios, against `felix-cluster
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

/* Up to MAX_RECORDS per batch are kept; the scenarios send fewer. */
#define MAX_RECORDS 8

struct batch_waiter {
  struct waiter w;
  bool has_results;
  size_t count;
  FelixRecordResult results[MAX_RECORDS];
};

static void on_batch(void *user_data, FelixStatus status, const char *message,
                     const FelixRecordResult *results, size_t count) {
  struct batch_waiter *b = user_data;
  b->has_results = results != NULL;
  b->count = count;
  for (size_t i = 0; i < count && i < MAX_RECORDS; i++) {
    b->results[i] = results[i];
    /* Only valid while the callback runs. */
    b->results[i].message = NULL;
  }
  finish(&b->w, status, message, NULL);
}

static void on_subscribe(void *user_data, FelixStatus status,
                         const char *message, FelixSubscription *sub) {
  finish(user_data, status, message, sub);
}

static void on_event(void *user_data, FelixStatus status, const char *message,
                     FelixEvent *event) {
  finish(user_data, status, message, event);
}

struct item_waiter {
  struct waiter w;
  FelixShardItemKind kind;
  uint32_t shard;
  bool has_event;
  char payload[128];
  uint64_t offset;
  bool has_offset;
};

static void on_item(void *user_data, FelixStatus status, const char *message,
                    FelixShardItemKind kind, uint32_t shard,
                    FelixEvent *event) {
  struct item_waiter *it = user_data;
  it->kind = kind;
  it->shard = shard;
  it->has_event = event != NULL;
  if (event) {
    const uint8_t *data = NULL;
    size_t len = 0;
    felix_event_payload(event, &data, &len);
    if (len >= sizeof it->payload) {
      len = sizeof it->payload - 1;
    }
    memcpy(it->payload, data, len);
    it->payload[len] = '\0';
    felix_event_offset(event, &it->offset, &it->has_offset);
    felix_event_free(event);
  }
  finish(&it->w, status, message, NULL);
}

static void on_sharded_subscribe(void *user_data, FelixStatus status,
                                 const char *message,
                                 FelixShardedSubscription *sub) {
  finish(user_data, status, message, sub);
}

static void on_offset(void *user_data, FelixStatus status, const char *message,
                      bool has_offset, uint64_t offset) {
  on_publish(user_data, status, message, has_offset, offset);
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

static void batch_init(struct batch_waiter *b) {
  memset(b, 0, sizeof *b);
  waiter_init(&b->w);
}

/* The callback's status, or the call's when it was refused. */
static FelixStatus batch_wait(struct batch_waiter *b, FelixStatus started) {
  return started == FELIX_STATUS_OK ? wait_for(&b->w, WAIT_MS) : started;
}

/* Whether every record of a batch of `count` was accepted. */
static bool all_accepted(const struct batch_waiter *b, size_t count) {
  if (!b->has_results || b->count != count) {
    return false;
  }
  for (size_t i = 0; i < count; i++) {
    if (b->results[i].status != FELIX_STATUS_OK) {
      return false;
    }
  }
  return true;
}

/* Whether the first `count` records all carry offsets. A publish forwarded
   by a broker that does not own the shard can be acknowledged without one,
   so offsets are compared only when the broker reported them. */
static bool all_offsets(const struct batch_waiter *b, size_t count) {
  for (size_t i = 0; i < count; i++) {
    if (!b->results[i].has_offset) {
      return false;
    }
  }
  return true;
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
      *unauthorized_token, *group_operator_token;
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

  status = felix_client_publish(NULL, "t", "n", "s", NULL, 0, NULL, 0,
                                FELIX_ACK_PER_MESSAGE,
                                FELIX_DELIVERY_AT_MOST_ONCE, NULL, on_publish,
                                &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null client gave %d",
        (int)status);

  struct batch_waiter b;
  batch_init(&b);
  status = felix_client_publish_batch(NULL, "t", "n", "s", NULL, NULL, 0, NULL,
                                      0, FELIX_ACK_PER_BATCH, NULL, on_batch,
                                      &b);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null client gave %d",
        (int)status);

  status = felix_subscription_next(NULL, 0, NULL, on_event, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null subscription gave %d",
        (int)status);

  status = felix_client_subscribe(NULL, "t", "n", "s", 0, FELIX_START_LATEST, 0,
                                  NULL, on_subscribe, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null client gave %d",
        (int)status);
  status = felix_client_subscribe_sharded(NULL, "t", "n", "s",
                                          FELIX_START_LATEST, 0, NULL,
                                          on_sharded_subscribe, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null client gave %d",
        (int)status);
  status = felix_client_resubscribe_sharded(
      NULL, "t", "n", "s", NULL, NULL, 0, FELIX_START_LATEST, 0, NULL,
      on_sharded_subscribe, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null client gave %d",
        (int)status);
  status = felix_client_offset_for_time(NULL, "t", "n", "s", 0, 1, NULL,
                                        on_offset, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null client gave %d",
        (int)status);
  status = felix_sharded_next(NULL, 0, NULL, on_item, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null sharded gave %d",
        (int)status);
  FelixCloseReason reason = 0;
  status = felix_subscription_close_reason(NULL, &reason);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null close reason gave %d",
        (int)status);
  status = felix_sharded_positions(NULL, NULL, NULL, 0, NULL);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null positions gave %d",
        (int)status);

  pause_ms(100);
  CHECK(w.calls == 0, "a refused call ran its callback %d time(s)", w.calls);
  CHECK(b.w.calls == 0, "a refused batch ran its callback %d time(s)",
        b.w.calls);
  waiter_destroy(&w);
  waiter_destroy(&b.w);

  /* Every free takes null, and so does cancel. */
  felix_client_free(NULL);
  felix_subscription_free(NULL);
  felix_sharded_subscription_free(NULL);
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
      strlen(payload), NULL, 0, FELIX_ACK_PER_MESSAGE,
      FELIX_DELIVERY_AT_MOST_ONCE, NULL, on_publish, &w);
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

static FelixStatus subscribe_shard(FelixClient *client,
                                   const struct fixture *f, uint32_t shard,
                                   FelixStartKind start, uint64_t offset,
                                   FelixSubscription **out) {
  struct waiter w;
  waiter_init(&w);
  FelixStatus status =
      felix_client_subscribe(client, f->tenant, f->ns, f->stream, shard, start,
                             offset, NULL, on_subscribe, &w);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, WAIT_MS);
    *out = w.handle;
  }
  waiter_destroy(&w);
  return status;
}

static FelixStatus subscribe(FelixClient *client, const struct fixture *f,
                             FelixStartKind start, uint64_t offset,
                             FelixSubscription **out) {
  return subscribe_shard(client, f, 0, start, offset, out);
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

/* publish.batch, publish.on_shard, publish.keyed: every record of a batch has
   its own result, and records sent together have contiguous offsets. */
static void batches(const struct fixture *f, const char *run) {
  FelixClient *client = NULL;
  REQUIRE_OK(connect_sync(f->addrs, f->tenant, f->token, f->ca_file, &client));
  FelixSubscription *sub = NULL;
  FelixStatus status = subscribe(client, f, FELIX_START_LATEST, 0, &sub);
  if (status != FELIX_STATUS_OK) {
    CHECK(0, "subscribe gave %d", (int)status);
    felix_client_free(client);
    return;
  }

  enum { N = 3 };
  char payloads[2 * N][64];
  const char *expected[2 * N];
  uint8_t data[2 * N * 64];
  size_t lens[2 * N];
  size_t used = 0;
  for (int i = 0; i < 2 * N; i++) {
    snprintf(payloads[i], sizeof payloads[i], "c-%s-batch-%d", run, i);
    expected[i] = payloads[i];
    lens[i] = strlen(payloads[i]);
    memcpy(data + used, payloads[i], lens[i]);
    used += lens[i];
  }
  size_t first_half = lens[0] + lens[1] + lens[2];

  /* An unkeyed batch goes to shard 0, which the subscription reads, and so
     does one named for shard 0. */
  uint64_t sent[2 * N] = {0};
  bool sent_has[2 * N] = {false};
  struct batch_waiter b;
  batch_init(&b);
  status = batch_wait(
      &b, felix_client_publish_batch(client, f->tenant, f->ns, f->stream, data,
                                     lens, N, NULL, 0, FELIX_ACK_PER_BATCH,
                                     NULL, on_batch, &b));
  CHECK(status == FELIX_STATUS_OK && all_accepted(&b, N),
        "an unkeyed batch gave %d (%s)", (int)status, b.w.message);
  for (int i = 0; i < N; i++) {
    sent[i] = b.results[i].offset;
    sent_has[i] = b.results[i].has_offset;
  }
  waiter_destroy(&b.w);

  batch_init(&b);
  status = batch_wait(
      &b, felix_client_publish_on_shard(
              client, f->tenant, f->ns, f->stream, 0, data + first_half,
              lens + N, N, FELIX_ACK_PER_BATCH, NULL, on_batch, &b));
  CHECK(status == FELIX_STATUS_OK && all_accepted(&b, N),
        "a batch on shard 0 gave %d (%s)", (int)status, b.w.message);
  for (int i = 0; i < N; i++) {
    sent[N + i] = b.results[i].offset;
    sent_has[N + i] = b.results[i].has_offset;
  }
  waiter_destroy(&b.w);

  uint64_t read[2 * N] = {0};
  if (read_expected(sub, expected, 2 * N, read)) {
    for (int i = 0; i < 2 * N; i++) {
      CHECK(!sent_has[i] || read[i] == sent[i],
            "record %d was read at %" PRIu64 " but reported at %" PRIu64, i,
            read[i], sent[i]);
    }
  }
  felix_subscription_free(sub);

  /* A shard the stream does not have writes nothing and fails every record. */
  batch_init(&b);
  status = batch_wait(
      &b, felix_client_publish_on_shard(client, f->tenant, f->ns, f->stream,
                                        1000, data, lens, 2,
                                        FELIX_ACK_PER_BATCH, NULL, on_batch,
                                        &b));
  CHECK(status != FELIX_STATUS_OK && status != FELIX_STATUS_PANIC,
        "a missing shard gave %d", (int)status);
  CHECK(b.has_results && b.count == 2 && b.results[0].status == status &&
            b.results[1].status == status,
        "a refused batch did not fail every record");
  waiter_destroy(&b.w);

  /* Records sharing a key share a shard and go together. Whether "a" and "b"
     share one is up to the hash, so either layout of offsets is right. */
  const uint8_t keys[] = "abab";
  const size_t key_lens[] = {1, 1, 1, 1};
  const FelixDelivery deliveries[] = {FELIX_DELIVERY_AT_MOST_ONCE,
                                      FELIX_DELIVERY_AT_LEAST_ONCE};
  for (int d = 0; d < 2; d++) {
    batch_init(&b);
    status = batch_wait(
        &b, felix_client_publish_records(client, f->tenant, f->ns, f->stream,
                                         data, lens, keys, key_lens, 4,
                                         FELIX_ACK_PER_BATCH, deliveries[d],
                                         NULL, on_batch, &b));
    CHECK(status == FELIX_STATUS_OK && all_accepted(&b, 4),
          "keyed records (delivery %d) gave %d (%s)", (int)deliveries[d],
          (int)status, b.w.message);
    uint64_t o[4];
    for (int i = 0; i < 4; i++) {
      o[i] = b.results[i].offset;
    }
    bool split = o[2] == o[0] + 1 && o[3] == o[1] + 1;
    bool together = o[1] == o[0] + 1 && o[2] == o[0] + 2 && o[3] == o[0] + 3;
    CHECK(status != FELIX_STATUS_OK || !all_offsets(&b, 4) || split ||
              together,
          "keyed offsets %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64, o[0],
          o[1], o[2], o[3]);
    waiter_destroy(&b.w);

    /* One keyed record. */
    struct waiter w;
    waiter_init(&w);
    status = felix_client_publish(client, f->tenant, f->ns, f->stream, data,
                                  lens[0], keys, 1, FELIX_ACK_PER_MESSAGE,
                                  deliveries[d], NULL, on_publish, &w);
    if (status == FELIX_STATUS_OK) {
      status = wait_for(&w, WAIT_MS);
    }
    CHECK(status == FELIX_STATUS_OK,
          "a keyed publish (delivery %d) gave %d (%s)", (int)deliveries[d],
          (int)status, w.message);
    waiter_destroy(&w);
  }

  /* A keyed batch is one shard's, so all of it is contiguous. */
  batch_init(&b);
  status = batch_wait(
      &b, felix_client_publish_batch(client, f->tenant, f->ns, f->stream, data,
                                     lens, N, keys, 1, FELIX_ACK_PER_BATCH,
                                     NULL, on_batch, &b));
  CHECK(status == FELIX_STATUS_OK && all_accepted(&b, N),
        "a keyed batch gave %d (%s)", (int)status, b.w.message);
  CHECK(!all_offsets(&b, N) ||
            (b.results[1].offset == b.results[0].offset + 1 &&
             b.results[2].offset == b.results[0].offset + 2),
        "a keyed batch was not contiguous");
  waiter_destroy(&b.w);

  /* An empty batch completes with no results. */
  batch_init(&b);
  status = batch_wait(
      &b, felix_client_publish_batch(client, f->tenant, f->ns, f->stream, NULL,
                                     NULL, 0, NULL, 0, FELIX_ACK_PER_BATCH,
                                     NULL, on_batch, &b));
  CHECK(status == FELIX_STATUS_OK && !b.has_results && b.count == 0,
        "an empty batch gave %d", (int)status);
  waiter_destroy(&b.w);

  /* Bad layouts are refused before anything is sent. */
  batch_init(&b);
  status = felix_client_publish_batch(client, f->tenant, f->ns, f->stream,
                                      NULL, lens, 2, NULL, 0,
                                      FELIX_ACK_PER_BATCH, NULL, on_batch, &b);
  const char *message = felix_last_error_message();
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT && message &&
            strstr(message, "data"),
        "lengths without data gave %d", (int)status);
  status = felix_client_publish_records(
      client, f->tenant, f->ns, f->stream, data, lens, NULL, key_lens, 4,
      FELIX_ACK_PER_BATCH, FELIX_DELIVERY_AT_MOST_ONCE, NULL, on_batch, &b);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT,
        "key lengths without keys gave %d", (int)status);
  status = felix_client_publish_records(client, f->tenant, f->ns, f->stream,
                                        data, lens, keys, key_lens, 4,
                                        FELIX_ACK_PER_BATCH, 7, NULL, on_batch,
                                        &b);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "delivery 7 gave %d",
        (int)status);
  pause_ms(100);
  CHECK(b.w.calls == 0, "a refused batch ran its callback");
  waiter_destroy(&b.w);

  /* A batch cancelled before it starts says so, with no results. */
  batch_init(&b);
  FelixCancelToken *cancel = felix_cancel_token_new();
  felix_cancel_token_cancel(cancel);
  status = batch_wait(
      &b, felix_client_publish_batch(client, f->tenant, f->ns, f->stream, data,
                                     lens, N, NULL, 0, FELIX_ACK_PER_BATCH,
                                     cancel, on_batch, &b));
  felix_cancel_token_free(cancel);
  CHECK(status == FELIX_STATUS_CANCELLED && !b.has_results,
        "a cancelled batch gave %d", (int)status);
  waiter_destroy(&b.w);

  felix_client_free(client);
}

/* One read of a sharded subscription, waited for. */
static FelixStatus next_item(FelixShardedSubscription *sub, int timeout_ms,
                             struct item_waiter *it) {
  memset(it, 0, sizeof *it);
  waiter_init(&it->w);
  FelixStatus status = felix_sharded_next(sub, timeout_ms, NULL, on_item, it);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&it->w, WAIT_MS * 2);
  }
  waiter_destroy(&it->w);
  return status;
}

#define MAX_SHARDS 16

/* The highest offset read from each shard, whoever published it. */
struct seen {
  bool any[MAX_SHARDS];
  uint64_t max_offset[MAX_SHARDS];
};

/* Reads until `want` payloads starting with `prefix` arrive, counting the
   ones starting with `forbid` on the way. Other runs' records are skipped. */
static bool read_prefixed(FelixShardedSubscription *sub, const char *prefix,
                          int want, const char *forbid, int *forbidden,
                          struct seen *seen) {
  int found = 0;
  while (found < want) {
    struct item_waiter it;
    FelixStatus status = next_item(sub, WAIT_MS, &it);
    if (status != FELIX_STATUS_OK) {
      CHECK(0, "sharded read after %d of %d records gave %d (%s)", found, want,
            (int)status, it.w.message);
      return false;
    }
    CHECK(it.kind == FELIX_SHARD_RECORD || !it.has_event,
          "kind %d carried an event", (int)it.kind);
    if (it.kind != FELIX_SHARD_RECORD) {
      continue;
    }
    if (it.has_offset && it.shard < MAX_SHARDS) {
      if (!seen->any[it.shard] || it.offset > seen->max_offset[it.shard]) {
        seen->max_offset[it.shard] = it.offset;
      }
      seen->any[it.shard] = true;
    }
    if (strncmp(it.payload, prefix, strlen(prefix)) == 0) {
      found++;
    }
    if (forbid && strncmp(it.payload, forbid, strlen(forbid)) == 0) {
      (*forbidden)++;
    }
  }
  return true;
}

static FelixStatus publish_keyed(FelixClient *client, const struct fixture *f,
                                 const char *key, const char *payload) {
  struct waiter w;
  waiter_init(&w);
  FelixStatus status = felix_client_publish(
      client, f->tenant, f->ns, f->stream, (const uint8_t *)payload,
      strlen(payload), (const uint8_t *)key, strlen(key),
      FELIX_ACK_PER_MESSAGE, FELIX_DELIVERY_AT_MOST_ONCE, NULL, on_publish, &w);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, WAIT_MS);
  }
  waiter_destroy(&w);
  return status;
}

/* subscribe on a named shard, the sharded subscription and its positions,
   resuming from them, offset_for_time, and close reasons. */
static void subscriptions(const struct fixture *f, const char *run) {
  FelixClient *client = NULL;
  REQUIRE_OK(connect_sync(f->addrs, f->tenant, f->token, f->ca_file, &client));

  /* Each named shard reads back what was published to it. */
  for (uint32_t shard = 1; shard <= 3; shard++) {
    FelixSubscription *sub = NULL;
    FelixStatus status = subscribe_shard(client, f, shard, FELIX_START_LATEST, 0, &sub);
    CHECK(status == FELIX_STATUS_OK, "subscribe on shard %u gave %d",
          (unsigned)shard, (int)status);
    if (status != FELIX_STATUS_OK) {
      continue;
    }
    FelixCloseReason reason = -1;
    CHECK(felix_subscription_close_reason(sub, &reason) == FELIX_STATUS_OK &&
              reason == FELIX_CLOSE_OPEN,
          "a live subscription reported close reason %d", (int)reason);
    uint64_t start = 0;
    bool has_start = false;
    CHECK(felix_subscription_start_offset(sub, &start, &has_start) ==
              FELIX_STATUS_OK,
          "start_offset failed");
    CHECK(has_start, "a durable stream reported no start offset");

    char payloads[2][64];
    const char *expected[2];
    uint8_t data[128];
    size_t lens[2], used = 0;
    for (int i = 0; i < 2; i++) {
      snprintf(payloads[i], sizeof payloads[i], "c-%s-shard%u-%d", run,
               (unsigned)shard, i);
      expected[i] = payloads[i];
      lens[i] = strlen(payloads[i]);
      memcpy(data + used, payloads[i], lens[i]);
      used += lens[i];
    }
    struct batch_waiter b;
    batch_init(&b);
    FelixStatus sent = batch_wait(
        &b, felix_client_publish_on_shard(client, f->tenant, f->ns, f->stream,
                                          shard, data, lens, 2,
                                          FELIX_ACK_PER_BATCH, NULL, on_batch,
                                          &b));
    CHECK(sent == FELIX_STATUS_OK, "publish on shard %u gave %d (%s)",
          (unsigned)shard, (int)sent, b.w.message);
    waiter_destroy(&b.w);
    uint64_t offsets[2];
    read_expected(sub, expected, 2, offsets);
    CHECK(offsets[0] >= start, "read at %" PRIu64 " below the start %" PRIu64,
          offsets[0], start);
    felix_subscription_free(sub);
  }

  /* A shard past the stream's width is refused by the broker. */
  FelixSubscription *none = NULL;
  FelixStatus status = subscribe_shard(client, f, 1000, FELIX_START_LATEST, 0, &none);
  CHECK(status != FELIX_STATUS_OK && status != FELIX_STATUS_PANIC &&
            none == NULL,
        "a missing shard gave %d", (int)status);

  /* Every shard, merged. Distinct keys spread the records. */
  FelixShardedSubscription *sharded = NULL;
  struct waiter w;
  waiter_init(&w);
  status = felix_client_subscribe_sharded(client, f->tenant, f->ns, f->stream,
                                          FELIX_START_LATEST, 0, NULL,
                                          on_sharded_subscribe, &w);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, WAIT_MS);
    sharded = w.handle;
  }
  waiter_destroy(&w);
  CHECK(status == FELIX_STATUS_OK, "sharded subscribe gave %d", (int)status);
  if (status != FELIX_STATUS_OK) {
    felix_client_free(client);
    return;
  }
  uint32_t width = 0;
  CHECK(felix_sharded_shards(sharded, &width) == FELIX_STATUS_OK && width > 1,
        "the sharded subscription covers %u shards", (unsigned)width);

  enum { KEYED = 8, RESUMED = 4 };
  char prefix[64], resumed_prefix[64];
  snprintf(prefix, sizeof prefix, "c-%s-sh-", run);
  snprintf(resumed_prefix, sizeof resumed_prefix, "c-%s-re-", run);
  for (int i = 0; i < KEYED; i++) {
    char key[16], payload[96];
    snprintf(key, sizeof key, "key-%d", i);
    snprintf(payload, sizeof payload, "%s%d", prefix, i);
    CHECK(publish_keyed(client, f, key, payload) == FELIX_STATUS_OK,
          "keyed publish %d failed", i);
  }
  struct seen seen;
  memset(&seen, 0, sizeof seen);
  int forbidden = 0;
  read_prefixed(sharded, prefix, KEYED, NULL, &forbidden, &seen);

  /* Positions are what was handed out, per shard that delivered. */
  size_t total = 0;
  CHECK(felix_sharded_positions(sharded, NULL, NULL, 0, &total) ==
            FELIX_STATUS_OK,
        "counting positions failed");
  uint32_t pos_shards[MAX_SHARDS];
  uint64_t pos_offsets[MAX_SHARDS];
  size_t count = 0;
  CHECK(felix_sharded_positions(sharded, pos_shards, pos_offsets, MAX_SHARDS,
                                &count) == FELIX_STATUS_OK &&
            count == total && count > 0 && count <= MAX_SHARDS,
        "positions gave %zu of %zu", count, total);
  size_t delivered = 0;
  for (uint32_t s = 0; s < MAX_SHARDS; s++) {
    delivered += seen.any[s];
  }
  CHECK(count == delivered, "%zu positions for %zu shards that delivered",
        count, delivered);
  for (size_t i = 0; i < count && i < MAX_SHARDS; i++) {
    CHECK(pos_shards[i] < MAX_SHARDS && seen.any[pos_shards[i]] &&
              pos_offsets[i] == seen.max_offset[pos_shards[i]],
          "shard %u position %" PRIu64 " is not the last offset read",
          (unsigned)pos_shards[i], pos_offsets[i]);
  }

  /* A read in flight can be cancelled, and loses nothing. */
  struct item_waiter it;
  memset(&it, 0, sizeof it);
  waiter_init(&it.w);
  FelixCancelToken *cancel = felix_cancel_token_new();
  REQUIRE_OK(felix_sharded_next(sharded, -1, cancel, on_item, &it));
  CHECK(felix_sharded_next(sharded, -1, NULL, on_item, &it) ==
            FELIX_STATUS_INVALID_ARGUMENT,
        "a second pending sharded read was accepted");
  size_t during = 0;
  CHECK(felix_sharded_positions(sharded, NULL, NULL, 0, &during) ==
                FELIX_STATUS_OK &&
            during == total,
        "positions are not readable while a read is pending");
  pause_ms(50);
  felix_cancel_token_cancel(cancel);
  status = wait_for(&it.w, WAIT_MS);
  CHECK(status == FELIX_STATUS_CANCELLED, "a cancelled sharded read gave %d",
        (int)status);
  felix_cancel_token_free(cancel);
  waiter_destroy(&it.w);
  CHECK(next_item(sharded, 100, &it) == FELIX_STATUS_TIMEOUT,
        "an idle sharded read did not time out");

  /* Resume from the positions: only later records arrive. */
  for (int i = 0; i < RESUMED; i++) {
    char key[16], payload[96];
    snprintf(key, sizeof key, "key-%d", i);
    snprintf(payload, sizeof payload, "%s%d", resumed_prefix, i);
    CHECK(publish_keyed(client, f, key, payload) == FELIX_STATUS_OK,
          "keyed publish %d failed", i);
  }
  felix_sharded_subscription_free(sharded);
  sharded = NULL;

  waiter_init(&w);
  status = felix_client_resubscribe_sharded(
      client, f->tenant, f->ns, f->stream, pos_shards, pos_offsets, count,
      FELIX_START_EARLIEST, 0, NULL, on_sharded_subscribe, &w);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, WAIT_MS);
    sharded = w.handle;
  }
  waiter_destroy(&w);
  CHECK(status == FELIX_STATUS_OK, "resubscribe gave %d", (int)status);
  if (sharded) {
    memset(&seen, 0, sizeof seen);
    forbidden = 0;
    read_prefixed(sharded, resumed_prefix, RESUMED, prefix, &forbidden, &seen);
    /* Shards that resumed at EARLIEST replay history, which can still be
       queued. Drain it, still counting replays, so the pending read below
       waits on an empty feed. */
    for (int drained = 0; drained < 200; drained++) {
      struct item_waiter extra;
      if (next_item(sharded, 100, &extra) != FELIX_STATUS_OK) {
        break;
      }
      if (extra.kind == FELIX_SHARD_RECORD &&
          strncmp(extra.payload, prefix, strlen(prefix)) == 0) {
        forbidden++;
      }
    }
    CHECK(forbidden == 0, "a resumed subscription replayed %d records",
          forbidden);

    /* Freeing with a read pending completes it as cancelled. */
    memset(&it, 0, sizeof it);
    waiter_init(&it.w);
    REQUIRE_OK(felix_sharded_next(sharded, -1, NULL, on_item, &it));
    pause_ms(50);
    felix_sharded_subscription_free(sharded);
    status = wait_for(&it.w, WAIT_MS);
    CHECK(status == FELIX_STATUS_CANCELLED,
          "a sharded read pending at free gave %d", (int)status);
    waiter_destroy(&it.w);
  }

  /* The same shard twice is refused before anything starts. */
  uint32_t twice[] = {1, 1};
  uint64_t at[] = {5, 6};
  waiter_init(&w);
  status = felix_client_resubscribe_sharded(
      client, f->tenant, f->ns, f->stream, twice, at, 2, FELIX_START_LATEST, 0,
      NULL, on_sharded_subscribe, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "a duplicate shard gave %d",
        (int)status);
  pause_ms(100);
  CHECK(w.calls == 0, "a refused resubscribe ran its callback");
  waiter_destroy(&w);

  /* A record exists by now: an hour ago has an offset, an hour ahead none. */
  uint64_t now = (uint64_t)time(NULL) * 1000000ULL;
  uint64_t hour = 3600ULL * 1000000ULL;
  for (int ahead = 0; ahead < 2; ahead++) {
    waiter_init(&w);
    status = felix_client_offset_for_time(
        client, f->tenant, f->ns, f->stream, 0, ahead ? now + hour : now - hour,
        NULL, on_offset, &w);
    if (status == FELIX_STATUS_OK) {
      status = wait_for(&w, WAIT_MS);
    }
    CHECK(status == FELIX_STATUS_OK, "offset_for_time gave %d (%s)",
          (int)status, w.message);
    CHECK(status != FELIX_STATUS_OK || w.has_offset == !ahead,
          "offset_for_time %s: has_offset was %d", ahead ? "ahead" : "behind",
          (int)w.has_offset);
    waiter_destroy(&w);
  }

  /* A cancelled lookup says so. */
  waiter_init(&w);
  cancel = felix_cancel_token_new();
  felix_cancel_token_cancel(cancel);
  status = felix_client_offset_for_time(client, f->tenant, f->ns, f->stream, 0,
                                        now, cancel, on_offset, &w);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, WAIT_MS);
  }
  felix_cancel_token_free(cancel);
  CHECK(status == FELIX_STATUS_CANCELLED, "a cancelled lookup gave %d",
        (int)status);
  waiter_destroy(&w);

  felix_client_free(client);
}

/* ---- consumer groups ---------------------------------------------------- */

#define MAX_GROUP_RECORDS 8

struct grecord {
  uint64_t offset;
  uint32_t attempts;
  uint64_t skipped_before;
  char payload[96];
};

/* One group call's result; each call fills the fields it reports. */
struct gres {
  struct waiter w;
  struct grecord records[MAX_GROUP_RECORDS];
  size_t count;
  uint64_t visible_ms;
  uint64_t position;
  bool moved;
  bool existed;
  uint32_t shards;
  FelixGroupInfo info;
  bool has_info;
};

static void on_done(void *user_data, FelixStatus status, const char *message) {
  struct gres *r = user_data;
  finish(&r->w, status, message, NULL);
}

static void on_group_poll(void *user_data, FelixStatus status,
                          const char *message, const FelixGroupRecord *records,
                          size_t count) {
  struct gres *r = user_data;
  r->count = count;
  for (size_t i = 0; i < count && i < MAX_GROUP_RECORDS; i++) {
    struct grecord *out = &r->records[i];
    out->offset = records[i].offset;
    out->attempts = records[i].attempts;
    out->skipped_before = records[i].skipped_before;
    size_t len = records[i].payload_len;
    if (len >= sizeof out->payload) {
      len = sizeof out->payload - 1;
    }
    if (len) {
      memcpy(out->payload, records[i].payload, len);
    }
    out->payload[len] = '\0';
  }
  finish(&r->w, status, message, NULL);
}

static void on_extend(void *user_data, FelixStatus status, const char *message,
                      uint64_t visible_ms) {
  struct gres *r = user_data;
  r->visible_ms = visible_ms;
  finish(&r->w, status, message, NULL);
}

static void on_dead_letters(void *user_data, FelixStatus status,
                            const char *message, const uint64_t *offsets,
                            size_t count) {
  struct gres *r = user_data;
  r->count = count;
  for (size_t i = 0; i < count && i < MAX_GROUP_RECORDS; i++) {
    r->records[i].offset = offsets[i];
  }
  finish(&r->w, status, message, NULL);
}

static void on_position(void *user_data, FelixStatus status,
                        const char *message, uint64_t offset, bool moved) {
  struct gres *r = user_data;
  r->position = offset;
  r->moved = moved;
  finish(&r->w, status, message, NULL);
}

static void on_info(void *user_data, FelixStatus status, const char *message,
                    const FelixGroupInfo *info) {
  struct gres *r = user_data;
  r->has_info = info != NULL;
  if (info) {
    r->info = *info;
  }
  finish(&r->w, status, message, NULL);
}

static void on_deleted(void *user_data, FelixStatus status, const char *message,
                       bool existed) {
  struct gres *r = user_data;
  r->existed = existed;
  finish(&r->w, status, message, NULL);
}

static void on_shards(void *user_data, FelixStatus status, const char *message,
                      uint32_t shards) {
  struct gres *r = user_data;
  r->shards = shards;
  finish(&r->w, status, message, NULL);
}

/* `client` consumes groups; `admin` holds group.manage and creates, seeks and
   deletes them, and discards and redrives dead letters. */
struct gctx {
  FelixClient *client;
  FelixClient *admin;
  const struct fixture *f;
  uint32_t shard;
  const char *group;
};

static void gres_init(struct gres *r) {
  memset(r, 0, sizeof *r);
  waiter_init(&r->w);
}

/* The callback's status, or the call's when it was refused. */
static FelixStatus gwait(struct gres *r, FelixStatus started) {
  return started == FELIX_STATUS_OK ? wait_for(&r->w, WAIT_MS) : started;
}

/* The group calls below each run one call to completion. */
static FelixStatus g_create(const struct gctx *g, FelixStartKind start,
                            uint64_t *offset, bool *moved) {
  struct gres r;
  gres_init(&r);
  FelixStatus status = gwait(
      &r, felix_client_group_create(g->admin, g->f->tenant, g->f->ns,
                                    g->f->stream, g->shard, g->group, start, 0,
                                    NULL, on_position, &r));
  *offset = r.position;
  *moved = r.moved;
  waiter_destroy(&r.w);
  return status;
}

static FelixStatus g_seek(const struct gctx *g, FelixStartKind start,
                          uint64_t *offset) {
  struct gres r;
  gres_init(&r);
  FelixStatus status = gwait(
      &r, felix_client_group_seek(g->admin, g->f->tenant, g->f->ns,
                                  g->f->stream, g->shard, g->group, start, 0,
                                  NULL, on_position, &r));
  *offset = r.position;
  waiter_destroy(&r.w);
  return status;
}

static FelixStatus g_poll(const struct gctx *g, uint32_t max, uint64_t wait_ms,
                          const char *consumer, struct gres *out) {
  gres_init(out);
  return gwait(out, felix_client_group_poll(
                        g->client, g->f->tenant, g->f->ns, g->f->stream,
                        g->shard, g->group, max, wait_ms, 0, consumer, false,
                        NULL, on_group_poll, out));
}

/* Polls until `want` records have arrived. Fewer is a failure to report. */
static size_t g_collect(const struct gctx *g, const char *consumer,
                        size_t want, struct grecord *got) {
  size_t have = 0;
  for (int attempt = 0; attempt < 10 && have < want; attempt++) {
    struct gres r;
    FelixStatus status = g_poll(g, (uint32_t)(want - have), 3000, consumer, &r);
    CHECK(status == FELIX_STATUS_OK, "group poll gave %d (%s)", (int)status,
          r.w.message);
    for (size_t i = 0; status == FELIX_STATUS_OK && i < r.count && have < want;
         i++) {
      got[have++] = r.records[i];
    }
    waiter_destroy(&r.w);
    if (status != FELIX_STATUS_OK) {
      break;
    }
  }
  CHECK(have == want, "group poll returned %zu of %zu records", have, want);
  return have;
}

static FelixStatus g_settle(const struct gctx *g, const char *what,
                            uint64_t offset, uint32_t attempts,
                            uint64_t arg_ms, uint64_t *visible_ms) {
  struct gres r;
  gres_init(&r);
  const FelixClient *c = g->client;
  if (strcmp(what, "discard") == 0 || strcmp(what, "redrive") == 0) {
    c = g->admin;
  }
  const char *t = g->f->tenant, *n = g->f->ns, *s = g->f->stream;
  FelixStatus started;
  if (strcmp(what, "ack") == 0) {
    started = felix_client_group_ack(c, t, n, s, g->shard, g->group, offset,
                                     NULL, on_done, &r);
  } else if (strcmp(what, "nack") == 0) {
    started = felix_client_group_nack(c, t, n, s, g->shard, g->group, offset,
                                      attempts, arg_ms, NULL, on_done, &r);
  } else if (strcmp(what, "dead_letter") == 0) {
    started = felix_client_group_dead_letter(c, t, n, s, g->shard, g->group,
                                             offset, NULL, on_done, &r);
  } else if (strcmp(what, "discard") == 0) {
    started = felix_client_group_discard(c, t, n, s, g->shard, g->group, offset,
                                         NULL, on_done, &r);
  } else if (strcmp(what, "redrive") == 0) {
    started = felix_client_group_redrive(c, t, n, s, g->shard, g->group, offset,
                                         NULL, on_done, &r);
  } else {
    started = felix_client_group_extend(c, t, n, s, g->shard, g->group, offset,
                                        attempts, arg_ms, NULL, on_extend, &r);
  }
  FelixStatus status = gwait(&r, started);
  CHECK(status == FELIX_STATUS_OK, "group %s gave %d (%s)", what, (int)status,
        r.w.message);
  if (visible_ms) {
    *visible_ms = r.visible_ms;
  }
  waiter_destroy(&r.w);
  return status;
}

/* The group's dead letters, as offsets. */
static size_t g_dead_letters(const struct gctx *g, uint64_t *offsets) {
  struct gres r;
  gres_init(&r);
  FelixStatus status = gwait(
      &r, felix_client_group_dead_letters(g->client, g->f->tenant, g->f->ns,
                                          g->f->stream, g->shard, g->group, NULL,
                                          on_dead_letters, &r));
  CHECK(status == FELIX_STATUS_OK, "dead_letters gave %d (%s)", (int)status,
        r.w.message);
  size_t count = status == FELIX_STATUS_OK ? r.count : 0;
  for (size_t i = 0; i < count && i < MAX_GROUP_RECORDS; i++) {
    offsets[i] = r.records[i].offset;
  }
  waiter_destroy(&r.w);
  return count < MAX_GROUP_RECORDS ? count : MAX_GROUP_RECORDS;
}

static bool g_describe(const struct gctx *g, FelixGroupInfo *info) {
  struct gres r;
  gres_init(&r);
  FelixStatus status = gwait(
      &r, felix_client_group_describe(g->client, g->f->tenant, g->f->ns,
                                      g->f->stream, g->shard, g->group, NULL,
                                      on_info, &r));
  CHECK(status == FELIX_STATUS_OK && r.has_info, "describe gave %d (%s)",
        (int)status, r.w.message);
  *info = r.info;
  bool ok = status == FELIX_STATUS_OK && r.has_info;
  waiter_destroy(&r.w);
  return ok;
}

static FelixStatus g_delete(const struct gctx *g, bool *existed) {
  struct gres r;
  gres_init(&r);
  FelixStatus status = gwait(
      &r, felix_client_group_delete(g->admin, g->f->tenant, g->f->ns,
                                    g->f->stream, g->shard, g->group, NULL,
                                    on_deleted, &r));
  *existed = r.existed;
  waiter_destroy(&r.w);
  return status;
}

/* Publishes `count` payloads to the group's shard in one batch. */
static void g_publish(const struct gctx *g, char (*payloads)[96],
                      size_t count) {
  uint8_t data[8 * 96];
  size_t lens[8], used = 0;
  for (size_t i = 0; i < count; i++) {
    lens[i] = strlen(payloads[i]);
    memcpy(data + used, payloads[i], lens[i]);
    used += lens[i];
  }
  struct batch_waiter b;
  batch_init(&b);
  FelixStatus status = batch_wait(
      &b, felix_client_publish_on_shard(g->client, g->f->tenant, g->f->ns,
                                        g->f->stream, g->shard, data, lens,
                                        count, FELIX_ACK_PER_BATCH, NULL,
                                        on_batch, &b));
  CHECK(status == FELIX_STATUS_OK, "group publish gave %d (%s)", (int)status,
        b.w.message);
  waiter_destroy(&b.w);
}

static void groups(const struct fixture *f, const char *run) {
  FelixClient *client = NULL;
  REQUIRE_OK(connect_sync(f->addrs, f->tenant, f->token, f->ca_file, &client));

  char name[128], idle_name[128], consumer[128];
  snprintf(name, sizeof name, "cg-%s", run);
  snprintf(idle_name, sizeof idle_name, "cg-%s-idle", run);
  snprintf(consumer, sizeof consumer, "consumer-%s", run);
  FelixClient *admin = NULL;
  if (connect_sync(f->addrs, f->tenant, f->group_operator_token, f->ca_file,
                   &admin) != FELIX_STATUS_OK) {
    CHECK(0, "the group operator could not connect");
    felix_client_free(client);
    return;
  }
  struct gctx g = {client, admin, f, 1, name};
  uint64_t offset = 0;
  bool moved = false;

  /* Creating reports the move; creating again leaves the group alone. Then
     skip to the tail so earlier scenarios' records are not the group's. */
  FelixStatus status = g_create(&g, FELIX_START_EARLIEST, &offset, &moved);
  CHECK(status == FELIX_STATUS_OK && moved, "create gave %d, moved %d",
        (int)status, (int)moved);
  status = g_create(&g, FELIX_START_EARLIEST, &offset, &moved);
  CHECK(status == FELIX_STATUS_OK && !moved, "a second create gave %d, moved %d",
        (int)status, (int)moved);
  status = g_seek(&g, FELIX_START_LATEST, &offset);
  CHECK(status == FELIX_STATUS_OK, "seek to latest gave %d", (int)status);

  char payloads[4][96];
  for (int i = 0; i < 4; i++) {
    snprintf(payloads[i], sizeof payloads[i], "cg-%s-rec%d", run, i);
  }
  g_publish(&g, payloads, 3);

  struct grecord got[4];
  if (g_collect(&g, consumer, 3, got) != 3) {
    felix_client_free(admin);
    felix_client_free(client);
    return;
  }
  for (int i = 0; i < 3; i++) {
    CHECK(strcmp(got[i].payload, payloads[i]) == 0,
          "record %d was '%s', want '%s'", i, got[i].payload, payloads[i]);
    CHECK(got[i].attempts == 1, "record %d first attempt was %u", i,
          (unsigned)got[i].attempts);
  }

  /* A nack with no delay hands the first record out again, one attempt on. */
  CHECK(g_settle(&g, "nack", got[0].offset, got[0].attempts, 0, NULL) ==
            FELIX_STATUS_OK,
        "nack failed");
  struct grecord again[1];
  if (g_collect(&g, consumer, 1, again) == 1) {
    CHECK(again[0].offset == got[0].offset, "redelivered offset %" PRIu64
          ", want %" PRIu64, again[0].offset, got[0].offset);
    CHECK(again[0].attempts == 2, "redelivery was attempt %u",
          (unsigned)again[0].attempts);

    uint64_t visible = 0;
    g_settle(&g, "extend", again[0].offset, again[0].attempts, 30000, &visible);
    CHECK(visible > 0, "extend reported a claim of %" PRIu64 " ms", visible);
    g_settle(&g, "ack", again[0].offset, 0, 0, NULL);
  }
  g_settle(&g, "ack", got[1].offset, 0, 0, NULL);

  /* A dead letter is listed, can be driven back, and is delivered again. */
  g_settle(&g, "dead_letter", got[2].offset, 0, 0, NULL);
  uint64_t letters[MAX_GROUP_RECORDS];
  size_t n = g_dead_letters(&g, letters);
  CHECK(n == 1 && letters[0] == got[2].offset, "dead letters: %zu listed", n);
  FelixGroupInfo info;
  if (g_describe(&g, &info)) {
    CHECK(info.dead_letters == 1, "describe counted %" PRIu64 " dead letters",
          info.dead_letters);
  }
  g_settle(&g, "redrive", got[2].offset, 0, 0, NULL);
  struct grecord redriven[1];
  if (g_collect(&g, consumer, 1, redriven) == 1) {
    CHECK(redriven[0].offset == got[2].offset, "redrove offset %" PRIu64
          ", want %" PRIu64, redriven[0].offset, got[2].offset);
    g_settle(&g, "ack", redriven[0].offset, 0, 0, NULL);
  }

  /* A dead letter can instead be discarded. */
  g_publish(&g, payloads + 3, 1);
  struct grecord last[1];
  if (g_collect(&g, consumer, 1, last) == 1) {
    g_settle(&g, "dead_letter", last[0].offset, 0, 0, NULL);
    g_settle(&g, "discard", last[0].offset, 0, 0, NULL);
    n = g_dead_letters(&g, letters);
    CHECK(n == 0, "%zu dead letters after a discard", n);
  }

  if (g_describe(&g, &info)) {
    CHECK(info.has_committed && info.committed <= info.tail,
          "describe: committed %" PRIu64 " (%d), tail %" PRIu64,
          info.committed, (int)info.has_committed, info.tail);
    CHECK(info.dead_letters == 0, "describe counted %" PRIu64 " dead letters",
          info.dead_letters);
  }

  status = g_seek(&g, FELIX_START_LATEST, &offset);
  CHECK(status == FELIX_STATUS_OK, "seek to latest gave %d", (int)status);

  /* Deleting says whether the group was there. */
  bool existed = false;
  status = g_delete(&g, &existed);
  CHECK(status == FELIX_STATUS_OK && existed, "delete gave %d, existed %d",
        (int)status, (int)existed);
  status = g_delete(&g, &existed);
  CHECK(status == FELIX_STATUS_OK && !existed,
        "a second delete gave %d, existed %d", (int)status, (int)existed);

  /* The fixture's stream has four shards. */
  struct gres r;
  gres_init(&r);
  status = gwait(&r, felix_client_stream_shards(client, f->tenant, f->ns,
                                                f->stream, NULL, on_shards, &r));
  CHECK(status == FELIX_STATUS_OK && r.shards == 4,
        "stream_shards gave %d, %u shards", (int)status, (unsigned)r.shards);
  waiter_destroy(&r.w);

  /* A poll waiting for records is cancelled through its token. */
  g.group = idle_name;
  status = g_create(&g, FELIX_START_LATEST, &offset, &moved);
  CHECK(status == FELIX_STATUS_OK, "create of the idle group gave %d",
        (int)status);
  gres_init(&r);
  FelixCancelToken *cancel = felix_cancel_token_new();
  status = felix_client_group_poll(client, f->tenant, f->ns, f->stream, 1,
                                   idle_name, 1, 10000, 0, NULL, false, cancel,
                                   on_group_poll, &r);
  CHECK(status == FELIX_STATUS_OK, "starting a long poll gave %d", (int)status);
  if (status == FELIX_STATUS_OK) {
    pause_ms(100);
    felix_cancel_token_cancel(cancel);
    status = wait_for(&r.w, WAIT_MS);
    CHECK(status == FELIX_STATUS_CANCELLED, "a cancelled poll gave %d",
          (int)status);
    CHECK(r.count == 0, "a cancelled poll returned %zu records", r.count);
  }
  felix_cancel_token_free(cancel);
  waiter_destroy(&r.w);
  g_delete(&g, &existed);

  /* Arguments the call itself refuses; the callback never runs. */
  gres_init(&r);
  status = felix_client_group_poll(client, f->tenant, f->ns, f->stream, 1,
                                   idle_name, 1, 0, 0, NULL, true, NULL,
                                   on_group_poll, &r);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT,
        "reclaim without a consumer gave %d", (int)status);
  status = felix_client_group_poll(client, f->tenant, f->ns, f->stream, 1,
                                   idle_name, 0, 0, 0, NULL, false, NULL,
                                   on_group_poll, &r);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "max_records 0 gave %d",
        (int)status);
  status = felix_client_group_extend(client, f->tenant, f->ns, f->stream, 1,
                                     idle_name, 0, 1, 0, NULL, on_extend, &r);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "extend of 0 ms gave %d",
        (int)status);
  pause_ms(100);
  CHECK(r.w.calls == 0, "a refused group call ran its callback");
  waiter_destroy(&r.w);

  felix_client_free(admin);
  felix_client_free(client);
}

int main(int argc, char **argv) {
  if (argc == 2 && strcmp(argv[1], "offline") == 0) {
    offline();
  } else if (argc == 11 && strcmp(argv[1], "fixture") == 0) {
    struct fixture f = {argv[2], argv[3], argv[4], argv[5], argv[6],
                        argv[7], argv[8], argv[9], argv[10]};
    /* Nanoseconds too: the shared and static builds run within a second of
       each other against the same fixture, and group names must not repeat. */
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
    char run[32];
    snprintf(run, sizeof run, "%ld-%09ld", (long)ts.tv_sec, (long)ts.tv_nsec);
    pubsub(&f, run);
    batches(&f, run);
    subscriptions(&f, run);
    groups(&f, run);
  } else {
    fprintf(stderr, "usage: %s offline | fixture ADDRS TENANT NAMESPACE TOKEN "
                    "CA_FILE STREAM MISSING UNAUTHORIZED_TOKEN "
                    "GROUP_OPERATOR_TOKEN\n",
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
