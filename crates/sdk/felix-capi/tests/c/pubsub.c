/*
 * Drives the C ABI the way a C, Go or C# caller would: through the header and
 * a linked library, with no Rust in sight.
 *
 *   pubsub offline
 *     Argument checking, the last-error channel, and connects that fail or
 *     are cancelled. Needs no broker.
 *
 *   pubsub fixture ADDRS TENANT NAMESPACE TOKEN CA_FILE STREAM MISSING UNAUTHORIZED_TOKEN GROUP_OPERATOR_TOKEN CACHE SINGLE_SHARD_CACHE
 *     The publish, batch, subscribe, subscription, group, cache and commit scenarios, against `felix-cluster
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

static void on_producer(void *user_data, FelixStatus status,
                        const char *message, FelixProducer *producer) {
  finish(user_data, status, message, producer);
}

static void on_producer_done(void *user_data, FelixStatus status,
                             const char *message) {
  finish(user_data, status, message, NULL);
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
      *unauthorized_token, *group_operator_token, *cache, *single_shard_cache;
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
  status = felix_client_idempotent_producer(NULL, NULL, on_producer, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null producer client gave %d",
        (int)status);
  status = felix_producer_publish(NULL, "t", "n", "s", NULL, NULL, 1, NULL, 0,
                                  NULL, on_publish, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null producer gave %d",
        (int)status);
  status = felix_producer_publish_batches(NULL, "t", "n", "s", NULL, NULL, 1,
                                          NULL, 1, NULL, on_producer_done, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null producer batches gave %d",
        (int)status);
  status = felix_producer_abandon_in_doubt(NULL, "t", "n", "s", NULL,
                                           on_producer_done, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null producer abandon gave %d",
        (int)status);
  status = felix_producer_close(NULL, -1, NULL, on_producer_done, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null producer close gave %d",
        (int)status);
  uint64_t producer_id = 0;
  status = felix_producer_id(NULL, &producer_id);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null producer id gave %d",
        (int)status);
  status = felix_producer_id_for(NULL, "t", "n", "s", &producer_id);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null producer id_for gave %d",
        (int)status);
  felix_producer_free(NULL);

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

/* ---- caches, counters and watches --------------------------------------- */

/* Copies `len` bytes of a callback's borrowed data into a NUL-terminated
   buffer, truncating. The data is gone once the callback returns. */
static void copy_bounded(char *dst, size_t cap, const void *src, size_t len) {
  if (len >= cap) {
    len = cap - 1;
  }
  if (len) {
    memcpy(dst, src, len);
  }
  dst[len] = '\0';
}

/* One cache call's result; each call fills the fields it reports. */
struct cres {
  struct waiter w;
  bool found;
  char value[96];
  size_t len;
  uint64_t version;
  bool applied;
  bool has_version;
  bool has_counter;
  int64_t counter;
};

static void cres_init(struct cres *r) {
  memset(r, 0, sizeof *r);
  waiter_init(&r->w);
}

static void on_cache_done(void *user_data, FelixStatus status,
                          const char *message) {
  struct cres *r = user_data;
  finish(&r->w, status, message, NULL);
}

static void on_cache_value(void *user_data, FelixStatus status,
                           const char *message, bool found, const uint8_t *value,
                           size_t len) {
  struct cres *r = user_data;
  r->found = found;
  r->len = len;
  copy_bounded(r->value, sizeof r->value, value, len);
  finish(&r->w, status, message, NULL);
}

static void on_cache_versioned(void *user_data, FelixStatus status,
                               const char *message, bool found,
                               const uint8_t *value, size_t len,
                               uint64_t version) {
  struct cres *r = user_data;
  r->version = version;
  on_cache_value(user_data, status, message, found, value, len);
}

static void on_cache_condition(void *user_data, FelixStatus status,
                               const char *message, bool applied,
                               bool has_version, uint64_t version) {
  struct cres *r = user_data;
  r->applied = applied;
  r->has_version = has_version;
  r->version = version;
  finish(&r->w, status, message, NULL);
}

static void on_cache_counter(void *user_data, FelixStatus status,
                             const char *message, bool has_value,
                             int64_t value) {
  struct cres *r = user_data;
  r->has_counter = has_value;
  r->counter = value;
  finish(&r->w, status, message, NULL);
}

/* The callback's status, or the call's when it was refused. Leaves `r` for
   the caller to read; destroy it after. */
static FelixStatus cres_wait(struct cres *r, FelixStatus started) {
  return started == FELIX_STATUS_OK ? wait_for(&r->w, WAIT_MS) : started;
}

struct cctx {
  FelixClient *client;
  const struct fixture *f;
  const char *cache;
};

static FelixStatus c_put(const struct cctx *c, const char *key,
                         const char *value, uint64_t ttl_ms) {
  struct cres r;
  cres_init(&r);
  FelixStatus status = cres_wait(
      &r, felix_client_cache_put(c->client, c->f->tenant, c->f->ns, c->cache,
                                 key, (const uint8_t *)value, strlen(value),
                                 ttl_ms, NULL, on_cache_done, &r));
  waiter_destroy(&r.w);
  return status;
}

static FelixStatus c_get(const struct cctx *c, const char *key, bool versioned,
                         struct cres *out) {
  cres_init(out);
  FelixStatus started =
      versioned ? felix_client_cache_get_versioned(
                      c->client, c->f->tenant, c->f->ns, c->cache, key, NULL,
                      on_cache_versioned, out)
                : felix_client_cache_get(c->client, c->f->tenant, c->f->ns,
                                         c->cache, key, NULL, on_cache_value,
                                         out);
  FelixStatus status = cres_wait(out, started);
  waiter_destroy(&out->w);
  return status;
}

static FelixStatus c_delete(const struct cctx *c, const char *key,
                            struct cres *out) {
  cres_init(out);
  FelixStatus status = cres_wait(
      out, felix_client_cache_delete(c->client, c->f->tenant, c->f->ns,
                                     c->cache, key, NULL, on_cache_value, out));
  waiter_destroy(&out->w);
  return status;
}

static FelixStatus c_put_if(const struct cctx *c, const char *key,
                            const char *value, FelixCacheCondition kind,
                            uint64_t version, struct cres *out) {
  cres_init(out);
  FelixStatus status = cres_wait(
      out, felix_client_cache_put_if(
               c->client, c->f->tenant, c->f->ns, c->cache, key,
               (const uint8_t *)value, strlen(value), 0, kind, version, NULL,
               on_cache_condition, out));
  waiter_destroy(&out->w);
  return status;
}

static FelixStatus c_delete_if(const struct cctx *c, const char *key,
                               uint64_t version, struct cres *out) {
  cres_init(out);
  FelixStatus status = cres_wait(
      out, felix_client_cache_delete_if(c->client, c->f->tenant, c->f->ns,
                                        c->cache, key, version, NULL,
                                        on_cache_condition, out));
  waiter_destroy(&out->w);
  return status;
}

static FelixStatus c_add(const struct cctx *c, const char *key, int64_t delta,
                         struct cres *out) {
  cres_init(out);
  FelixStatus status = cres_wait(
      out, felix_client_counter_add(c->client, c->f->tenant, c->f->ns, c->cache,
                                    key, delta, NULL, on_cache_counter, out));
  waiter_destroy(&out->w);
  return status;
}

static FelixStatus c_counter(const struct cctx *c, const char *key,
                             struct cres *out) {
  cres_init(out);
  FelixStatus status = cres_wait(
      out, felix_client_counter_get(c->client, c->f->tenant, c->f->ns, c->cache,
                                    key, NULL, on_cache_counter, out));
  waiter_destroy(&out->w);
  return status;
}

/* One watch read, copied out of the callback. */
struct witem {
  struct waiter w;
  bool has_item;
  FelixWatchItemKind kind;
  bool has_shard;
  uint32_t shard;
  char key[160];
  bool has_value;
  char value[96];
  uint64_t offset;
  uint64_t expires_at_millis;
  bool has_resume_from;
  uint64_t resume_from;
};

static void on_watch_open(void *user_data, FelixStatus status,
                          const char *message, FelixCacheWatch *watch) {
  finish(user_data, status, message, watch);
}

static void on_watch_item(void *user_data, FelixStatus status,
                          const char *message, const FelixWatchItem *item) {
  struct witem *it = user_data;
  it->has_item = item != NULL;
  if (item) {
    it->kind = item->kind;
    it->has_shard = item->has_shard;
    it->shard = item->shard;
    copy_bounded(it->key, sizeof it->key, item->key, item->key_len);
    it->has_value = item->has_value;
    copy_bounded(it->value, sizeof it->value, item->value, item->value_len);
    it->offset = item->offset;
    it->expires_at_millis = item->expires_at_millis;
    it->has_resume_from = item->has_resume_from;
    it->resume_from = item->resume_from;
  }
  finish(&it->w, status, message, NULL);
}

static void witem_init(struct witem *it) {
  memset(it, 0, sizeof *it);
  waiter_init(&it->w);
}

/* Reads one item, waiting up to `timeout_ms` (negative waits for ever). */
static FelixStatus w_next(FelixCacheWatch *watch, int64_t timeout_ms,
                          struct witem *out) {
  witem_init(out);
  FelixStatus status = felix_cache_watch_next(watch, timeout_ms, NULL,
                                              on_watch_item, out);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&out->w, WAIT_MS);
  }
  waiter_destroy(&out->w);
  return status;
}

static FelixCacheWatch *w_open(const struct cctx *c, FelixWatchFilter kind,
                               const char *filter, bool retained) {
  struct waiter w;
  waiter_init(&w);
  FelixStatus status = felix_client_watch_cache(
      c->client, c->f->tenant, c->f->ns, c->cache, kind, filter, retained,
      false, 0, NULL, on_watch_open, &w);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, WAIT_MS);
  }
  CHECK(status == FELIX_STATUS_OK, "watch_cache gave %d", (int)status);
  FelixCacheWatch *watch = status == FELIX_STATUS_OK ? w.handle : NULL;
  waiter_destroy(&w);
  return watch;
}

#define MAX_SHARDS 16

struct resume {
  uint32_t shards[MAX_SHARDS];
  uint64_t offsets[MAX_SHARDS];
  size_t count;
};

static FelixCacheWatch *w_open_sharded(const struct cctx *c, const char *prefix,
                                       bool retained, const struct resume *at) {
  struct waiter w;
  waiter_init(&w);
  FelixStatus status = felix_client_watch_cache_sharded(
      c->client, c->f->tenant, c->f->ns, c->cache, prefix, retained,
      at ? at->shards : NULL, at ? at->offsets : NULL, at ? at->count : 0, NULL,
      on_watch_open, &w);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, WAIT_MS);
  }
  CHECK(status == FELIX_STATUS_OK, "watch_cache_sharded gave %d", (int)status);
  FelixCacheWatch *watch = status == FELIX_STATUS_OK ? w.handle : NULL;
  waiter_destroy(&w);
  return watch;
}

static void caches(const struct fixture *f, const char *run) {
  FelixClient *client = NULL;
  REQUIRE_OK(connect_sync(f->addrs, f->tenant, f->token, f->ca_file, &client));
  struct cctx sharded = {client, f, f->cache};
  struct cctx single = {client, f, f->single_shard_cache};
  struct cres r;
  struct witem it;
  char key[160], key2[160], pfx[96];
  snprintf(pfx, sizeof pfx, "cc-%s-", run);

  /* Put, get and versioned get. */
  snprintf(key, sizeof key, "%sa", pfx);
  FelixStatus status = c_put(&sharded, key, "hello", 0);
  CHECK(status == FELIX_STATUS_OK, "put gave %d", (int)status);
  status = c_get(&sharded, key, false, &r);
  CHECK(status == FELIX_STATUS_OK && r.found && strcmp(r.value, "hello") == 0,
        "get gave %d, found %d, '%s'", (int)status, (int)r.found, r.value);
  status = c_get(&sharded, key, true, &r);
  CHECK(status == FELIX_STATUS_OK && r.found && strcmp(r.value, "hello") == 0,
        "versioned get gave %d, found %d, '%s'", (int)status, (int)r.found,
        r.value);
  uint64_t v1 = r.version;
  snprintf(key2, sizeof key2, "%smissing", pfx);
  status = c_get(&sharded, key2, false, &r);
  CHECK(status == FELIX_STATUS_OK && !r.found, "a missing key gave %d, found %d",
        (int)status, (int)r.found);

  /* Conditional writes: a failed condition is OK with applied false. */
  status = c_put_if(&sharded, key, "again", FELIX_IF_ABSENT, 0, &r);
  CHECK(status == FELIX_STATUS_OK && !r.applied,
        "put_if absent on a live key gave %d, applied %d", (int)status,
        (int)r.applied);
  snprintf(key2, sizeof key2, "%sb", pfx);
  status = c_put_if(&sharded, key2, "new", FELIX_IF_ABSENT, 0, &r);
  CHECK(status == FELIX_STATUS_OK && r.applied,
        "put_if absent on a free key gave %d, applied %d", (int)status,
        (int)r.applied);
  status = c_put_if(&sharded, key, "hello2", FELIX_IF_VERSION, v1, &r);
  CHECK(status == FELIX_STATUS_OK && r.applied,
        "put_if at the current version gave %d, applied %d", (int)status,
        (int)r.applied);
  status = c_put_if(&sharded, key, "hello3", FELIX_IF_VERSION, v1, &r);
  CHECK(status == FELIX_STATUS_OK && !r.applied,
        "put_if at a stale version gave %d, applied %d", (int)status,
        (int)r.applied);
  status = c_delete_if(&sharded, key, v1, &r);
  CHECK(status == FELIX_STATUS_OK && !r.applied,
        "delete_if at a stale version gave %d, applied %d", (int)status,
        (int)r.applied);
  status = c_get(&sharded, key, true, &r);
  CHECK(status == FELIX_STATUS_OK && r.found && strcmp(r.value, "hello2") == 0 &&
            r.version != v1,
        "the value after the conditions was '%s' at %" PRIu64, r.value,
        r.version);
  status = c_delete_if(&sharded, key, r.version, &r);
  CHECK(status == FELIX_STATUS_OK && r.applied,
        "delete_if at the current version gave %d, applied %d", (int)status,
        (int)r.applied);
  status = c_get(&sharded, key, false, &r);
  CHECK(status == FELIX_STATUS_OK && !r.found, "a deleted key was found");

  /* Delete returns the value it removed. */
  status = c_delete(&sharded, key2, &r);
  CHECK(status == FELIX_STATUS_OK && r.found && strcmp(r.value, "new") == 0,
        "delete gave %d, found %d, '%s'", (int)status, (int)r.found, r.value);
  status = c_delete(&sharded, key2, &r);
  CHECK(status == FELIX_STATUS_OK && !r.found,
        "a second delete gave %d, found %d", (int)status, (int)r.found);

  /* Counters: missing, then added to. */
  snprintf(key, sizeof key, "%scount", pfx);
  status = c_counter(&sharded, key, &r);
  CHECK(status == FELIX_STATUS_OK && !r.has_counter,
        "a missing counter gave %d, has %d", (int)status, (int)r.has_counter);
  status = c_add(&sharded, key, 5, &r);
  CHECK(status == FELIX_STATUS_OK && r.has_counter && r.counter == 5,
        "first add gave %d, %" PRId64, (int)status, r.counter);
  status = c_add(&sharded, key, -2, &r);
  CHECK(status == FELIX_STATUS_OK && r.has_counter && r.counter == 3,
        "second add gave %d, %" PRId64, (int)status, r.counter);
  status = c_counter(&sharded, key, &r);
  CHECK(status == FELIX_STATUS_OK && r.has_counter && r.counter == 3,
        "counter get gave %d, %" PRId64, (int)status, r.counter);

  /* A key watch sees a put with its expiry, then the delete. */
  snprintf(key, sizeof key, "%swatched", pfx);
  FelixCacheWatch *watch = w_open(&sharded, FELIX_WATCH_KEY, key, false);
  if (watch) {
    CHECK(c_put(&sharded, key, "w1", 60000) == FELIX_STATUS_OK, "watched put");
    status = w_next(watch, 10000, &it);
    CHECK(status == FELIX_STATUS_OK && it.has_item &&
              it.kind == FELIX_WATCH_CHANGE && strcmp(it.key, key) == 0 &&
              it.has_value && strcmp(it.value, "w1") == 0 &&
              it.expires_at_millis > 0 && !it.has_shard,
          "the put arrived as status %d kind %d key '%s' value '%s' expiry %" PRIu64,
          (int)status, (int)it.kind, it.key, it.value, it.expires_at_millis);
    CHECK(c_delete(&sharded, key, &r) == FELIX_STATUS_OK, "watched delete");
    status = w_next(watch, 10000, &it);
    CHECK(status == FELIX_STATUS_OK && it.has_item &&
              it.kind == FELIX_WATCH_CHANGE && strcmp(it.key, key) == 0 &&
              !it.has_value,
          "the delete arrived as status %d kind %d has_value %d", (int)status,
          (int)it.kind, (int)it.has_value);
    /* Nothing more is waiting. */
    status = w_next(watch, 150, &it);
    CHECK(status == FELIX_STATUS_TIMEOUT, "an idle watch gave %d", (int)status);
    status = felix_cache_watch_resume_offsets(watch, NULL, NULL, 0, NULL);
    CHECK(status == FELIX_STATUS_INVALID_ARGUMENT,
          "resume offsets of a single watch gave %d", (int)status);
    felix_cache_watch_free(watch);
  }

  /* A retained prefix watch delivers what is stored, then live changes. */
  for (int i = 1; i <= 2; i++) {
    snprintf(key, sizeof key, "%sk%d", pfx, i);
    CHECK(c_put(&single, key, "stored", 0) == FELIX_STATUS_OK, "stored put %d",
          i);
  }
  watch = w_open(&single, FELIX_WATCH_PREFIX, pfx, true);
  if (watch) {
    uint64_t retained = 0;
    bool has_retained = false;
    status = felix_cache_watch_retained_count(watch, &retained, &has_retained);
    CHECK(status == FELIX_STATUS_OK && has_retained && retained >= 2,
          "retained count gave %d, has %d, %" PRIu64, (int)status,
          (int)has_retained, retained);
    int seen = 0;
    for (int i = 0; i < 2; i++) {
      status = w_next(watch, 10000, &it);
      CHECK(status == FELIX_STATUS_OK && it.has_item &&
                it.kind == FELIX_WATCH_CHANGE && it.has_value &&
                strncmp(it.key, pfx, strlen(pfx)) == 0,
            "retained item %d gave %d kind %d key '%s'", i, (int)status,
            (int)it.kind, it.key);
      seen += it.key[strlen(pfx) + 1] - '0';
    }
    CHECK(seen == 3, "the retained keys were not k1 and k2 (sum %d)", seen);
    snprintf(key, sizeof key, "%sk3", pfx);
    CHECK(c_put(&single, key, "live", 0) == FELIX_STATUS_OK, "live put");
    status = w_next(watch, 10000, &it);
    CHECK(status == FELIX_STATUS_OK && it.kind == FELIX_WATCH_CHANGE &&
              strcmp(it.key, key) == 0 && strcmp(it.value, "live") == 0,
          "the live change gave %d kind %d key '%s'", (int)status, (int)it.kind,
          it.key);
    felix_cache_watch_free(watch);
  }

  /* A sharded watch sees puts spread over the shards; resuming from its
     offsets sees only what came after. */
  watch = w_open_sharded(&sharded, pfx, false, NULL);
  struct resume at;
  memset(&at, 0, sizeof at);
  if (watch) {
    unsigned seen_mask = 0;
    for (int i = 0; i < 8; i++) {
      snprintf(key, sizeof key, "%ss%d", pfx, i);
      CHECK(c_put(&sharded, key, "spread", 0) == FELIX_STATUS_OK, "spread put %d",
            i);
    }
    for (int i = 0; i < 8; i++) {
      status = w_next(watch, 10000, &it);
      CHECK(status == FELIX_STATUS_OK && it.has_item &&
                it.kind == FELIX_WATCH_CHANGE && it.has_shard &&
                strncmp(it.key, pfx, strlen(pfx)) == 0,
            "spread item %d gave %d kind %d key '%s'", i, (int)status,
            (int)it.kind, it.key);
      if (strlen(it.key) == strlen(pfx) + 2) {
        seen_mask |= 1u << (it.key[strlen(pfx) + 1] - '0');
      }
    }
    CHECK(seen_mask == 0xff, "the watch saw keys %#x of 8", seen_mask);
    status = felix_cache_watch_resume_offsets(watch, at.shards, at.offsets,
                                              MAX_SHARDS, &at.count);
    CHECK(status == FELIX_STATUS_OK && at.count >= 1 && at.count <= MAX_SHARDS,
          "resume offsets gave %d, %zu shards", (int)status, at.count);
    felix_cache_watch_free(watch);

    watch = w_open_sharded(&sharded, pfx, false, &at);
    if (watch) {
      for (int i = 0; i < 2; i++) {
        snprintf(key, sizeof key, "%slater%d", pfx, i);
        CHECK(c_put(&sharded, key, "later", 0) == FELIX_STATUS_OK, "later put");
      }
      for (int i = 0; i < 2; i++) {
        status = w_next(watch, 10000, &it);
        CHECK(status == FELIX_STATUS_OK && it.has_item &&
                  it.kind == FELIX_WATCH_CHANGE &&
                  strncmp(it.key + strlen(pfx), "later", 5) == 0,
              "resumed item %d gave %d kind %d key '%s'", i, (int)status,
              (int)it.kind, it.key);
      }
      status = w_next(watch, 300, &it);
      CHECK(status == FELIX_STATUS_TIMEOUT,
            "the resumed watch had more than the later puts (%d, key '%s')",
            (int)status, it.key);
      felix_cache_watch_free(watch);
    }
  }

  /* A sharded retained watch delivers every shard's state, then says so. */
  watch = w_open_sharded(&sharded, pfx, true, NULL);
  if (watch) {
    int changes = 0;
    bool complete = false;
    for (int i = 0; i < 64 && !complete; i++) {
      status = w_next(watch, 10000, &it);
      if (status != FELIX_STATUS_OK || !it.has_item) {
        CHECK(0, "retained read %d gave %d", i, (int)status);
        break;
      }
      if (it.kind == FELIX_WATCH_STATE_COMPLETE) {
        complete = true;
        CHECK(!it.has_shard, "state complete named a shard");
      } else if (it.kind == FELIX_WATCH_CHANGE &&
                 strncmp(it.key, pfx, strlen(pfx)) == 0 &&
                 (it.key[strlen(pfx)] == 's' || it.key[strlen(pfx)] == 'l')) {
        /* Only the keys the spread and resume steps stored. */
        changes++;
      }
    }
    /* s0..s7 and later0..later1 are stored; the deleted keys are not. */
    CHECK(complete && changes == 10,
          "the retained watch delivered %d changes, complete %d", changes,
          (int)complete);
    felix_cache_watch_free(watch);
  }

  /* A pending read is cancelled through its token and loses nothing. */
  snprintf(key, sizeof key, "%scancel", pfx);
  watch = w_open(&sharded, FELIX_WATCH_KEY, key, false);
  if (watch) {
    witem_init(&it);
    FelixCancelToken *cancel = felix_cancel_token_new();
    status = felix_cache_watch_next(watch, -1, cancel, on_watch_item, &it);
    CHECK(status == FELIX_STATUS_OK, "starting a long read gave %d", (int)status);
    if (status == FELIX_STATUS_OK) {
      pause_ms(100);
      /* One read at a time. */
      struct witem second;
      witem_init(&second);
      status = felix_cache_watch_next(watch, 0, NULL, on_watch_item, &second);
      CHECK(status == FELIX_STATUS_INVALID_ARGUMENT,
            "a second pending read gave %d", (int)status);
      waiter_destroy(&second.w);
      felix_cancel_token_cancel(cancel);
      status = wait_for(&it.w, WAIT_MS);
      CHECK(status == FELIX_STATUS_CANCELLED, "a cancelled read gave %d",
            (int)status);
    }
    waiter_destroy(&it.w);
    felix_cancel_token_free(cancel);
    CHECK(c_put(&sharded, key, "after", 0) == FELIX_STATUS_OK, "put after cancel");
    status = w_next(watch, 10000, &it);
    CHECK(status == FELIX_STATUS_OK && it.has_item &&
              strcmp(it.value, "after") == 0,
          "the read after a cancel gave %d", (int)status);

    /* Freeing the watch under a pending read cancels it. */
    witem_init(&it);
    status = felix_cache_watch_next(watch, -1, NULL, on_watch_item, &it);
    CHECK(status == FELIX_STATUS_OK, "starting a read to free under gave %d",
          (int)status);
    if (status == FELIX_STATUS_OK) {
      pause_ms(100);
      felix_cache_watch_free(watch);
      status = wait_for(&it.w, WAIT_MS);
      CHECK(status == FELIX_STATUS_CANCELLED, "a freed watch's read gave %d",
            (int)status);
    } else {
      felix_cache_watch_free(watch);
    }
    waiter_destroy(&it.w);
  }

  /* Arguments the call itself refuses; the callback never runs. */
  struct waiter w;
  waiter_init(&w);
  status = felix_client_watch_cache(client, f->tenant, f->ns, f->cache,
                                    FELIX_WATCH_PREFIX, pfx, true, true, 5, NULL,
                                    on_watch_open, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT,
        "retained with a from_offset gave %d", (int)status);
  status = felix_client_watch_cache(client, f->tenant, f->ns, f->cache, 7, pfx,
                                    false, false, 0, NULL, on_watch_open, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "an unknown filter gave %d",
        (int)status);
  uint32_t dup_shards[2] = {1, 1};
  uint64_t dup_offsets[2] = {0, 0};
  status = felix_client_watch_cache_sharded(client, f->tenant, f->ns, f->cache,
                                            pfx, false, dup_shards, dup_offsets,
                                            2, NULL, on_watch_open, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT,
        "a shard listed twice gave %d", (int)status);
  status = felix_client_watch_cache_sharded(client, f->tenant, f->ns, f->cache,
                                            pfx, true, dup_shards, dup_offsets,
                                            1, NULL, on_watch_open, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT,
        "retained with resume offsets gave %d", (int)status);
  struct cres cr;
  cres_init(&cr);
  status = felix_client_cache_put_if(client, f->tenant, f->ns, f->cache, pfx,
                                     (const uint8_t *)"x", 1, 0, 9, 0, NULL,
                                     on_cache_condition, &cr);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT,
        "an unknown condition kind gave %d", (int)status);
  pause_ms(100);
  CHECK(w.calls == 0 && cr.w.calls == 0, "a refused cache call ran its callback");
  waiter_destroy(&cr.w);
  waiter_destroy(&w);

  /* Clean up what this run stored. */
  const char *stored[] = {"k1", "k2", "k3"};
  for (size_t i = 0; i < sizeof stored / sizeof *stored; i++) {
    snprintf(key, sizeof key, "%s%s", pfx, stored[i]);
    c_delete(&single, key, &r);
  }
  felix_client_free(client);
}

/* ---- atomic commits, conditional publishes and state reads ------------- */

/* One commit-family call's result; each call fills the fields it reports. */
struct kres {
  struct waiter w;
  uint64_t offset;
  bool written;
  bool has_state;
  bool has_value;
  char value[64];
  bool has_version;
  uint64_t version;
  bool has_as_of;
  uint64_t as_of;
};

static void kres_init(struct kres *r) {
  memset(r, 0, sizeof *r);
  waiter_init(&r->w);
}

static void on_commit_done(void *user_data, FelixStatus status,
                           const char *message, uint64_t offset) {
  struct kres *r = user_data;
  r->offset = offset;
  finish(&r->w, status, message, NULL);
}

static void on_conditional_done(void *user_data, FelixStatus status,
                                const char *message, bool written,
                                uint64_t offset) {
  struct kres *r = user_data;
  r->written = written;
  r->offset = offset;
  finish(&r->w, status, message, NULL);
}

static void on_state_done(void *user_data, FelixStatus status,
                          const char *message, const FelixStateValue *state) {
  struct kres *r = user_data;
  r->has_state = state != NULL;
  if (state) {
    r->has_value = state->has_value;
    copy_bounded(r->value, sizeof r->value, state->value, state->value_len);
    r->has_version = state->has_version;
    r->version = state->version;
    r->has_as_of = state->has_as_of;
    r->as_of = state->as_of;
  }
  finish(&r->w, status, message, NULL);
}

static FelixStatus kres_wait(struct kres *r, FelixStatus started) {
  FelixStatus status =
      started == FELIX_STATUS_OK ? wait_for(&r->w, WAIT_MS) : started;
  waiter_destroy(&r->w);
  return status;
}

struct kctx {
  FelixClient *client;
  const struct fixture *f;
};

/* A commit of one event and one state change: a put of `value`, or a delete
   when `value` is NULL. `shard` < 0 routes by the entity key. `expected` is
   used by the conditional forms. */
static FelixStatus k_commit(const struct kctx *k, int64_t shard,
                            const char *entity, const char *event,
                            const char *key, const char *value, bool conditional,
                            uint64_t expected, struct kres *r) {
  FelixCommitOp ops[2];
  memset(ops, 0, sizeof ops);
  ops[0].kind = FELIX_COMMIT_PUBLISH;
  ops[0].stream = k->f->stream;
  ops[0].data = (const uint8_t *)event;
  ops[0].data_len = strlen(event);
  ops[1].kind = value ? FELIX_COMMIT_PUT : FELIX_COMMIT_DELETE;
  ops[1].stream = k->f->stream;
  ops[1].key = key;
  ops[1].data = (const uint8_t *)value;
  ops[1].data_len = value ? strlen(value) : 0;
  const uint8_t *ek = (const uint8_t *)entity;
  size_t ek_len = strlen(entity);
  kres_init(r);
  FelixStatus started;
  if (conditional) {
    started = shard < 0
                  ? felix_client_commit_if(k->client, k->f->tenant, k->f->ns,
                                           ek, ek_len, ops, 2, expected, NULL,
                                           on_conditional_done, r)
                  : felix_client_commit_if_on_shard(
                        k->client, k->f->tenant, k->f->ns, (uint32_t)shard, ek,
                        ek_len, ops, 2, expected, NULL, on_conditional_done, r);
  } else {
    started = shard < 0
                  ? felix_client_commit(k->client, k->f->tenant, k->f->ns, ek,
                                        ek_len, ops, 2, NULL, on_commit_done, r)
                  : felix_client_commit_on_shard(
                        k->client, k->f->tenant, k->f->ns, (uint32_t)shard, ek,
                        ek_len, ops, 2, NULL, on_commit_done, r);
  }
  return kres_wait(r, started);
}

static FelixStatus k_state(const struct kctx *k, int64_t shard,
                           const char *entity, const char *key,
                           struct kres *r) {
  kres_init(r);
  FelixStatus started =
      shard < 0 ? felix_client_state_get(k->client, k->f->tenant, k->f->ns,
                                         k->f->stream, (const uint8_t *)entity,
                                         strlen(entity), key, NULL,
                                         on_state_done, r)
                : felix_client_state_get_on_shard(
                      k->client, k->f->tenant, k->f->ns, k->f->stream,
                      (uint32_t)shard, key, NULL, on_state_done, r);
  return kres_wait(r, started);
}

/* A conditional publish of two records. */
static FelixStatus k_publish_if(const struct kctx *k, int64_t shard,
                                const char *key, uint64_t expected,
                                struct kres *r) {
  const uint8_t data[] = {'p', 'i', '-', '1', 'p', 'i', '-', '2'};
  const size_t lens[2] = {4, 4};
  kres_init(r);
  FelixStatus started =
      shard < 0
          ? felix_client_publish_if(k->client, k->f->tenant, k->f->ns,
                                    k->f->stream, (const uint8_t *)key,
                                    strlen(key), data, lens, 2, expected, NULL,
                                    on_conditional_done, r)
          : felix_client_publish_if_on_shard(
                k->client, k->f->tenant, k->f->ns, k->f->stream,
                (uint32_t)shard, data, lens, 2, expected, NULL,
                on_conditional_done, r);
  return kres_wait(r, started);
}

/* A conditional write at a stale offset is refused with the tail; one at the
   tail is written there. `write` runs the form under test. */
typedef FelixStatus (*conditional_fn)(const struct kctx *k, int64_t shard,
                                      const char *tag, uint64_t expected,
                                      struct kres *r);

static FelixStatus cond_commit(const struct kctx *k, int64_t shard,
                               const char *tag, uint64_t expected,
                               struct kres *r) {
  return k_commit(k, shard, tag, tag, tag, "cond", true, expected, r);
}

static FelixStatus cond_publish(const struct kctx *k, int64_t shard,
                                const char *tag, uint64_t expected,
                                struct kres *r) {
  return k_publish_if(k, shard, tag, expected, r);
}

static void check_conditional(const struct kctx *k, const char *what,
                              conditional_fn write, int64_t shard,
                              const char *tag) {
  struct kres r;
  /* No shard's tail is this large, so the first write is always stale. */
  FelixStatus status = write(k, shard, tag, UINT64_MAX, &r);
  CHECK(status == FELIX_STATUS_OK && !r.written,
        "%s at a stale offset gave %d, written %d", what, (int)status,
        (int)r.written);
  uint64_t tail = r.offset;
  status = write(k, shard, tag, tail, &r);
  CHECK(status == FELIX_STATUS_OK && r.written && r.offset == tail,
        "%s at the tail %" PRIu64 " gave %d, written %d at %" PRIu64, what, tail,
        (int)status, (int)r.written, r.offset);
  uint64_t landed = r.offset;
  status = write(k, shard, tag, tail, &r);
  CHECK(status == FELIX_STATUS_OK && !r.written && r.offset > landed,
        "%s again at %" PRIu64 " gave %d, written %d, tail %" PRIu64, what, tail,
        (int)status, (int)r.written, r.offset);
}

static void commits(const struct fixture *f, const char *run) {
  FelixClient *client = NULL;
  REQUIRE_OK(connect_sync(f->addrs, f->tenant, f->token, f->ca_file, &client));
  struct kctx k = {client, f};
  struct kres r;
  char key[160], other[160];
  snprintf(key, sizeof key, "cm-%s-a", run);

  /* A commit returns its offset, and its state reads back at that version. */
  FelixStatus status = k_commit(&k, -1, key, "placed", key, "placed", false, 0, &r);
  CHECK(status == FELIX_STATUS_OK, "commit gave %d", (int)status);
  uint64_t offset = r.offset;
  status = k_state(&k, -1, key, key, &r);
  CHECK(status == FELIX_STATUS_OK && r.has_state && r.has_value &&
            strcmp(r.value, "placed") == 0 && r.has_version &&
            r.version == offset && r.has_as_of && r.as_of >= offset,
        "state gave %d, has_value %d '%s', version %" PRIu64 " (commit at %" PRIu64 ")",
        (int)status, (int)r.has_value, r.value, r.version, offset);

  /* Deleting the key leaves it absent. */
  status = k_commit(&k, -1, key, "cancelled", key, NULL, false, 0, &r);
  CHECK(status == FELIX_STATUS_OK, "delete commit gave %d", (int)status);
  status = k_state(&k, -1, key, key, &r);
  CHECK(status == FELIX_STATUS_OK && r.has_state && !r.has_value,
        "state after the delete gave %d, has_value %d", (int)status,
        (int)r.has_value);

  /* Conditional commits, routed by the entity key and by shard. */
  snprintf(other, sizeof other, "cm-%s-if", run);
  check_conditional(&k, "commit_if", cond_commit, -1, other);
  snprintf(other, sizeof other, "cm-%s-ifs", run);
  check_conditional(&k, "commit_if_on_shard", cond_commit, 2, other);

  /* A commit on a named shard reads back from that shard. */
  snprintf(other, sizeof other, "cm-%s-shard", run);
  status = k_commit(&k, 2, other, "on-shard", other, "shard-value", false, 0, &r);
  CHECK(status == FELIX_STATUS_OK, "commit_on_shard gave %d", (int)status);
  offset = r.offset;
  status = k_state(&k, 2, other, other, &r);
  CHECK(status == FELIX_STATUS_OK && r.has_value &&
            strcmp(r.value, "shard-value") == 0 && r.has_version &&
            r.version == offset,
        "state_get_on_shard gave %d, '%s', version %" PRIu64
        " (commit at %" PRIu64 ")",
        (int)status, r.value, r.version, offset);

  /* Conditional publishes. */
  snprintf(other, sizeof other, "cm-%s-pub", run);
  check_conditional(&k, "publish_if", cond_publish, -1, other);
  check_conditional(&k, "publish_if_on_shard", cond_publish, 3, other);

  /* Commits the call itself refuses; the callback never runs. */
  struct kres bad;
  kres_init(&bad);
  FelixCommitOp two_events[2];
  memset(two_events, 0, sizeof two_events);
  two_events[0].kind = FELIX_COMMIT_PUBLISH;
  two_events[0].stream = f->stream;
  two_events[1].kind = FELIX_COMMIT_ENQUEUE;
  two_events[1].stream = f->stream;
  status = felix_client_commit(client, f->tenant, f->ns,
                               (const uint8_t *)key, strlen(key), two_events, 2,
                               NULL, on_commit_done, &bad);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "two events gave %d",
        (int)status);
  FelixCommitOp split[2];
  memset(split, 0, sizeof split);
  split[0].kind = FELIX_COMMIT_PUBLISH;
  split[0].stream = f->stream;
  split[1].kind = FELIX_COMMIT_PUT;
  split[1].stream = f->missing;
  split[1].key = key;
  status = felix_client_commit(client, f->tenant, f->ns, (const uint8_t *)key,
                               strlen(key), split, 2, NULL, on_commit_done,
                               &bad);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "a second stream gave %d",
        (int)status);
  const char *message = felix_last_error_message();
  CHECK(message && strstr(message, f->missing),
        "the refusal does not name the stream");
  status = felix_client_commit(client, f->tenant, f->ns, (const uint8_t *)key,
                               strlen(key), NULL, 0, NULL, on_commit_done,
                               &bad);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "no operations gave %d",
        (int)status);
  two_events[1].kind = 9;
  status = felix_client_commit(client, f->tenant, f->ns, (const uint8_t *)key,
                               strlen(key), two_events, 2, NULL, on_commit_done,
                               &bad);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "an unknown kind gave %d",
        (int)status);
  status = felix_client_publish_if(client, f->tenant, f->ns, f->stream, NULL, 0,
                                   NULL, NULL, 0, 0, NULL, on_conditional_done,
                                   &bad);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT,
        "a conditional publish of nothing gave %d", (int)status);
  pause_ms(100);
  CHECK(bad.w.calls == 0, "a refused commit call ran its callback");
  waiter_destroy(&bad.w);
  felix_client_free(client);
}

/* ---- idempotent producers ---------------------------------------------- */

/* `count` payloads, each under 64 bytes, laid end to end. */
struct laid {
  char data[8 * 64];
  size_t lens[8];
  size_t count;
};

static void lay(struct laid *l, const char *run, const char *tag, size_t count) {
  size_t at = 0;
  l->count = count;
  for (size_t i = 0; i < count; i++) {
    int n = snprintf(l->data + at, 64, "pr-%s-%s-%zu", run, tag, i);
    l->lens[i] = (size_t)n;
    at += (size_t)n;
  }
}

static FelixStatus producer_publish(FelixProducer *p, const struct fixture *f,
                                    const struct laid *l, const char *key,
                                    struct waiter *w) {
  FelixStatus status = felix_producer_publish(
      p, f->tenant, f->ns, f->stream, (const uint8_t *)l->data, l->lens,
      l->count, (const uint8_t *)key, key ? strlen(key) : 0, NULL, on_publish,
      w);
  return status == FELIX_STATUS_OK ? wait_for(w, WAIT_MS) : status;
}

static void producers(const struct fixture *f, const char *run) {
  FelixClient *client = NULL;
  REQUIRE_OK(connect_sync(f->addrs, f->tenant, f->token, f->ca_file, &client));

  struct waiter made;
  waiter_init(&made);
  FelixStatus status =
      felix_client_idempotent_producer(client, NULL, on_producer, &made);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&made, WAIT_MS);
  }
  FelixProducer *p = made.handle;
  if (status != FELIX_STATUS_OK || !p) {
    CHECK(0, "making a producer gave %d: %s", (int)status, made.message);
    waiter_destroy(&made);
    felix_client_free(client);
    return;
  }
  waiter_destroy(&made);

  uint64_t id = 0, stream_id = 0;
  CHECK(felix_producer_id(p, &id) == FELIX_STATUS_OK && id != 0,
        "producer id %" PRIu64, id);
  CHECK(felix_producer_id_for(p, f->tenant, f->ns, f->stream, &stream_id) ==
                FELIX_STATUS_OK &&
            stream_id == id,
        "id_for gave %" PRIu64 " against %" PRIu64, stream_id, id);

  /* Single-record publishes get increasing offsets, when the broker reports
     them. */
  bool seen = false;
  uint64_t previous = 0;
  for (int i = 0; i < 3; i++) {
    struct laid l;
    char tag[16];
    snprintf(tag, sizeof tag, "one%d", i);
    lay(&l, run, tag, 1);
    struct waiter w;
    waiter_init(&w);
    status = producer_publish(p, f, &l, NULL, &w);
    CHECK(status == FELIX_STATUS_OK, "publish %d gave %d: %s", i, (int)status,
          w.message);
    if (status == FELIX_STATUS_OK && w.has_offset) {
      CHECK(!seen || w.offset > previous,
            "offset %" PRIu64 " did not follow %" PRIu64, w.offset, previous);
      seen = true;
      previous = w.offset;
    }
    waiter_destroy(&w);
  }

  /* A keyed batch of two. */
  {
    struct laid l;
    char key[64];
    snprintf(key, sizeof key, "pk-%s", run);
    lay(&l, run, "keyed", 2);
    struct waiter w;
    waiter_init(&w);
    status = producer_publish(p, f, &l, key, &w);
    CHECK(status == FELIX_STATUS_OK, "keyed batch gave %d: %s", (int)status,
          w.message);
    waiter_destroy(&w);
  }

  /* Two batches in one call: sizes 2 and 1. */
  {
    struct laid l;
    lay(&l, run, "many", 3);
    size_t sizes[2] = {2, 1};
    struct waiter w;
    waiter_init(&w);
    status = felix_producer_publish_batches(
        p, f->tenant, f->ns, f->stream, (const uint8_t *)l.data, l.lens, 3,
        sizes, 2, NULL, on_producer_done, &w);
    if (status == FELIX_STATUS_OK) {
      status = wait_for(&w, WAIT_MS);
    }
    CHECK(status == FELIX_STATUS_OK, "publish_batches gave %d: %s",
          (int)status, w.message);
    /* Sizes that do not add up are refused at the call. */
    size_t bad[2] = {2, 2};
    status = felix_producer_publish_batches(
        p, f->tenant, f->ns, f->stream, (const uint8_t *)l.data, l.lens, 3,
        bad, 2, NULL, on_producer_done, &w);
    CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "uneven sizes gave %d",
          (int)status);
    size_t zero[2] = {3, 0};
    status = felix_producer_publish_batches(
        p, f->tenant, f->ns, f->stream, (const uint8_t *)l.data, l.lens, 3,
        zero, 2, NULL, on_producer_done, &w);
    CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "a zero size gave %d",
          (int)status);
    status = felix_producer_publish(p, f->tenant, f->ns, f->stream, NULL, NULL,
                                    0, NULL, 0, NULL, on_publish, &w);
    CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "an empty publish gave %d",
          (int)status);
    waiter_destroy(&w);
  }

  /* Nothing is in doubt, so giving up is a no-op that succeeds. */
  {
    struct waiter w;
    waiter_init(&w);
    status = felix_producer_abandon_in_doubt(p, f->tenant, f->ns, f->stream,
                                             NULL, on_producer_done, &w);
    if (status == FELIX_STATUS_OK) {
      status = wait_for(&w, WAIT_MS);
    }
    CHECK(status == FELIX_STATUS_OK, "abandon_in_doubt gave %d: %s",
          (int)status, w.message);
    waiter_destroy(&w);
  }

  /* Closing waits for what was sent; the handle then reports CLOSED. */
  {
    struct waiter w;
    waiter_init(&w);
    status = felix_producer_close(p, 5000, NULL, on_producer_done, &w);
    if (status == FELIX_STATUS_OK) {
      status = wait_for(&w, WAIT_MS);
    }
    CHECK(status == FELIX_STATUS_OK, "close gave %d: %s", (int)status,
          w.message);
    waiter_destroy(&w);

    struct laid l;
    lay(&l, run, "late", 1);
    waiter_init(&w);
    status = producer_publish(p, f, &l, NULL, &w);
    CHECK(status == FELIX_STATUS_CLOSED, "publish after close gave %d",
          (int)status);
    waiter_destroy(&w);
    CHECK(felix_producer_id(p, &id) == FELIX_STATUS_CLOSED,
          "id after close was not CLOSED");
    waiter_init(&w);
    status = felix_producer_close(p, -1, NULL, on_producer_done, &w);
    if (status == FELIX_STATUS_OK) {
      status = wait_for(&w, WAIT_MS);
    }
    CHECK(status == FELIX_STATUS_CLOSED, "a second close gave %d", (int)status);
    waiter_destroy(&w);
  }
  felix_producer_free(p);

  /* A producer that is freed without being closed. */
  waiter_init(&made);
  status = felix_client_idempotent_producer(client, NULL, on_producer, &made);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&made, WAIT_MS);
  }
  CHECK(status == FELIX_STATUS_OK && made.handle, "second producer gave %d",
        (int)status);
  felix_producer_free(made.handle);
  waiter_destroy(&made);
  felix_client_free(client);
}

int main(int argc, char **argv) {
  if (argc == 2 && strcmp(argv[1], "offline") == 0) {
    offline();
  } else if (argc == 13 && strcmp(argv[1], "fixture") == 0) {
    struct fixture f = {argv[2], argv[3], argv[4],  argv[5],  argv[6],
                        argv[7], argv[8], argv[9], argv[10], argv[11],
                        argv[12]};
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
    caches(&f, run);
    commits(&f, run);
    producers(&f, run);
  } else {
    fprintf(stderr, "usage: %s offline | fixture ADDRS TENANT NAMESPACE TOKEN "
                    "CA_FILE STREAM MISSING UNAUTHORIZED_TOKEN "
                    "GROUP_OPERATOR_TOKEN CACHE SINGLE_SHARD_CACHE\n",
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
