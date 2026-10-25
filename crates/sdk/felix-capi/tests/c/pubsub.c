/*
 * Drives the C ABI the way a C, Go or C# caller would: through the header and
 * a linked library, with no Rust in sight.
 *
 *   pubsub offline
 *     Argument checking, the last-error channel, and connects that fail or
 *     are cancelled. Needs no broker.
 *
 *   pubsub fixture FILE
 *     The publish, batch, subscribe, subscription, group, cache and commit
 *     scenarios, against `felix-cluster client-fixture`. tests/c/run.sh starts
 *     the fixture and writes its fields to FILE as key=value lines.
 *
 * Scenarios from the conformance catalogue (crates/testing/felix-conformance)
 * are tagged with SCENARIO_BEGIN and SCENARIO_END around the checks that
 * demonstrate them. One passes when no CHECK failed inside it; SKIP records
 * why one was not run. When FELIX_CONFORMANCE_LINES names a file, each outcome
 * is appended to it, and run.sh merges the two builds' files into the results
 * document `felix-conformance verify` reads.
 *
 * Every call is asynchronous. `struct waiter` turns one into a blocking call
 * with a mutex and a condition variable, which is all a blocking caller needs.
 */
#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "felix.h"

#define WAIT_MS 20000

static int failures = 0;

/* A call waits at least this long for its callback. The fault scenarios raise
   it: a call may legitimately take a hold plus the idle timeout to give up. */
static int wait_floor_ms = 0;

/* The conformance ledger. Scenarios open and close like a stack; a failed
   CHECK fails every scenario open at that moment, and one still open when the
   run ends counts as failed, since an early return skipped its checks. */
#define MAX_OPEN 16

static struct {
  const char *id;
  bool failed;
  char where[96];
} open_scenarios[MAX_OPEN];
static size_t open_count = 0;

static void ledger_write(const char *id, const char *status,
                         const char *detail) {
  const char *path = getenv("FELIX_CONFORMANCE_LINES");
  if (!path || !*path) {
    return;
  }
  FILE *out = fopen(path, "a");
  if (!out) {
    fprintf(stderr, "cannot append to %s\n", path);
    failures++;
    return;
  }
  fprintf(out, "%s\t%s\t", id, status);
  for (const char *c = detail ? detail : ""; *c; c++) {
    fputc(*c == '\t' || *c == '\n' ? ' ' : *c, out);
  }
  fputc('\n', out);
  fclose(out);
}

static void ledger_begin(const char *id) {
  if (open_count == MAX_OPEN) {
    fprintf(stderr, "too many open scenarios at %s\n", id);
    failures++;
    return;
  }
  open_scenarios[open_count].id = id;
  open_scenarios[open_count].failed = false;
  open_scenarios[open_count].where[0] = '\0';
  open_count++;
}

/* Closes `id`. `skip` is a reason to record it as skipped instead of passed,
   unless a check inside it already failed. */
static void ledger_finish(const char *id, const char *skip) {
  for (size_t i = open_count; i > 0; i--) {
    if (strcmp(open_scenarios[i - 1].id, id) != 0) {
      continue;
    }
    if (open_scenarios[i - 1].failed) {
      ledger_write(id, "fail", open_scenarios[i - 1].where);
    } else if (skip) {
      ledger_write(id, "skip", skip);
    } else {
      ledger_write(id, "pass", "");
    }
    for (size_t j = i; j < open_count; j++) {
      open_scenarios[j - 1] = open_scenarios[j];
    }
    open_count--;
    return;
  }
  fprintf(stderr, "no open scenario %s\n", id);
  failures++;
}

static void ledger_end(const char *id) { ledger_finish(id, NULL); }

static void ledger_failed(const char *file, int line) {
  for (size_t i = 0; i < open_count; i++) {
    if (!open_scenarios[i].failed) {
      open_scenarios[i].failed = true;
      snprintf(open_scenarios[i].where, sizeof open_scenarios[i].where,
               "check failed at %.60s:%d", file, line);
    }
  }
}

/* Records what is still open as failed. Called as the run ends. */
static void ledger_close_all(void) {
  while (open_count > 0) {
    const char *id = open_scenarios[open_count - 1].id;
    open_scenarios[open_count - 1].failed = true;
    if (!open_scenarios[open_count - 1].where[0]) {
      snprintf(open_scenarios[open_count - 1].where,
               sizeof open_scenarios[open_count - 1].where,
               "the scenario did not finish");
    }
    ledger_end(id);
  }
}

#define SCENARIO_BEGIN(id) ledger_begin(id)
#define SCENARIO_END(id) ledger_end(id)
#define SKIP(id, reason) ledger_write(id, "skip", reason)
/* Gives up an open scenario for a reason that is not a failure. */
#define SCENARIO_SKIP(id, reason) ledger_finish(id, reason)

#define CHECK(cond, ...)                                                       \
  do {                                                                         \
    if (!(cond)) {                                                             \
      fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__);                     \
      fprintf(stderr, __VA_ARGS__);                                            \
      const char *last = felix_last_error_message();                           \
      fprintf(stderr, " (last error: %s)\n", last ? last : "none");            \
      failures++;                                                              \
      ledger_failed(__FILE__, __LINE__);                                       \
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

struct endpoint_list {
  struct waiter w;
  size_t count, with_port;
};

static void on_endpoints(void *user_data, FelixStatus status,
                         const char *message, const char *const *addrs,
                         size_t count) {
  struct endpoint_list *list = user_data;
  list->count = count;
  list->with_port = 0;
  for (size_t i = 0; i < count; i++) {
    list->with_port += addrs[i] && strchr(addrs[i], ':') != NULL;
  }
  finish(&list->w, status, message, NULL);
}

/* The callback's status. Exits the test if it does not arrive in time. */
static FelixStatus wait_for(struct waiter *w, int ms) {
  if (ms < wait_floor_ms) {
    ms = wait_floor_ms;
  }
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
    ledger_close_all();
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
  /* Empty when the fixture has no control endpoint. */
  const char *movable_stream, *quorum_stream, *control_url;
  /* "true" when the fixture's brokers store each record's publisher. That
     needs the fleet to finalize publisher_principal, which is opt-in and which
     the fixture does not do, so a client that asks is told nothing. */
  const char *records_publishers;
  /* Brokers that advertise no features, so no commits. Empty when the fixture
     has none. */
  const char *no_commit_addrs;
  /* The link interposer: a broker address that faults can be injected on, and
     the single-shard stream behind it. Empty when the fixture has none. */
  const char *link_addr, *link_stream;
  /* The script that kills the broker listening on an address. */
  const char *kill_helper;
  /* The fixture file, which also holds the fault.<id> steps. */
  const char *path;
};

/* The fixture file: one key=value per line, the value running to the end of
   the line. Lines for keys this test does not know are ignored. */
#define MAX_LINE 4096

static char *fixture_value(FILE *in, const char *key) {
  char line[MAX_LINE];
  size_t klen = strlen(key);
  rewind(in);
  while (fgets(line, sizeof line, in)) {
    size_t len = strlen(line);
    while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
      line[--len] = '\0';
    }
    if (strncmp(line, key, klen) == 0 && line[klen] == '=') {
      return strdup(line + klen + 1);
    }
  }
  return NULL;
}

static bool load_fixture(const char *path, struct fixture *f) {
  FILE *in = fopen(path, "r");
  if (!in) {
    fprintf(stderr, "cannot read the fixture file %s\n", path);
    return false;
  }
  struct {
    const char **field;
    const char *key;
    bool required;
  } fields[] = {
      {&f->addrs, "addrs", true},
      {&f->tenant, "tenant_id", true},
      {&f->ns, "namespace", true},
      {&f->token, "token", true},
      {&f->ca_file, "ca_file", true},
      {&f->stream, "durable_stream", true},
      {&f->missing, "missing_stream", true},
      {&f->unauthorized_token, "unauthorized_token", true},
      {&f->group_operator_token, "group_operator_token", true},
      {&f->cache, "cache", true},
      {&f->single_shard_cache, "single_shard_cache", true},
      {&f->movable_stream, "movable_stream", false},
      {&f->quorum_stream, "quorum_stream", false},
      {&f->control_url, "control_url", false},
      {&f->records_publishers, "records_publishers", false},
      {&f->no_commit_addrs, "no_commit_addrs", false},
      {&f->link_addr, "link_addr", false},
      {&f->link_stream, "link_stream", false},
      {&f->kill_helper, "kill_helper", false},
  };
  bool ok = true;
  for (size_t i = 0; i < sizeof fields / sizeof *fields; i++) {
    char *value = fixture_value(in, fields[i].key);
    if (!value && fields[i].required) {
      fprintf(stderr, "the fixture file has no %s\n", fields[i].key);
      ok = false;
    }
    *fields[i].field = value ? value : strdup("");
  }
  fclose(in);
  f->path = strdup(path);
  return ok;
}

/* One connection-fault step from the catalogue, as the fixture file holds it:
   fault.<id>={"during":"publish","fault":"drop","after_records":3,...}. */
struct step {
  char during[24], fault[24];
  int after_records, records, hold_ms;
};

static bool json_text(const char *json, const char *key, char *out, size_t cap) {
  char needle[64];
  snprintf(needle, sizeof needle, "\"%s\":\"", key);
  const char *at = strstr(json, needle);
  if (!at) {
    return false;
  }
  at += strlen(needle);
  const char *end = strchr(at, '"');
  if (!end || (size_t)(end - at) >= cap) {
    return false;
  }
  memcpy(out, at, (size_t)(end - at));
  out[end - at] = '\0';
  return true;
}

static bool json_number(const char *json, const char *key, int *out) {
  char needle[64];
  snprintf(needle, sizeof needle, "\"%s\":", key);
  const char *at = strstr(json, needle);
  if (!at) {
    return false;
  }
  *out = atoi(at + strlen(needle));
  return true;
}

static bool load_step(const struct fixture *f, const char *id,
                      struct step *step) {
  FILE *in = fopen(f->path, "r");
  if (!in) {
    return false;
  }
  char key[160];
  snprintf(key, sizeof key, "fault.%s", id);
  char *json = fixture_value(in, key);
  fclose(in);
  if (!json) {
    return false;
  }
  bool ok = json_text(json, "during", step->during, sizeof step->during) &&
            json_text(json, "fault", step->fault, sizeof step->fault) &&
            json_number(json, "after_records", &step->after_records) &&
            json_number(json, "records", &step->records) &&
            json_number(json, "hold_ms", &step->hold_ms);
  free(json);
  return ok && step->records > 0 && step->after_records >= 0 &&
         step->after_records < step->records;
}

/* Addresses in a comma separated list. */
static size_t addr_count(const char *addrs) {
  size_t n = *addrs ? 1 : 0;
  for (const char *c = addrs; *c; c++) {
    n += *c == ',';
  }
  return n;
}

static bool addr_at(const char *addrs, size_t index, char *out, size_t cap) {
  for (size_t i = 0; i < index; i++) {
    addrs = strchr(addrs, ',');
    if (!addrs) {
      return false;
    }
    addrs++;
  }
  size_t len = strcspn(addrs, ",");
  if (len == 0 || len >= cap) {
    return false;
  }
  memcpy(out, addrs, len);
  out[len] = '\0';
  return true;
}

/* ---- sharded groups, stream-wide group admin, connect options ---------- */

#define PARITY_SHARDS 8

struct srec {
  uint32_t shard;
  uint64_t offset;
  uint32_t attempts;
  char payload[96];
  bool has_timestamp;
  uint64_t timestamp_micros;
  size_t publisher_len;
};

/* One call's result; each callback fills the fields it reports. */
struct pres {
  struct waiter w;
  struct srec records[PARITY_SHARDS * 2];
  size_t count;
  FelixGroupPosition positions[PARITY_SHARDS];
  FelixGroupInfo infos[PARITY_SHARDS];
  bool existed;
};

static void pres_init(struct pres *r) {
  memset(r, 0, sizeof *r);
  waiter_init(&r->w);
}

static void on_sharded_group(void *user_data, FelixStatus status,
                             const char *message, FelixShardedGroup *group) {
  finish(user_data, status, message, group);
}

static void on_sharded_poll(void *user_data, FelixStatus status,
                            const char *message,
                            const FelixShardedGroupRecord *records,
                            size_t count) {
  struct pres *r = user_data;
  r->count = count;
  for (size_t i = 0; i < count && i < PARITY_SHARDS * 2; i++) {
    struct srec *out = &r->records[i];
    const FelixGroupRecord *in = &records[i].record;
    out->shard = records[i].shard;
    out->offset = in->offset;
    out->attempts = in->attempts;
    size_t len = in->payload_len;
    if (len >= sizeof out->payload) {
      len = sizeof out->payload - 1;
    }
    if (len) {
      memcpy(out->payload, in->payload, len);
    }
    out->payload[len] = '\0';
    out->has_timestamp = in->has_timestamp;
    out->timestamp_micros = in->timestamp_micros;
    out->publisher_len = in->publisher_len;
  }
  finish(&r->w, status, message, NULL);
}

static void on_positions(void *user_data, FelixStatus status,
                         const char *message,
                         const FelixGroupPosition *positions, size_t count) {
  struct pres *r = user_data;
  r->count = count;
  for (size_t i = 0; i < count && i < PARITY_SHARDS; i++) {
    r->positions[i] = positions[i];
  }
  finish(&r->w, status, message, NULL);
}

static void on_infos(void *user_data, FelixStatus status, const char *message,
                     const FelixGroupInfo *infos, size_t count) {
  struct pres *r = user_data;
  r->count = count;
  for (size_t i = 0; i < count && i < PARITY_SHARDS; i++) {
    r->infos[i] = infos[i];
  }
  finish(&r->w, status, message, NULL);
}

static void on_pres_done(void *user_data, FelixStatus status,
                         const char *message) {
  struct pres *r = user_data;
  finish(&r->w, status, message, NULL);
}

static void on_stream_deleted(void *user_data, FelixStatus status,
                              const char *message, bool existed) {
  struct pres *r = user_data;
  r->existed = existed;
  finish(&r->w, status, message, NULL);
}

/* A token provider for the tests: the fetch callback answers from a thread of
   its own, as an app doing I/O would, and counts what it was asked. */
struct token_app {
  const char *token;   /* NULL makes the fetch fail */
  atomic_int fetches;
  atomic_int released;
};

struct token_job {
  struct token_app *app;
  FelixTokenReply *reply;
};

static void *token_answer(void *arg) {
  struct token_job *job = arg;
  if (job->app->token) {
    /* The reply is consumed by this call. */
    felix_token_reply_ok(job->reply, job->app->token);
  } else {
    felix_token_reply_err(job->reply, "no token for you");
  }
  free(job);
  return NULL;
}

static void token_fetch(void *user_data, FelixTokenReply *reply) {
  struct token_app *app = user_data;
  atomic_fetch_add(&app->fetches, 1);
  struct token_job *job = malloc(sizeof *job);
  pthread_t thread;
  if (!job) {
    felix_token_reply_err(reply, "out of memory");
    return;
  }
  job->app = app;
  job->reply = reply;
  if (pthread_create(&thread, NULL, token_answer, job) != 0) {
    free(job);
    felix_token_reply_err(reply, "no thread");
    return;
  }
  pthread_detach(thread);
}

static void token_release(void *user_data) {
  struct token_app *app = user_data;
  atomic_fetch_add(&app->released, 1);
}

static void token_app_init(struct token_app *app, const char *token) {
  app->token = token;
  atomic_init(&app->fetches, 0);
  atomic_init(&app->released, 0);
}

static FelixConnectOptions token_options(struct token_app *app) {
  FelixConnectOptions options;
  memset(&options, 0, sizeof options);
  options.size = sizeof options;
  options.token_fetch = token_fetch;
  options.token_user_data = app;
  options.token_release = token_release;
  return options;
}

/* True once the release has run, waiting a little for the runtime to wind
   down; then checks it did not run again. */
static bool released_once(struct token_app *app) {
  for (int i = 0; i < 500 && atomic_load(&app->released) == 0; i++) {
    pause_ms(10);
  }
  pause_ms(100);
  return atomic_load(&app->released) == 1;
}

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

  status = felix_client_endpoints(NULL, NULL, on_endpoints, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "endpoints of a null client gave %d",
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
  status = felix_client_connect_with(NULL, "t1", "token", NULL, NULL, NULL,
                                     NULL, on_connect, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null addrs to connect_with gave %d",
        (int)status);
  {
    FelixConnectOptions bad = {.size = 0};
    status = felix_client_connect_with("127.0.0.1:1", "t1", "token", NULL, NULL,
                                       &bad, NULL, on_connect, &w);
    CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "options of size 0 gave %d",
          (int)status);
  }
  status = felix_client_group_sharded(NULL, "t", "n", "s", "g", NULL,
                                      on_sharded_group, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null sharded group client gave %d",
        (int)status);
  status = felix_sharded_group_poll(NULL, 1, NULL, on_sharded_poll, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null sharded group gave %d",
        (int)status);
  status = felix_sharded_group_shards(NULL, NULL);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null sharded group shards gave %d",
        (int)status);
  status = felix_client_group_create_stream(NULL, "t", "n", "s", "g",
                                            FELIX_START_LATEST, 0, NULL,
                                            on_positions, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null create_stream client gave %d",
        (int)status);
  status = felix_client_group_seek_stream(NULL, "t", "n", "s", "g",
                                          FELIX_START_LATEST, 0, NULL,
                                          on_positions, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null seek_stream client gave %d",
        (int)status);
  status = felix_client_group_describe_stream(NULL, "t", "n", "s", "g", NULL,
                                              on_infos, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null describe_stream client gave %d",
        (int)status);
  status = felix_client_group_delete_stream(NULL, "t", "n", "s", "g", NULL,
                                            on_stream_deleted, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null delete_stream client gave %d",
        (int)status);
  {
    const uint8_t *data = NULL;
    size_t len = 0;
    status = felix_event_publisher(NULL, &data, &len);
    CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null event publisher gave %d",
          (int)status);
    bool has = false;
    uint64_t micros = 0;
    status = felix_event_timestamp(NULL, &micros, &has);
    CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null event timestamp gave %d",
          (int)status);
  }
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
  felix_sharded_group_free(NULL);
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

  /* A token is required unless a fetch function supplies them, and a call
     that is refused does not take the app's data. */
  struct token_app app;
  token_app_init(&app, "unused");
  FelixConnectOptions with_fetch = token_options(&app);
  waiter_init(&w);
  status = felix_client_connect("127.0.0.1:1", "t1", NULL, NULL, NULL, NULL,
                                on_connect, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "a null token gave %d",
        (int)status);
  status = felix_client_connect_with(NULL, "t1", NULL, NULL, NULL, &with_fetch,
                                     NULL, on_connect, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "null addrs gave %d",
        (int)status);
  CHECK(atomic_load(&app.released) == 0, "a refused call released the data");

  /* A struct from before the token fields: they are not read. */
  FelixConnectOptions old_size = with_fetch;
  old_size.size = offsetof(FelixConnectOptions, token_fetch);
  status = felix_client_connect_with("127.0.0.1:1", "t1", NULL, NULL, NULL,
                                     &old_size, NULL, on_connect, &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT,
        "options without the token fields allowed a null token: %d",
        (int)status);

  /* Accepted with a fetch function and no token: the connect fails (nothing
     listens) through the callback, and the data is released exactly once. */
  status = felix_client_connect_with("127.0.0.1:1", "t1", NULL, NULL, NULL,
                                     &with_fetch, NULL, on_connect, &w);
  CHECK(status == FELIX_STATUS_OK, "connect with a fetch gave %d", (int)status);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, WAIT_MS);
    CHECK(status != FELIX_STATUS_OK, "connecting to a closed port worked");
    CHECK(released_once(&app), "token_release ran %d times",
          atomic_load(&app.released));
  }
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

/* Reads one event at a time until `payload` arrives, counting the events seen
   on the way, others' included. */
static bool read_until(FelixSubscription *sub, const char *payload, int *seen,
                       uint64_t *offset) {
  for (int i = 0; i < 100000; i++) {
    struct waiter w;
    waiter_init(&w);
    FelixStatus status = felix_subscription_next(sub, WAIT_MS, NULL, on_event, &w);
    if (status == FELIX_STATUS_OK) {
      status = wait_for(&w, WAIT_MS * 2);
    }
    FelixEvent *event = status == FELIX_STATUS_OK ? w.handle : NULL;
    waiter_destroy(&w);
    if (!event) {
      return false;
    }
    const uint8_t *data = NULL;
    size_t len = 0;
    bool has_offset = false;
    uint64_t at = 0;
    felix_event_payload(event, &data, &len);
    felix_event_offset(event, &at, &has_offset);
    bool match = len == strlen(payload) && memcmp(data, payload, len) == 0;
    felix_event_free(event);
    (*seen)++;
    if (match) {
      *offset = at;
      return true;
    }
  }
  return false;
}

static void pubsub(const struct fixture *f, const char *run) {
  FelixClient *client = NULL;
  REQUIRE_OK(connect_sync(f->addrs, f->tenant, f->token, f->ca_file, &client));

  /* pubsub.roundtrip, pubsub.order, pubsub.offsets_are_contiguous */
  SCENARIO_BEGIN("pubsub.roundtrip");
  SCENARIO_BEGIN("pubsub.order");
  SCENARIO_BEGIN("pubsub.offsets_are_contiguous");
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
  SCENARIO_END("pubsub.offsets_are_contiguous");
  SCENARIO_END("pubsub.order");
  SCENARIO_END("pubsub.roundtrip");

  /* Freeing a subscription with a read pending still calls back, once: the end
     is reported rather than waited on for ever. */
  SCENARIO_BEGIN("client.subscription_ends_cleanly");
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
  SCENARIO_END("client.subscription_ends_cleanly");

  /* pubsub.resume_from_offset: published while nothing is subscribed, then
     read by resuming at the offset after the last one handled. */
  char resumed[64];
  snprintf(resumed, sizeof resumed, "c-%s-resumed", run);
  status = publish(client, f, f->stream, resumed, NULL, NULL, 0);
  CHECK(status == FELIX_STATUS_OK, "publish before resume gave %d",
        (int)status);
  sub = NULL;
  /* The record at last + 1 is the first one published after the last handled,
     so reading it there means no gap, and nothing at or before `last` came
     again. */
  SCENARIO_BEGIN("pubsub.resume_from_offset");
  SCENARIO_BEGIN("reconnect.subscription_resumes_at_the_next_offset");
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
  SCENARIO_END("reconnect.subscription_resumes_at_the_next_offset");
  SCENARIO_END("pubsub.resume_from_offset");

  /* error.unknown_stream_is_typed: never a connection failure, since a
     retry cannot make the stream exist. A broker with no assignment for the
     shard says it is unavailable, which is typed too. */
  char message[512] = "";
  SCENARIO_BEGIN("error.unknown_stream_is_typed");
  status = publish(client, f, f->missing, "nowhere", NULL, message,
                   sizeof message);
  CHECK(status == FELIX_STATUS_NOT_FOUND ||
            status == FELIX_STATUS_SHARD_UNAVAILABLE,
        "an unknown stream gave %d", (int)status);
  CHECK(message[0] != '\0', "no message for an unknown stream");
  SCENARIO_END("error.unknown_stream_is_typed");

  felix_client_free(client);

  /* error.unauthorized_is_typed */
  FelixClient *restricted = NULL;
  REQUIRE_OK(connect_sync(f->addrs, f->tenant, f->unauthorized_token,
                          f->ca_file, &restricted));
  /* Promptly is half of it: retrying a refusal no retry can fix would take
     the whole backoff budget to reach the same answer. */
  SCENARIO_BEGIN("error.unauthorized_is_typed");
  SCENARIO_BEGIN("retry.terminal_failures_are_not_retried");
  struct timespec began, ended;
  clock_gettime(CLOCK_MONOTONIC, &began);
  status = publish(restricted, f, f->stream, "not allowed", NULL, NULL, 0);
  clock_gettime(CLOCK_MONOTONIC, &ended);
  CHECK(status == FELIX_STATUS_AUTH, "an unauthorized publish gave %d",
        (int)status);
  double refused_in = (double)(ended.tv_sec - began.tv_sec) +
                      (double)(ended.tv_nsec - began.tv_nsec) / 1e9;
  CHECK(refused_in < 5.0, "the refusal took %.2fs, so it was retried",
        refused_in);
  SCENARIO_END("retry.terminal_failures_are_not_retried");
  SCENARIO_END("error.unauthorized_is_typed");
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
  SCENARIO_BEGIN("explicit_shard.out_of_range_is_refused");
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
  SCENARIO_END("explicit_shard.out_of_range_is_refused");

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
  SCENARIO_BEGIN("sharded.delivers_every_shard");

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
  /* More than one shard contributed, or the merge was never exercised. */
  size_t contributing = 0;
  for (uint32_t s = 0; s < MAX_SHARDS; s++) {
    contributing += seen.any[s];
  }
  CHECK(contributing > 1, "every record came from %zu shard", contributing);
  SCENARIO_END("sharded.delivers_every_shard");

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
  SCENARIO_BEGIN("sharded.resumes_per_shard");
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
    SCENARIO_END("sharded.resumes_per_shard");

    /* Freeing with a read pending completes it as cancelled. */
    SCENARIO_BEGIN("client.subscription_ends_cleanly");
    memset(&it, 0, sizeof it);
    waiter_init(&it.w);
    REQUIRE_OK(felix_sharded_next(sharded, -1, NULL, on_item, &it));
    pause_ms(50);
    felix_sharded_subscription_free(sharded);
    status = wait_for(&it.w, WAIT_MS);
    CHECK(status == FELIX_STATUS_CANCELLED,
          "a sharded read pending at free gave %d", (int)status);
    waiter_destroy(&it.w);
    SCENARIO_END("client.subscription_ends_cleanly");
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
  SCENARIO_BEGIN("queue.poll_returns_claimed_records");
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
  SCENARIO_END("queue.poll_returns_claimed_records");

  /* A nack with no delay hands the first record out again, one attempt on. */
  SCENARIO_BEGIN("queue.unacknowledged_records_are_redelivered");
  CHECK(g_settle(&g, "nack", got[0].offset, got[0].attempts, 0, NULL) ==
            FELIX_STATUS_OK,
        "nack failed");
  struct grecord again[1];
  if (g_collect(&g, consumer, 1, again) == 1) {
    CHECK(again[0].offset == got[0].offset, "redelivered offset %" PRIu64
          ", want %" PRIu64, again[0].offset, got[0].offset);
    CHECK(again[0].attempts == 2, "redelivery was attempt %u",
          (unsigned)again[0].attempts);
    SCENARIO_END("queue.unacknowledged_records_are_redelivered");

    uint64_t visible = 0;
    g_settle(&g, "extend", again[0].offset, again[0].attempts, 30000, &visible);
    CHECK(visible > 0, "extend reported a claim of %" PRIu64 " ms", visible);
    g_settle(&g, "ack", again[0].offset, 0, 0, NULL);
  }
  SCENARIO_BEGIN("queue.acknowledged_records_are_not_redelivered");
  g_settle(&g, "ack", got[1].offset, 0, 0, NULL);

  /* Settled records stay settled: a poll that waits for them turns up neither
     of the two acked. Records still claimed are not owed yet. */
  struct gres after;
  status = g_poll(&g, 4, 1000, consumer, &after);
  CHECK(status == FELIX_STATUS_OK, "the poll after the acks gave %d (%s)",
        (int)status, after.w.message);
  for (size_t i = 0; status == FELIX_STATUS_OK && i < after.count; i++) {
    CHECK(after.records[i].offset != got[0].offset &&
              after.records[i].offset != got[1].offset,
          "acked offset %" PRIu64 " came back", after.records[i].offset);
  }
  waiter_destroy(&after.w);
  SCENARIO_END("queue.acknowledged_records_are_not_redelivered");

  /* A dead letter is listed, can be driven back, and is delivered again. */
  SCENARIO_BEGIN("queue.dead_letters_are_listable_and_redrivable");
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
  SCENARIO_END("queue.dead_letters_are_listable_and_redrivable");

  /* Nothing is owed any more, and an empty poll is an answer, not an error. */
  SCENARIO_BEGIN("queue.poll_returns_claimed_records");
  struct gres empty;
  status = g_poll(&g, 1, 500, consumer, &empty);
  CHECK(status == FELIX_STATUS_OK && empty.count == 0,
        "a poll with nothing owed gave %d, %zu records", (int)status,
        empty.count);
  waiter_destroy(&empty.w);
  SCENARIO_END("queue.poll_returns_claimed_records");

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
  SCENARIO_BEGIN("sharded.reports_the_shard_count");
  struct gres r;
  gres_init(&r);
  status = gwait(&r, felix_client_stream_shards(client, f->tenant, f->ns,
                                                f->stream, NULL, on_shards, &r));
  CHECK(status == FELIX_STATUS_OK && r.shards == 4,
        "stream_shards gave %d, %u shards", (int)status, (unsigned)r.shards);
  waiter_destroy(&r.w);
  /* Zero, not one: an unknown stream must not read as a real single-shard one. */
  gres_init(&r);
  status = gwait(&r, felix_client_stream_shards(client, f->tenant, f->ns,
                                                f->missing, NULL, on_shards, &r));
  CHECK(status == FELIX_STATUS_OK && r.shards == 0,
        "stream_shards of an unknown stream gave %d, %u shards", (int)status,
        (unsigned)r.shards);
  waiter_destroy(&r.w);
  SCENARIO_END("sharded.reports_the_shard_count");

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


static FelixStatus connect_with_sync(const struct fixture *f,
                                     const FelixConnectOptions *options,
                                     FelixClient **out) {
  struct waiter w;
  waiter_init(&w);
  FelixStatus status = felix_client_connect_with(
      f->addrs, f->tenant, f->token, NULL, f->ca_file, options, NULL,
      on_connect, &w);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, WAIT_MS);
    *out = w.handle;
  }
  waiter_destroy(&w);
  return status;
}

/* Publishes one record to `shard` of the fixture's stream. */
static void publish_one_on_shard(FelixClient *client, const struct fixture *f,
                                 uint32_t shard, const char *payload) {
  const uint8_t *data = (const uint8_t *)payload;
  size_t len = strlen(payload);
  struct batch_waiter b;
  batch_init(&b);
  FelixStatus status = batch_wait(
      &b, felix_client_publish_on_shard(client, f->tenant, f->ns, f->stream,
                                        shard, data, &len, 1,
                                        FELIX_ACK_PER_BATCH, NULL, on_batch,
                                        &b));
  CHECK(status == FELIX_STATUS_OK, "publish_on_shard gave %d (%s)", (int)status,
        b.w.message);
  waiter_destroy(&b.w);
}

struct event_meta {
  bool found;
  bool has_timestamp;
  uint64_t timestamp_micros;
  bool has_publisher;
};

/* Reads shard events until `payload` arrives and records its metadata. */
static void read_event_meta(FelixSubscription *sub, const char *payload,
                            struct event_meta *out) {
  memset(out, 0, sizeof *out);
  for (int i = 0; i < 200 && !out->found; i++) {
    struct waiter w;
    waiter_init(&w);
    FelixStatus status = felix_subscription_next(sub, WAIT_MS, NULL, on_event, &w);
    if (status == FELIX_STATUS_OK) {
      status = wait_for(&w, WAIT_MS * 2);
    }
    FelixEvent *event = status == FELIX_STATUS_OK ? w.handle : NULL;
    waiter_destroy(&w);
    if (!event) {
      return;
    }
    const uint8_t *data = NULL;
    size_t len = 0;
    felix_event_payload(event, &data, &len);
    if (len == strlen(payload) && memcmp(data, payload, len) == 0) {
      const uint8_t *who = (const uint8_t *)1;
      size_t who_len = 99;
      CHECK(felix_event_publisher(event, &who, &who_len) == FELIX_STATUS_OK,
            "event_publisher failed");
      CHECK((who == NULL) == (who_len == 0),
            "publisher pointer and length disagree");
      out->has_publisher = who_len > 0;
      CHECK(felix_event_timestamp(event, &out->timestamp_micros,
                                  &out->has_timestamp) == FELIX_STATUS_OK,
            "event_timestamp failed");
      out->found = true;
    }
    felix_event_free(event);
  }
}

static uint64_t now_micros(void) {
  struct timespec ts;
  timespec_get(&ts, TIME_UTC);
  return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

/* Publisher and timestamp fields: absent by default, present when connected
   asking for them. */
static void connect_options(const struct fixture *f, const char *run) {
  FelixClient *plain = NULL, *asking = NULL, *older = NULL;
  FelixConnectOptions both = {
      .size = sizeof(FelixConnectOptions), .publishers = true, .timestamps = true};
  /* A struct from before the flags existed: only its size field. */
  FelixConnectOptions size_only = {.size = sizeof(size_t),
                                   .publishers = true,
                                   .timestamps = true};
  REQUIRE_OK(connect_sync(f->addrs, f->tenant, f->token, f->ca_file, &plain));
  REQUIRE_OK(connect_with_sync(f, &both, &asking));
  REQUIRE_OK(connect_with_sync(f, &size_only, &older));

  FelixSubscription *subs[3] = {NULL, NULL, NULL};
  FelixClient *clients[3] = {plain, asking, older};
  for (int i = 0; i < 3; i++) {
    FelixStatus status =
        subscribe_shard(clients[i], f, 1, FELIX_START_LATEST, 0, &subs[i]);
    CHECK(status == FELIX_STATUS_OK, "subscribe gave %d", (int)status);
  }
  char payload[96];
  snprintf(payload, sizeof payload, "c-%s-meta", run);
  if (subs[0] && subs[1] && subs[2]) {
    uint64_t before = now_micros();
    publish_one_on_shard(plain, f, 1, payload);
    struct event_meta m[3];
    for (int i = 0; i < 3; i++) {
      read_event_meta(subs[i], payload, &m[i]);
      CHECK(m[i].found, "client %d never saw the record", i);
    }
    CHECK(!m[0].has_timestamp && !m[0].has_publisher,
          "a default client was told a time or publisher");
    CHECK(!m[2].has_timestamp && !m[2].has_publisher,
          "options smaller than the flags enabled them");
    CHECK(m[1].has_timestamp, "no timestamp for a client that asked");
    /* A broker tells a client that asked only what it recorded, so a publisher
       is required only where the fixture says brokers record one. */
    if (strcmp(f->records_publishers, "true") == 0) {
      CHECK(m[1].has_publisher, "no publisher for a client that asked");
    }
    /* The broker is on this machine, so its clock agrees to well under this. */
    CHECK(m[1].timestamp_micros + 5u * 1000000u >= before &&
              m[1].timestamp_micros <= now_micros() + 5u * 1000000u,
          "timestamp %" PRIu64 " is not near now", m[1].timestamp_micros);
  }
  for (int i = 0; i < 3; i++) {
    felix_subscription_free(subs[i]);
  }
  felix_client_free(older);
  felix_client_free(asking);
  felix_client_free(plain);
}

/* A client that gets its token from a fetch function: with none to start
   from, with one it never needs to replace, and with a fetch that fails. */
static void token_provider(const struct fixture *f, const char *run) {
  (void)run;
  struct token_app app;
  token_app_init(&app, f->token);
  FelixConnectOptions options = token_options(&app);
  FelixClient *client = NULL;
  struct waiter w;
  waiter_init(&w);
  FelixStatus status = felix_client_connect_with(
      f->addrs, f->tenant, NULL, NULL, f->ca_file, &options, NULL, on_connect,
      &w);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, WAIT_MS);
    client = w.handle;
  }
  waiter_destroy(&w);
  CHECK(status == FELIX_STATUS_OK, "connecting through a fetch gave %d",
        (int)status);
  if (status == FELIX_STATUS_OK) {
    char message[256];
    status = publish(client, f, f->stream, "token-provider", NULL, message,
                     sizeof message);
    CHECK(status == FELIX_STATUS_OK, "publish through a fetched token: %d %s",
          (int)status, message);
    CHECK(atomic_load(&app.fetches) >= 1, "the fetch was never called");
    CHECK(atomic_load(&app.released) == 0, "released while the client lives");
    felix_client_free(client);
  }
  CHECK(released_once(&app), "token_release ran %d times",
        atomic_load(&app.released));

  /* An initial token that stays valid: the fetch is never asked. */
  struct token_app idle;
  token_app_init(&idle, f->token);
  options = token_options(&idle);
  client = NULL;
  waiter_init(&w);
  status = felix_client_connect_with(f->addrs, f->tenant, f->token, NULL,
                                     f->ca_file, &options, NULL, on_connect,
                                     &w);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, WAIT_MS);
    client = w.handle;
  }
  waiter_destroy(&w);
  CHECK(status == FELIX_STATUS_OK, "connecting with an initial token: %d",
        (int)status);
  if (status == FELIX_STATUS_OK) {
    char message[256];
    status = publish(client, f, f->stream, "token-initial", NULL, message,
                     sizeof message);
    CHECK(status == FELIX_STATUS_OK, "publish with an initial token: %d %s",
          (int)status, message);
    CHECK(atomic_load(&idle.fetches) == 0, "the fetch ran %d times",
          atomic_load(&idle.fetches));
    felix_client_free(client);
  }
  CHECK(released_once(&idle), "token_release ran %d times",
        atomic_load(&idle.released));

  /* An initial token the broker refuses is replaced by one fetch, and the
     stream is retried with it. The fixture mints only hour-long tokens and
     keeps no signing key, so a short-lived token that expires mid-run cannot
     be made here; the refresh timing is covered by felix-client's tests. */
  struct token_app refused;
  token_app_init(&refused, f->token);
  options = token_options(&refused);
  client = NULL;
  waiter_init(&w);
  status = felix_client_connect_with(f->addrs, f->tenant, "not-a-felix-token",
                                     NULL, f->ca_file, &options, NULL,
                                     on_connect, &w);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, WAIT_MS);
    client = w.handle;
  }
  waiter_destroy(&w);
  CHECK(status == FELIX_STATUS_OK, "connecting with a refused token: %d",
        (int)status);
  if (status == FELIX_STATUS_OK) {
    char message[256];
    status = publish(client, f, f->stream, "token-refused", NULL, message,
                     sizeof message);
    CHECK(status == FELIX_STATUS_OK, "publish after a refused token: %d %s",
          (int)status, message);
    CHECK(atomic_load(&refused.fetches) == 1, "the fetch ran %d times",
          atomic_load(&refused.fetches));
    felix_client_free(client);
  }
  CHECK(released_once(&refused), "token_release ran %d times",
        atomic_load(&refused.released));

  /* A fetch that fails with nothing to fall back on fails the connect. */
  struct token_app failing;
  token_app_init(&failing, NULL);
  options = token_options(&failing);
  client = NULL;
  waiter_init(&w);
  status = felix_client_connect_with(f->addrs, f->tenant, NULL, NULL,
                                     f->ca_file, &options, NULL, on_connect,
                                     &w);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, WAIT_MS);
    client = w.handle;
  }
  waiter_destroy(&w);
  CHECK(status != FELIX_STATUS_OK, "a failing fetch connected");
  felix_client_free(client);
  CHECK(released_once(&failing), "token_release ran %d times",
        atomic_load(&failing.released));
}

/* The sharded group and the stream-wide admin calls. */
static void sharded_groups(const struct fixture *f, const char *run) {
  FelixClient *client = NULL, *admin = NULL;
  FelixConnectOptions asking = {.size = sizeof(FelixConnectOptions),
                                .timestamps = true};
  REQUIRE_OK(connect_with_sync(f, &asking, &client));
  if (connect_sync(f->addrs, f->tenant, f->group_operator_token, f->ca_file,
                   &admin) != FELIX_STATUS_OK) {
    CHECK(0, "the group operator could not connect");
    felix_client_free(client);
    return;
  }
  char group[128];
  snprintf(group, sizeof group, "sg-%s", run);
  const char *t = f->tenant, *n = f->ns, *s = f->stream;
  struct pres r;

  /* An offset means something different on each shard, so it is refused. */
  pres_init(&r);
  FelixStatus status = felix_client_group_create_stream(
      admin, t, n, s, group, FELIX_START_OFFSET, 5, NULL, on_positions, &r);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&r.w, WAIT_MS);
  }
  CHECK(status != FELIX_STATUS_OK, "an offset start on four shards was accepted");
  waiter_destroy(&r.w);

  pres_init(&r);
  status = felix_client_group_create_stream(admin, t, n, s, group,
                                            FELIX_START_LATEST, 0, NULL,
                                            on_positions, &r);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&r.w, WAIT_MS);
  }
  CHECK(status == FELIX_STATUS_OK && r.count == 4, "create_stream gave %d, %zu shards",
        (int)status, r.count);
  for (size_t i = 0; status == FELIX_STATUS_OK && i < r.count && i < PARITY_SHARDS; i++) {
    CHECK(r.positions[i].moved, "shard %zu was not created", i);
  }
  waiter_destroy(&r.w);

  /* Creating again leaves every shard alone. */
  pres_init(&r);
  status = felix_client_group_create_stream(admin, t, n, s, group,
                                            FELIX_START_LATEST, 0, NULL,
                                            on_positions, &r);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&r.w, WAIT_MS);
  }
  CHECK(status == FELIX_STATUS_OK && r.count == 4, "a second create_stream gave %d, %zu",
        (int)status, r.count);
  for (size_t i = 0; status == FELIX_STATUS_OK && i < r.count && i < PARITY_SHARDS; i++) {
    CHECK(!r.positions[i].moved, "shard %zu moved on a second create", i);
  }
  waiter_destroy(&r.w);

  struct waiter gw;
  waiter_init(&gw);
  status = felix_client_group_sharded(client, t, n, s, group, NULL,
                                      on_sharded_group, &gw);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&gw, WAIT_MS);
  }
  FelixShardedGroup *sg = gw.handle;
  waiter_destroy(&gw);
  if (status != FELIX_STATUS_OK || !sg) {
    CHECK(0, "group_sharded gave %d", (int)status);
    felix_client_free(admin);
    felix_client_free(client);
    return;
  }
  uint32_t shards = 0;
  CHECK(felix_sharded_group_shards(sg, &shards) == FELIX_STATUS_OK && shards == 4,
        "the sharded group spans %u shards", (unsigned)shards);

  /* One record on each shard; the poll finds them by visiting every shard. */
  char payloads[4][96];
  for (uint32_t shard = 0; shard < 4; shard++) {
    snprintf(payloads[shard], sizeof payloads[shard], "sg-%s-s%u", run,
             (unsigned)shard);
    publish_one_on_shard(client, f, shard, payloads[shard]);
  }
  bool seen[4] = {false, false, false, false};
  size_t settled = 0;
  for (int attempt = 0; attempt < 20 && settled < 4; attempt++) {
    pres_init(&r);
    status = felix_sharded_group_poll(sg, 8, NULL, on_sharded_poll, &r);
    if (status == FELIX_STATUS_OK) {
      status = wait_for(&r.w, WAIT_MS);
    }
    CHECK(status == FELIX_STATUS_OK, "sharded poll gave %d (%s)", (int)status,
          r.w.message);
    for (size_t i = 0; status == FELIX_STATUS_OK && i < r.count && i < PARITY_SHARDS * 2; i++) {
      const struct srec *rec = &r.records[i];
      for (uint32_t shard = 0; shard < 4; shard++) {
        if (strcmp(rec->payload, payloads[shard]) != 0) {
          continue;
        }
        CHECK(rec->shard == shard, "record for shard %u came from shard %u",
              (unsigned)shard, (unsigned)rec->shard);
        CHECK(rec->has_timestamp, "no timestamp on a group record");
        /* The settle names the shard the record came from. */
        struct pres ack;
        pres_init(&ack);
        FelixStatus acked = felix_client_group_ack(
            client, t, n, s, rec->shard, group, rec->offset, NULL, on_pres_done, &ack);
        if (acked == FELIX_STATUS_OK) {
          acked = wait_for(&ack.w, WAIT_MS);
        }
        CHECK(acked == FELIX_STATUS_OK, "ack on shard %u gave %d",
              (unsigned)rec->shard, (int)acked);
        waiter_destroy(&ack.w);
        if (!seen[shard]) {
          seen[shard] = true;
          settled++;
        }
      }
    }
    waiter_destroy(&r.w);
    if (status != FELIX_STATUS_OK) {
      break;
    }
  }
  CHECK(settled == 4, "the sharded group delivered %zu of 4 shards' records", settled);

  pres_init(&r);
  status = felix_client_group_describe_stream(client, t, n, s, group, NULL,
                                              on_infos, &r);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&r.w, WAIT_MS);
  }
  CHECK(status == FELIX_STATUS_OK && r.count == 4, "describe_stream gave %d, %zu",
        (int)status, r.count);
  for (size_t i = 0; status == FELIX_STATUS_OK && i < r.count && i < PARITY_SHARDS; i++) {
    CHECK(r.infos[i].dead_letters == 0, "shard %zu has dead letters", i);
  }
  waiter_destroy(&r.w);

  pres_init(&r);
  status = felix_client_group_seek_stream(admin, t, n, s, group,
                                          FELIX_START_LATEST, 0, NULL,
                                          on_positions, &r);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&r.w, WAIT_MS);
  }
  CHECK(status == FELIX_STATUS_OK && r.count == 4, "seek_stream gave %d, %zu",
        (int)status, r.count);
  waiter_destroy(&r.w);

  /* A token that is already cancelled cancels the poll. */
  FelixCancelToken *cancel = felix_cancel_token_new();
  felix_cancel_token_cancel(cancel);
  pres_init(&r);
  status = felix_sharded_group_poll(sg, 1, cancel, on_sharded_poll, &r);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&r.w, WAIT_MS);
  }
  CHECK(status == FELIX_STATUS_CANCELLED, "a cancelled sharded poll gave %d",
        (int)status);
  waiter_destroy(&r.w);
  felix_cancel_token_free(cancel);

  pres_init(&r);
  status = felix_sharded_group_poll(sg, 0, NULL, on_sharded_poll, &r);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "max_records 0 gave %d",
        (int)status);
  waiter_destroy(&r.w);
  felix_sharded_group_free(sg);

  pres_init(&r);
  status = felix_client_group_delete_stream(admin, t, n, s, group, NULL,
                                            on_stream_deleted, &r);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&r.w, WAIT_MS);
  }
  CHECK(status == FELIX_STATUS_OK && r.existed, "delete_stream gave %d, existed %d",
        (int)status, (int)r.existed);
  waiter_destroy(&r.w);
  pres_init(&r);
  status = felix_client_group_delete_stream(admin, t, n, s, group, NULL,
                                            on_stream_deleted, &r);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&r.w, WAIT_MS);
  }
  CHECK(status == FELIX_STATUS_OK && !r.existed,
        "a second delete_stream gave %d, existed %d", (int)status, (int)r.existed);
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
  SCENARIO_BEGIN("cache.put_get");
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
  SCENARIO_END("cache.put_get");
  SCENARIO_BEGIN("cache.miss_is_absent");
  snprintf(key2, sizeof key2, "%smissing", pfx);
  status = c_get(&sharded, key2, false, &r);
  CHECK(status == FELIX_STATUS_OK && !r.found, "a missing key gave %d, found %d",
        (int)status, (int)r.found);
  SCENARIO_END("cache.miss_is_absent");

  /* A value with a TTL reads back, then stops reading after it. Expiry is
     lazy, so the test is that it reads as absent, not that storage shrank. */
  SCENARIO_BEGIN("cache.ttl_expires");
  snprintf(key2, sizeof key2, "%sbrief", pfx);
  status = c_put(&sharded, key2, "brief", 1000);
  CHECK(status == FELIX_STATUS_OK, "put with a TTL gave %d", (int)status);
  status = c_get(&sharded, key2, false, &r);
  CHECK(status == FELIX_STATUS_OK && r.found && strcmp(r.value, "brief") == 0,
        "a value within its TTL gave %d, found %d", (int)status, (int)r.found);
  pause_ms(2000);
  status = c_get(&sharded, key2, false, &r);
  CHECK(status == FELIX_STATUS_OK && !r.found,
        "a value past its TTL gave %d, found %d", (int)status, (int)r.found);
  SCENARIO_END("cache.ttl_expires");

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
  SCENARIO_BEGIN("cache.delete_returns_previous");
  status = c_delete(&sharded, key2, &r);
  CHECK(status == FELIX_STATUS_OK && r.found && strcmp(r.value, "new") == 0,
        "delete gave %d, found %d, '%s'", (int)status, (int)r.found, r.value);
  status = c_delete(&sharded, key2, &r);
  CHECK(status == FELIX_STATUS_OK && !r.found,
        "a second delete gave %d, found %d", (int)status, (int)r.found);
  SCENARIO_END("cache.delete_returns_previous");

  /* Counters: missing, then added to. */
  snprintf(key, sizeof key, "%scount", pfx);
  SCENARIO_BEGIN("counter.get_absent_is_none");
  status = c_counter(&sharded, key, &r);
  CHECK(status == FELIX_STATUS_OK && !r.has_counter,
        "a missing counter gave %d, has %d", (int)status, (int)r.has_counter);
  SCENARIO_END("counter.get_absent_is_none");
  SCENARIO_BEGIN("counter.add_returns_sum");
  status = c_add(&sharded, key, 5, &r);
  CHECK(status == FELIX_STATUS_OK && r.has_counter && r.counter == 5,
        "first add gave %d, %" PRId64, (int)status, r.counter);
  status = c_add(&sharded, key, -2, &r);
  CHECK(status == FELIX_STATUS_OK && r.has_counter && r.counter == 3,
        "second add gave %d, %" PRId64, (int)status, r.counter);
  status = c_counter(&sharded, key, &r);
  CHECK(status == FELIX_STATUS_OK && r.has_counter && r.counter == 3,
        "counter get gave %d, %" PRId64, (int)status, r.counter);
  SCENARIO_END("counter.add_returns_sum");

  /* A key watch sees a put with its expiry, then the delete. */
  snprintf(key, sizeof key, "%swatched", pfx);
  SCENARIO_BEGIN("watch.delivers_changes_for_the_filter");
  FelixCacheWatch *watch = w_open(&sharded, FELIX_WATCH_KEY, key, false);
  if (watch) {
    /* A neighbour's change comes first and must not arrive. */
    char neighbour[160];
    snprintf(neighbour, sizeof neighbour, "%swatched-not", pfx);
    CHECK(c_put(&sharded, neighbour, "other", 0) == FELIX_STATUS_OK,
          "neighbour put");
    CHECK(c_put(&sharded, key, "w1", 60000) == FELIX_STATUS_OK, "watched put");
    status = w_next(watch, 10000, &it);
    CHECK(status == FELIX_STATUS_OK && it.has_item &&
              it.kind == FELIX_WATCH_CHANGE && strcmp(it.key, key) == 0 &&
              it.has_value && strcmp(it.value, "w1") == 0 &&
              it.expires_at_millis > 0 && !it.has_shard,
          "the put arrived as status %d kind %d key '%s' value '%s' expiry %" PRIu64,
          (int)status, (int)it.kind, it.key, it.value, it.expires_at_millis);
    SCENARIO_END("watch.delivers_changes_for_the_filter");
    SCENARIO_BEGIN("watch.deletes_arrive_as_changes_without_values");
    CHECK(c_delete(&sharded, key, &r) == FELIX_STATUS_OK, "watched delete");
    status = w_next(watch, 10000, &it);
    CHECK(status == FELIX_STATUS_OK && it.has_item &&
              it.kind == FELIX_WATCH_CHANGE && strcmp(it.key, key) == 0 &&
              !it.has_value,
          "the delete arrived as status %d kind %d has_value %d", (int)status,
          (int)it.kind, (int)it.has_value);
    SCENARIO_END("watch.deletes_arrive_as_changes_without_values");
    /* Nothing more is waiting. */
    status = w_next(watch, 150, &it);
    CHECK(status == FELIX_STATUS_TIMEOUT, "an idle watch gave %d", (int)status);
    status = felix_cache_watch_resume_offsets(watch, NULL, NULL, 0, NULL);
    CHECK(status == FELIX_STATUS_INVALID_ARGUMENT,
          "resume offsets of a single watch gave %d", (int)status);
    felix_cache_watch_free(watch);
  }

  /* A retained prefix watch delivers what is stored, then live changes. */
  SCENARIO_BEGIN("watch.retained_delivers_state_before_changes");
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
  SCENARIO_END("watch.retained_delivers_state_before_changes");

  /* A watch that falls behind says so as a value carrying where to resume.
     Forcing an overflow takes a flood this run cannot make reliably, so this
     asserts what a healthy watch delivers, and that if the broker did end it
     for lag the item names the offset to resume from. */
  SCENARIO_BEGIN("watch.lag_is_reported_rather_than_silent");
  watch = w_open(&single, FELIX_WATCH_PREFIX, pfx, false);
  if (watch) {
    snprintf(key, sizeof key, "%slag", pfx);
    CHECK(c_put(&single, key, "v", 0) == FELIX_STATUS_OK, "lag put");
    status = w_next(watch, 10000, &it);
    CHECK(status == FELIX_STATUS_OK && it.has_item &&
              (it.kind == FELIX_WATCH_CHANGE || it.kind == FELIX_WATCH_LAGGED ||
               it.kind == FELIX_WATCH_SHARD_MOVED),
          "the watch gave %d, kind %d", (int)status, (int)it.kind);
    CHECK(!it.has_item || it.kind != FELIX_WATCH_LAGGED || it.has_resume_from,
          "a lag item carried no offset to resume from");
    felix_cache_watch_free(watch);
    c_delete(&single, key, &r);
  }
  SCENARIO_END("watch.lag_is_reported_rather_than_silent");

  /* A sharded watch sees puts spread over the shards; resuming from its
     offsets sees only what came after. */
  SCENARIO_BEGIN("watch.resumes_from_an_offset");
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
  SCENARIO_END("watch.resumes_from_an_offset");

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

/* The count of commits and state reads the no-commit broker was sent, from the
   control endpoint; -1 if it could not be read. */
static long no_commit_requests(const struct fixture *f) {
  char command[MAX_LINE + 64];
  snprintf(command, sizeof command, "curl -sS -m 10 '%s/no-commit/requests' 2>/dev/null",
           f->control_url);
  FILE *pipe = popen(command, "r");
  if (!pipe) {
    return -1;
  }
  char out[64];
  size_t used = fread(out, 1, sizeof out - 1, pipe);
  out[used] = '\0';
  pclose(pipe);
  return used ? atol(out) : -1;
}

/* A client sends commit and state_get only to a broker that advertised
   FEATURE_ATOMIC_COMMIT: against one that advertised nothing both fail at the
   call with UNSUPPORTED, and nothing reaches the broker. The commit that
   succeeds against a broker that did advertise it is in commits(). */
static void commits_negotiated(const struct fixture *f, const char *run) {
  if (!f->no_commit_addrs[0] || !f->control_url[0]) {
    SKIP("commit.negotiated", "the fixture file has no no_commit_addrs or control_url");
    return;
  }
  FelixClient *client = NULL;
  SCENARIO_BEGIN("commit.negotiated");
  REQUIRE_OK(connect_sync(f->no_commit_addrs, f->tenant, f->token, f->ca_file, &client));
  struct kctx k = {client, f};
  struct kres r;
  char key[160];
  snprintf(key, sizeof key, "cn-%s", run);
  FelixStatus status = k_commit(&k, -1, key, "placed", key, "placed", false, 0, &r);
  CHECK(status == FELIX_STATUS_UNSUPPORTED, "commit to a broker without the feature gave %d",
        (int)status);
  status = k_state(&k, -1, key, key, &r);
  CHECK(status == FELIX_STATUS_UNSUPPORTED, "state_get from a broker without the feature gave %d",
        (int)status);
  long sent = no_commit_requests(f);
  CHECK(sent == 0, "the broker without the feature was sent %ld commits or state reads", sent);
  felix_client_free(client);
  SCENARIO_END("commit.negotiated");
}

static void commits(const struct fixture *f, const char *run) {
  FelixClient *client = NULL;
  REQUIRE_OK(connect_sync(f->addrs, f->tenant, f->token, f->ca_file, &client));
  struct kctx k = {client, f};
  struct kres r;
  char key[160], other[160];
  snprintf(key, sizeof key, "cm-%s-a", run);

  /* A commit returns its offset, and its state reads back at that version.
     The commit is accepted, so the broker advertised commits. */
  SCENARIO_BEGIN("commit.state_get_returns_the_version");
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
  SCENARIO_END("commit.state_get_returns_the_version");

  /* Conditional commits, routed by the entity key and by shard. */
  SCENARIO_BEGIN("commit.expected_offset_refuses_a_stale_write");
  snprintf(other, sizeof other, "cm-%s-if", run);
  check_conditional(&k, "commit_if", cond_commit, -1, other);
  snprintf(other, sizeof other, "cm-%s-ifs", run);
  check_conditional(&k, "commit_if_on_shard", cond_commit, 2, other);
  SCENARIO_END("commit.expected_offset_refuses_a_stale_write");

  /* A commit on a named shard reads back from that shard, and its event is
     delivered there at the offset the commit answered with. */
  SCENARIO_BEGIN("commit.returns_the_offset");
  SCENARIO_BEGIN("explicit_shard.commit_lands_on_the_named_shard");
  snprintf(other, sizeof other, "cm-%s-shard", run);
  char event[96];
  snprintf(event, sizeof event, "cm-%s-event", run);
  status = k_commit(&k, 2, other, event, other, "shard-value", false, 0, &r);
  CHECK(status == FELIX_STATUS_OK, "commit_on_shard gave %d", (int)status);
  offset = r.offset;
  if (status == FELIX_STATUS_OK) {
    FelixSubscription *placed = NULL;
    CHECK(subscribe_shard(client, f, 2, FELIX_START_OFFSET, offset, &placed) ==
              FELIX_STATUS_OK,
          "subscribing at the commit's offset failed");
    if (placed) {
      int seen = 0;
      uint64_t at = 0;
      CHECK(read_until(placed, event, &seen, &at) && at == offset && seen == 1,
            "the commit's event was at %" PRIu64 " after %d events, not first at %"
            PRIu64,
            at, seen, offset);
      felix_subscription_free(placed);
    }
  }
  SCENARIO_END("commit.returns_the_offset");
  status = k_state(&k, 2, other, other, &r);
  CHECK(status == FELIX_STATUS_OK && r.has_value &&
            strcmp(r.value, "shard-value") == 0 && r.has_version &&
            r.version == offset,
        "state_get_on_shard gave %d, '%s', version %" PRIu64
        " (commit at %" PRIu64 ")",
        (int)status, r.value, r.version, offset);
  SCENARIO_END("explicit_shard.commit_lands_on_the_named_shard");

  /* Conditional publishes. */
  SCENARIO_BEGIN("publish_if.lands_at_the_expected_offset");
  SCENARIO_BEGIN("publish_if.refused_with_the_tail");
  snprintf(other, sizeof other, "cm-%s-pub", run);
  check_conditional(&k, "publish_if", cond_publish, -1, other);
  check_conditional(&k, "publish_if_on_shard", cond_publish, 3, other);
  SCENARIO_END("publish_if.refused_with_the_tail");
  SCENARIO_END("publish_if.lands_at_the_expected_offset");

  /* Commits the call itself refuses; the callback never runs. */
  struct kres bad;
  kres_init(&bad);
  SCENARIO_BEGIN("commit.exactly_one_event");
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
  FelixCommitOp state_only[1];
  memset(state_only, 0, sizeof state_only);
  state_only[0].kind = FELIX_COMMIT_PUT;
  state_only[0].stream = f->stream;
  state_only[0].key = key;
  status = felix_client_commit(client, f->tenant, f->ns, (const uint8_t *)key,
                               strlen(key), state_only, 1, NULL, on_commit_done,
                               &bad);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT, "no event gave %d",
        (int)status);
  SCENARIO_END("commit.exactly_one_event");
  /* An operation on another stream is refused before anything is sent. */
  SCENARIO_BEGIN("commit.another_stream_is_refused");
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
  SCENARIO_END("commit.another_stream_is_refused");
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
    SCENARIO_BEGIN("client.close_is_idempotent");
    waiter_init(&w);
    status = felix_producer_close(p, -1, NULL, on_producer_done, &w);
    if (status == FELIX_STATUS_OK) {
      status = wait_for(&w, WAIT_MS);
    }
    CHECK(status == FELIX_STATUS_CLOSED, "a second close gave %d", (int)status);
    waiter_destroy(&w);
    SCENARIO_END("client.close_is_idempotent");
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

/* ---- scenarios no earlier section covers ------------------------------- */

static FelixStatus publish_acked(FelixClient *client, const struct fixture *f,
                                 const char *payload, FelixAckMode ack,
                                 FelixDelivery delivery) {
  struct waiter w;
  waiter_init(&w);
  FelixStatus status = felix_client_publish(
      client, f->tenant, f->ns, f->stream, (const uint8_t *)payload,
      strlen(payload), NULL, 0, ack, delivery, NULL, on_publish, &w);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, WAIT_MS);
  }
  waiter_destroy(&w);
  return status;
}

static double seconds_since(const struct timespec *began) {
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  return (double)(now.tv_sec - began->tv_sec) +
         (double)(now.tv_nsec - began->tv_nsec) / 1e9;
}

#define THREADS 8
#define PER_THREAD 3

struct pub_job {
  const struct fixture *f;
  FelixClient *client;
  char payloads[PER_THREAD][96];
  FelixStatus status[PER_THREAD];
};

static void *pub_worker(void *arg) {
  struct pub_job *job = arg;
  for (int i = 0; i < PER_THREAD; i++) {
    job->status[i] = publish(job->client, job->f, job->f->stream,
                             job->payloads[i], NULL, NULL, 0);
  }
  return NULL;
}

/* Several threads share one client, and every publish lands. */
static void concurrency(const struct fixture *f, const char *run) {
  FelixClient *client = NULL;
  REQUIRE_OK(connect_sync(f->addrs, f->tenant, f->token, f->ca_file, &client));
  SCENARIO_BEGIN("client.concurrent_calls");
  FelixSubscription *sub = NULL;
  FelixStatus status = subscribe(client, f, FELIX_START_LATEST, 0, &sub);
  CHECK(status == FELIX_STATUS_OK, "subscribe gave %d", (int)status);
  if (status != FELIX_STATUS_OK) {
    felix_client_free(client);
    return;
  }
  struct pub_job jobs[THREADS];
  pthread_t threads[THREADS];
  bool started[THREADS];
  for (int t = 0; t < THREADS; t++) {
    jobs[t].f = f;
    jobs[t].client = client;
    for (int i = 0; i < PER_THREAD; i++) {
      snprintf(jobs[t].payloads[i], sizeof jobs[t].payloads[i],
               "c-%s-conc-%d-%d", run, t, i);
    }
    started[t] = pthread_create(&threads[t], NULL, pub_worker, &jobs[t]) == 0;
    CHECK(started[t], "could not start thread %d", t);
  }
  for (int t = 0; t < THREADS; t++) {
    if (!started[t]) {
      continue;
    }
    pthread_join(threads[t], NULL);
    for (int i = 0; i < PER_THREAD; i++) {
      CHECK(jobs[t].status[i] == FELIX_STATUS_OK,
            "concurrent publish %d/%d gave %d", t, i, (int)jobs[t].status[i]);
    }
  }

  /* Arrival order across threads is not defined; only that none is lost. */
  bool seen[THREADS][PER_THREAD];
  memset(seen, 0, sizeof seen);
  int found = 0;
  for (int reads = 0; found < THREADS * PER_THREAD && reads < 10000; reads++) {
    struct waiter w;
    waiter_init(&w);
    status = felix_subscription_next(sub, WAIT_MS, NULL, on_event, &w);
    if (status == FELIX_STATUS_OK) {
      status = wait_for(&w, WAIT_MS * 2);
    }
    FelixEvent *event = status == FELIX_STATUS_OK ? w.handle : NULL;
    waiter_destroy(&w);
    if (!event) {
      CHECK(0, "concurrent read gave %d after %d of %d", (int)status, found,
            THREADS * PER_THREAD);
      break;
    }
    const uint8_t *data = NULL;
    size_t len = 0;
    felix_event_payload(event, &data, &len);
    for (int t = 0; t < THREADS; t++) {
      for (int i = 0; i < PER_THREAD; i++) {
        if (!seen[t][i] && len == strlen(jobs[t].payloads[i]) &&
            memcmp(data, jobs[t].payloads[i], len) == 0) {
          seen[t][i] = true;
          found++;
        }
      }
    }
    felix_event_free(event);
  }
  CHECK(found == THREADS * PER_THREAD, "%d of %d concurrent publishes arrived",
        found, THREADS * PER_THREAD);
  felix_subscription_free(sub);
  felix_client_free(client);
  SCENARIO_END("client.concurrent_calls");
}

/* Acknowledgement modes, start positions, delivery choices and the calls that
   close things. */
static void delivery(const struct fixture *f, const char *run) {
  FelixClient *client = NULL;
  REQUIRE_OK(connect_sync(f->addrs, f->tenant, f->token, f->ca_file, &client));
  char payload[96];
  FelixStatus status;

  /* A per-message ack means the broker has the record: it reads back at the
     offset the ack named, with no wait in between. */
  SCENARIO_BEGIN("ack.per_message_means_the_broker_accepted_it");
  snprintf(payload, sizeof payload, "c-%s-acked", run);
  struct waiter w;
  waiter_init(&w);
  status = felix_client_publish(client, f->tenant, f->ns, f->stream,
                                (const uint8_t *)payload, strlen(payload), NULL,
                                0, FELIX_ACK_PER_MESSAGE,
                                FELIX_DELIVERY_AT_MOST_ONCE, NULL, on_publish,
                                &w);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, WAIT_MS);
  }
  CHECK(status == FELIX_STATUS_OK && w.has_offset,
        "an acked publish gave %d, has_offset %d", (int)status,
        (int)w.has_offset);
  if (status == FELIX_STATUS_OK && w.has_offset) {
    FelixSubscription *sub = NULL;
    uint64_t at = w.offset, found_at = 0;
    int seen = 0;
    status = subscribe(client, f, FELIX_START_OFFSET, at, &sub);
    CHECK(status == FELIX_STATUS_OK, "subscribe at the acked offset gave %d",
          (int)status);
    if (sub) {
      CHECK(read_until(sub, payload, &seen, &found_at) && found_at == at &&
                seen == 1,
            "the acked record was not first at offset %" PRIu64
            " (found at %" PRIu64 " after %d events)",
            at, found_at, seen);
      felix_subscription_free(sub);
    }
  }
  waiter_destroy(&w);
  SCENARIO_END("ack.per_message_means_the_broker_accepted_it");

  /* Unacknowledged publishes complete without a broker round trip. The bound
     is loose enough for a loaded machine and tight enough to catch a client
     that waits anyway. */
  SCENARIO_BEGIN("ack.none_does_not_wait_for_the_broker");
  snprintf(payload, sizeof payload, "c-%s-warm", run);
  CHECK(publish(client, f, f->stream, payload, NULL, NULL, 0) ==
            FELIX_STATUS_OK,
        "the warm-up publish failed");
  struct timespec began;
  clock_gettime(CLOCK_MONOTONIC, &began);
  for (int i = 0; i < 20; i++) {
    snprintf(payload, sizeof payload, "c-%s-none-%d", run, i);
    status = publish_acked(client, f, payload, FELIX_ACK_NONE,
                           FELIX_DELIVERY_AT_MOST_ONCE);
    CHECK(status == FELIX_STATUS_OK, "unacked publish %d gave %d", i,
          (int)status);
  }
  double took = seconds_since(&began);
  CHECK(took < 2.0, "20 unacknowledged publishes took %.2fs", took);
  SCENARIO_END("ack.none_does_not_wait_for_the_broker");

  /* Re-sending is opt-in: the choice exists, the plain publish is at most
     once, and an unknown choice is refused rather than defaulted. That an
     at-most-once publish with an unknown outcome is not re-sent is asserted
     in faults(), where one is produced. */
  SCENARIO_BEGIN("retry.ambiguous_outcomes_are_not_silently_retried");
  snprintf(payload, sizeof payload, "c-%s-once", run);
  status = publish_acked(client, f, payload, FELIX_ACK_PER_MESSAGE,
                         FELIX_DELIVERY_AT_MOST_ONCE);
  CHECK(status == FELIX_STATUS_OK, "an at-most-once publish gave %d",
        (int)status);
  snprintf(payload, sizeof payload, "c-%s-at-least", run);
  status = publish_acked(client, f, payload, FELIX_ACK_PER_MESSAGE,
                         FELIX_DELIVERY_AT_LEAST_ONCE);
  CHECK(status == FELIX_STATUS_OK, "an at-least-once publish gave %d",
        (int)status);
  waiter_init(&w);
  status = felix_client_publish(client, f->tenant, f->ns, f->stream,
                                (const uint8_t *)payload, strlen(payload), NULL,
                                0, FELIX_ACK_PER_MESSAGE, 7, NULL, on_publish,
                                &w);
  CHECK(status == FELIX_STATUS_INVALID_ARGUMENT,
        "delivery 7 gave %d, not a refusal", (int)status);
  waiter_destroy(&w);
  SCENARIO_END("retry.ambiguous_outcomes_are_not_silently_retried");

  /* Earliest is as far back as the log goes: records published before this
     subscription existed, ours among them, not only the live tail. */
  SCENARIO_BEGIN("pubsub.start_earliest");
  snprintf(payload, sizeof payload, "c-%s-earliest", run);
  CHECK(publish(client, f, f->stream, payload, NULL, NULL, 0) ==
            FELIX_STATUS_OK,
        "publish before the earliest subscription failed");
  FelixSubscription *sub = NULL;
  status = subscribe(client, f, FELIX_START_EARLIEST, 0, &sub);
  CHECK(status == FELIX_STATUS_OK, "earliest subscribe gave %d", (int)status);
  if (sub) {
    int seen = 0;
    uint64_t at = 0;
    CHECK(read_until(sub, payload, &seen, &at), "history did not include %s",
          payload);
    CHECK(seen > 1, "the earliest subscription began at the record published "
                    "just before it");
    felix_subscription_free(sub);
  }
  SCENARIO_END("pubsub.start_earliest");

  /* A dead seed among live ones does not stop the client. */
  SCENARIO_BEGIN("retry.transport_failures_are_retried_against_another_broker");
  char seeds[MAX_LINE + 32];
  snprintf(seeds, sizeof seeds, "127.0.0.1:1,%.*s", MAX_LINE - 1, f->addrs);
  FelixClient *seeded = NULL;
  status = connect_sync(seeds, f->tenant, f->token, f->ca_file, &seeded);
  CHECK(status == FELIX_STATUS_OK, "connecting past a dead seed gave %d",
        (int)status);
  if (seeded) {
    snprintf(payload, sizeof payload, "c-%s-deadseed", run);
    CHECK(publish(seeded, f, f->stream, payload, NULL, NULL, 0) ==
              FELIX_STATUS_OK,
          "a publish past a dead seed failed");
    felix_client_free(seeded);
  }
  SCENARIO_END("retry.transport_failures_are_retried_against_another_broker");

  /* Freeing nothing is safe for every handle, and cancelling a token after
     its call finished, and after the client is gone, neither runs the callback
     again nor crashes. */
  SCENARIO_BEGIN("client.close_is_idempotent");
  felix_client_free(NULL);
  felix_subscription_free(NULL);
  felix_sharded_subscription_free(NULL);
  felix_cache_watch_free(NULL);
  felix_producer_free(NULL);
  felix_event_free(NULL);
  felix_cancel_token_cancel(NULL);
  felix_cancel_token_free(NULL);
  FelixClient *closing = NULL;
  status = connect_sync(f->addrs, f->tenant, f->token, f->ca_file, &closing);
  CHECK(status == FELIX_STATUS_OK, "connect gave %d", (int)status);
  if (closing) {
    FelixCancelToken *token = felix_cancel_token_new();
    snprintf(payload, sizeof payload, "c-%s-closing", run);
    waiter_init(&w);
    status = felix_client_publish(closing, f->tenant, f->ns, f->stream,
                                  (const uint8_t *)payload, strlen(payload),
                                  NULL, 0, FELIX_ACK_PER_MESSAGE,
                                  FELIX_DELIVERY_AT_MOST_ONCE, token,
                                  on_publish, &w);
    if (status == FELIX_STATUS_OK) {
      status = wait_for(&w, WAIT_MS);
    }
    CHECK(status == FELIX_STATUS_OK, "the publish under a token gave %d",
          (int)status);
    felix_cancel_token_cancel(token);
    felix_cancel_token_cancel(token);
    felix_client_free(closing);
    felix_cancel_token_cancel(token);
    felix_cancel_token_free(token);
    pause_ms(100);
    CHECK(w.calls == 1, "the callback ran %d times", w.calls);
    waiter_destroy(&w);
  }
  SCENARIO_END("client.close_is_idempotent");
  felix_client_free(client);
}

/* Subscribing and publishing through each broker alone, so for all but one of
   them the shard's owner is somebody else. */
static void redirects(const struct fixture *f, const char *run) {
  char list[MAX_LINE];
  snprintf(list, sizeof list, "%s", f->addrs);
  char *addrs[16];
  size_t count = 0;
  for (char *save = NULL, *a = strtok_r(list, ",", &save); a && count < 16;
       a = strtok_r(NULL, ",", &save)) {
    addrs[count++] = a;
  }
  /* A shard has one owner, so with two or more brokers each asked alone, at
     least one is not the owner and the request has to be routed. */
  if (count < 2) {
    char reason[96];
    snprintf(reason, sizeof reason,
             "the fixture has %zu broker; a redirect needs a non-owner", count);
    SKIP("redirect.follows_to_the_shard_owner", reason);
    SKIP("redirect.is_not_reported_as_a_failure", reason);
    SKIP("redirect.carries_the_start_offset_through_every_hop", reason);
    return;
  }

  SCENARIO_BEGIN("redirect.follows_to_the_shard_owner");
  SCENARIO_BEGIN("redirect.is_not_reported_as_a_failure");
  for (size_t i = 0; i < count; i++) {
    FelixClient *client = NULL;
    FelixStatus status =
        connect_sync(addrs[i], f->tenant, f->token, f->ca_file, &client);
    CHECK(status == FELIX_STATUS_OK, "connect to %s gave %d", addrs[i],
          (int)status);
    if (!client) {
      continue;
    }
    FelixSubscription *sub = NULL;
    status = subscribe(client, f, FELIX_START_LATEST, 0, &sub);
    CHECK(status == FELIX_STATUS_OK, "subscribe through %s gave %d", addrs[i],
          (int)status);
    if (sub) {
      char payload[96];
      snprintf(payload, sizeof payload, "c-%s-redirect-%zu", run, i);
      status = publish(client, f, f->stream, payload, NULL, NULL, 0);
      CHECK(status == FELIX_STATUS_OK, "publish through %s gave %d", addrs[i],
            (int)status);
      const char *one[1] = {payload};
      uint64_t offset[1] = {0};
      read_expected(sub, one, 1, offset);
      felix_subscription_free(sub);
    }
    felix_client_free(client);
  }
  SCENARIO_END("redirect.is_not_reported_as_a_failure");
  SCENARIO_END("redirect.follows_to_the_shard_owner");

  /* Records exist before the subscription does, so a redirect that dropped
     the start offset would begin at the tail and never see the first. */
  SCENARIO_BEGIN("redirect.carries_the_start_offset_through_every_hop");
  FelixClient *writer = NULL;
  FelixStatus status =
      connect_sync(addrs[0], f->tenant, f->token, f->ca_file, &writer);
  CHECK(status == FELIX_STATUS_OK, "connect to %s gave %d", addrs[0],
        (int)status);
  if (writer) {
    enum { HISTORY = 6 };
    char payloads[HISTORY][96];
    const char *expected[HISTORY];
    FelixSubscription *watch = NULL;
    status = subscribe(writer, f, FELIX_START_LATEST, 0, &watch);
    CHECK(status == FELIX_STATUS_OK, "subscribe gave %d", (int)status);
    uint64_t offsets[HISTORY] = {0};
    bool placed = false;
    if (watch) {
      for (int h = 0; h < HISTORY; h++) {
        snprintf(payloads[h], sizeof payloads[h], "c-%s-history-%d", run, h);
        expected[h] = payloads[h];
        CHECK(publish(writer, f, f->stream, payloads[h], NULL, NULL, 0) ==
                  FELIX_STATUS_OK,
              "history publish %d failed", h);
      }
      placed = read_expected(watch, expected, HISTORY, offsets);
      felix_subscription_free(watch);
    }
    for (size_t i = 0; placed && i < count; i++) {
      FelixClient *client = NULL;
      status = connect_sync(addrs[i], f->tenant, f->token, f->ca_file, &client);
      CHECK(status == FELIX_STATUS_OK, "connect to %s gave %d", addrs[i],
            (int)status);
      if (!client) {
        continue;
      }
      FelixSubscription *resumed = NULL;
      status = subscribe(client, f, FELIX_START_OFFSET, offsets[0], &resumed);
      CHECK(status == FELIX_STATUS_OK, "resume through %s gave %d", addrs[i],
            (int)status);
      if (resumed) {
        const char *first[1] = {payloads[0]};
        uint64_t at[1] = {0};
        if (read_expected(resumed, first, 1, at)) {
          CHECK(at[0] == offsets[0],
                "through %s the record read at %" PRIu64
                ", asked for %" PRIu64,
                addrs[i], at[0], offsets[0]);
        }
        felix_subscription_free(resumed);
      }
      felix_client_free(client);
    }
    felix_client_free(writer);
  }
  SCENARIO_END("redirect.carries_the_start_offset_through_every_hop");
}

/* ---- faults the fixture's control endpoint produces --------------------- */

/* POSTs to the control endpoint and returns the HTTP status, 0 if curl could
   not get one. The body is copied to `body`. */
static int control_post_json(const struct fixture *f, const char *path,
                             const char *json, char *body, size_t cap) {
  char command[MAX_LINE + 512];
  int n = snprintf(command, sizeof command,
                   "curl -sS -m 150 -X POST %s%s%s -w '\n%%{http_code}' '%s%s' "
                   "2>/dev/null",
                   json ? "-H 'content-type: application/json' -d '" : "",
                   json ? json : "", json ? "'" : "", f->control_url, path);
  body[0] = '\0';
  if (n < 0 || (size_t)n >= sizeof command) {
    return 0;
  }
  FILE *pipe = popen(command, "r");
  if (!pipe) {
    return 0;
  }
  char out[2048];
  size_t used = fread(out, 1, sizeof out - 1, pipe);
  out[used] = '\0';
  pclose(pipe);
  char *last = strrchr(out, '\n');
  if (!last) {
    return 0;
  }
  int code = atoi(last + 1);
  *last = '\0';
  snprintf(body, cap, "%s", out);
  return code;
}

static int control_post(const struct fixture *f, const char *path, char *body,
                        size_t cap) {
  return control_post_json(f, path, NULL, body, cap);
}

/* The broker address in a control answer, `{"node_id":..,"addr":".."}`. */
static bool control_addr(const char *body, char *addr, size_t cap) {
  const char *at = strstr(body, "\"addr\":\"");
  if (!at) {
    return false;
  }
  at += strlen("\"addr\":\"");
  const char *end = strchr(at, '"');
  if (!end || (size_t)(end - at) >= cap || end == at) {
    return false;
  }
  memcpy(addr, at, (size_t)(end - at));
  addr[end - at] = '\0';
  return true;
}

/* How many copies of `payload` `stream` holds, read from the start until the
   feed has been idle for a few seconds. */
static int copies_of(const struct fixture *f, const char *stream,
                     const char *payload) {
  struct fixture q = *f;
  q.stream = stream;
  FelixClient *client = NULL;
  if (connect_sync(f->addrs, f->tenant, f->token, f->ca_file, &client) !=
      FELIX_STATUS_OK) {
    return -1;
  }
  FelixSubscription *sub = NULL;
  int copies = -1;
  if (subscribe(client, &q, FELIX_START_EARLIEST, 0, &sub) == FELIX_STATUS_OK) {
    copies = 0;
    for (int i = 0; i < 100000; i++) {
      struct waiter w;
      waiter_init(&w);
      FelixStatus status = felix_subscription_next(sub, 3000, NULL, on_event, &w);
      if (status == FELIX_STATUS_OK) {
        status = wait_for(&w, WAIT_MS * 2);
      }
      FelixEvent *event = status == FELIX_STATUS_OK ? w.handle : NULL;
      waiter_destroy(&w);
      if (!event) {
        if (status != FELIX_STATUS_TIMEOUT) {
          copies = -1;
        }
        break;
      }
      const uint8_t *data = NULL;
      size_t len = 0;
      felix_event_payload(event, &data, &len);
      if (len == strlen(payload) && memcmp(data, payload, len) == 0) {
        copies++;
      }
      felix_event_free(event);
    }
    felix_subscription_free(sub);
  }
  felix_client_free(client);
  return copies;
}

/* Heals, and waits until `stream` takes a publish through `addr` again, so the
   next scenario and the other build find a working cluster. */
static bool heal_and_recover(const struct fixture *f, const char *addr,
                             const char *stream, const char *run) {
  char body[2048];
  int code = control_post(f, "/heal", body, sizeof body);
  CHECK(code == 200, "heal answered %d: %s", code, body);
  struct timespec began;
  clock_gettime(CLOCK_MONOTONIC, &began);
  FelixClient *client = NULL;
  while (seconds_since(&began) < 90.0) {
    if (!client &&
        connect_sync(addr, f->tenant, f->token, f->ca_file, &client) !=
            FELIX_STATUS_OK) {
      client = NULL;
    }
    if (client) {
      char payload[96];
      snprintf(payload, sizeof payload, "c-%s-healed", run);
      if (publish(client, f, stream, payload, NULL, NULL, 0) ==
          FELIX_STATUS_OK) {
        felix_client_free(client);
        return true;
      }
    }
    pause_ms(250);
  }
  felix_client_free(client);
  CHECK(0, "%s never took a publish again after the heal", stream);
  return false;
}

static void faults(const struct fixture *f, const char *run) {
  static const char *const ids[] = {"error.shard_unavailable_is_retryable",
                                    "error.quorum_timeout_is_outcome_unknown"};
  const char *absent = !f->control_url[0]      ? "control_url"
                       : !f->movable_stream[0] ? "movable_stream"
                                               : !f->quorum_stream[0]
                                                     ? "quorum_stream"
                                                     : NULL;
  if (absent) {
    char reason[128];
    snprintf(reason, sizeof reason, "the fixture file has no %s", absent);
    for (size_t i = 0; i < 2; i++) {
      SKIP(ids[i], reason);
    }
    return;
  }
  char body[2048], addr[256];

  /* A fenced shard refuses and says so is safe to retry. Published straight
     to the fenced owner: through another broker the answer would be about the
     forward. */
  int code = control_post(f, "/fence", body, sizeof body);
  if (code < 200 || code > 299) {
    char reason[2200];
    snprintf(reason, sizeof reason, "POST /fence answered %d: %.2000s", code,
             body);
    CHECK(code == 409, "%s", reason);
    SKIP(ids[0], reason);
  } else {
    SCENARIO_BEGIN("error.shard_unavailable_is_retryable");
    CHECK(code == 200 && control_addr(body, addr, sizeof addr),
          "fence answered %d: %s", code, body);
    if (code == 200 && control_addr(body, addr, sizeof addr)) {
      FelixClient *client = NULL;
      FelixStatus status =
          connect_sync(addr, f->tenant, f->token, f->ca_file, &client);
      CHECK(status == FELIX_STATUS_OK, "connect to the owner gave %d",
            (int)status);
      if (client) {
        char message[512] = "";
        status = publish(client, f, f->movable_stream, "mid-move", NULL,
                         message, sizeof message);
        CHECK(status == FELIX_STATUS_SHARD_UNAVAILABLE,
              "a publish during a move gave %d (%s)", (int)status, message);
        felix_client_free(client);
      }
      /* Retryable means the same publish lands once the move finishes. */
      CHECK(heal_and_recover(f, addr, f->movable_stream, run),
            "the move never finished");
    }
    SCENARIO_END("error.shard_unavailable_is_retryable");
  }

  /* The leader wrote it and cannot say whether it will survive. */
  code = control_post(f, "/partition", body, sizeof body);
  if (code < 200 || code > 299) {
    char reason[2200];
    snprintf(reason, sizeof reason, "POST /partition answered %d: %.2000s", code,
             body);
    CHECK(code == 409, "%s", reason);
    SKIP(ids[1], reason);
  } else {
    SCENARIO_BEGIN("error.quorum_timeout_is_outcome_unknown");
    SCENARIO_BEGIN("retry.ambiguous_outcomes_are_not_silently_retried");
    char ambiguous[96];
    snprintf(ambiguous, sizeof ambiguous, "c-%s-no-majority", run);
    CHECK(code == 200 && control_addr(body, addr, sizeof addr),
          "partition answered %d: %s", code, body);
    if (code == 200 && control_addr(body, addr, sizeof addr)) {
      FelixClient *client = NULL;
      FelixStatus status =
          connect_sync(addr, f->tenant, f->token, f->ca_file, &client);
      CHECK(status == FELIX_STATUS_OK, "connect to the leader gave %d",
            (int)status);
      if (client) {
        char message[512] = "";
        status = publish(client, f, f->quorum_stream, ambiguous, NULL, message,
                         sizeof message);
        CHECK(status == FELIX_STATUS_OUTCOME_UNKNOWN,
              "a write no majority confirmed gave %d (%s)", (int)status,
              message);
        felix_client_free(client);
      }
      heal_and_recover(f, addr, f->quorum_stream, run);
      /* The publish was at most once and its outcome unknown: a client that
         re-sent it would leave two copies. One or none is right. */
      int copies = copies_of(f, f->quorum_stream, ambiguous);
      CHECK(copies >= 0 && copies <= 1,
            "%d copies of a publish that ended outcome unknown", copies);
    }
    SCENARIO_END("retry.ambiguous_outcomes_are_not_silently_retried");
    SCENARIO_END("error.quorum_timeout_is_outcome_unknown");
  }
}

/* ---- discovery ---------------------------------------------------------- */

/* A client given one broker learns the others while it connects. */
static void discovery(const struct fixture *f) {
  const char *id = "reconnect.discovers_brokers_from_one_seed";
  if (addr_count(f->addrs) < 2) {
    SKIP(id, "the fixture has one broker");
    return;
  }
  SCENARIO_BEGIN(id);
  char seed[128];
  CHECK(addr_at(f->addrs, 0, seed, sizeof seed), "no first address in %s",
        f->addrs);
  FelixClient *client = NULL;
  FelixStatus status =
      connect_sync(seed, f->tenant, f->token, f->ca_file, &client);
  CHECK(status == FELIX_STATUS_OK, "connect to one seed gave %d", (int)status);
  if (client) {
    struct endpoint_list list;
    memset(&list, 0, sizeof list);
    waiter_init(&list.w);
    status = felix_client_endpoints(client, NULL, on_endpoints, &list);
    if (status == FELIX_STATUS_OK) {
      status = wait_for(&list.w, WAIT_MS);
    }
    CHECK(status == FELIX_STATUS_OK, "endpoints gave %d", (int)status);
    CHECK(list.count > 1,
          "the client knows %zu endpoint(s) after one seed; it never learned "
          "the cluster",
          list.count);
    CHECK(list.with_port == list.count, "an endpoint had no port");
    waiter_destroy(&list.w);
    felix_client_free(client);
  }
  SCENARIO_END(id);
}

/* ---- connection faults through the link interposer ---------------------- */

/* Room for the idle timeout to fire and a reconnect to land, on top of the
   fault's own hold. */
#define SETTLE_MS 45000
#define POLL_MS 250
#define PAYLOAD_CAP 160
#define MAX_ACKED 16

/* The payloads (or cache keys) a scenario got an acknowledgement for. */
struct acked {
  char payload[MAX_ACKED][PAYLOAD_CAP];
  size_t count;
};

static void acked_add(struct acked *a, const char *payload) {
  if (a->count < MAX_ACKED) {
    snprintf(a->payload[a->count++], PAYLOAD_CAP, "%s", payload);
  }
}

static bool inject_fault(const struct fixture *f, const struct step *step) {
  char json[192], body[2048];
  snprintf(json, sizeof json, "{\"fault\":\"%s\",\"hold_ms\":%d}", step->fault,
           step->hold_ms);
  int code = control_post_json(f, "/link", json, body, sizeof body);
  CHECK(code == 200, "injecting %s answered %d: %s", step->fault, code, body);
  return code == 200;
}

/* The next event's payload and offset, or the read's status. */
static FelixStatus read_event(FelixSubscription *sub, int timeout_ms,
                              char *payload, size_t cap, bool *has_offset,
                              uint64_t *offset) {
  struct waiter w;
  waiter_init(&w);
  FelixStatus status = felix_subscription_next(sub, timeout_ms, NULL, on_event, &w);
  if (status == FELIX_STATUS_OK) {
    status = wait_for(&w, timeout_ms + WAIT_MS);
  }
  FelixEvent *event = status == FELIX_STATUS_OK ? w.handle : NULL;
  waiter_destroy(&w);
  if (event) {
    const uint8_t *data = NULL;
    size_t len = 0;
    felix_event_payload(event, &data, &len);
    copy_bounded(payload, cap, data, len);
    *has_offset = false;
    felix_event_offset(event, offset, has_offset);
    felix_event_free(event);
  }
  return status;
}

/* How many acknowledged payloads `stream` does not hold, read on a connection
   the fault never touched. */
static size_t unread(const struct fixture *f, const char *stream,
                     const struct acked *acked) {
  bool found[MAX_ACKED] = {false};
  size_t missing = acked->count;
  FelixClient *client = NULL;
  if (connect_sync(f->addrs, f->tenant, f->token, f->ca_file, &client) !=
      FELIX_STATUS_OK) {
    return missing;
  }
  struct fixture q = *f;
  q.stream = stream;
  FelixSubscription *sub = NULL;
  if (subscribe(client, &q, FELIX_START_EARLIEST, 0, &sub) == FELIX_STATUS_OK) {
    struct timespec began;
    clock_gettime(CLOCK_MONOTONIC, &began);
    while (missing > 0 && seconds_since(&began) < SETTLE_MS / 1000.0) {
      char payload[PAYLOAD_CAP];
      bool has_offset = false;
      uint64_t offset = 0;
      FelixStatus status = read_event(sub, 1000, payload, sizeof payload,
                                      &has_offset, &offset);
      if (status == FELIX_STATUS_TIMEOUT) {
        continue;
      }
      if (status != FELIX_STATUS_OK) {
        break;
      }
      for (size_t i = 0; i < acked->count; i++) {
        if (!found[i] && strcmp(payload, acked->payload[i]) == 0) {
          found[i] = true;
          missing--;
        }
      }
    }
    felix_subscription_free(sub);
  }
  felix_client_free(client);
  return missing;
}

static void publish_through_fault(const struct fixture *f,
                                  const struct step *step, const char *stamp) {
  FelixClient *linked = NULL;
  FelixStatus status =
      connect_sync(f->link_addr, f->tenant, f->token, f->ca_file, &linked);
  CHECK(status == FELIX_STATUS_OK, "connect through the link gave %d",
        (int)status);
  if (!linked) {
    return;
  }
  struct acked acked;
  memset(&acked, 0, sizeof acked);
  CHECK(step->records < MAX_ACKED, "%d records is more than this test tracks",
        step->records);
  struct timespec began;
  clock_gettime(CLOCK_MONOTONIC, &began);
  double healed_at = 0;
  bool posted = false;
  bool stall = strcmp(step->fault, "stall") == 0;
  for (int i = 0; i < step->records && i < MAX_ACKED - 1; i++) {
    if (i == step->after_records) {
      posted = inject_fault(f, step);
      healed_at = seconds_since(&began) + step->hold_ms / 1000.0;
    }
    char payload[PAYLOAD_CAP], message[256] = "";
    snprintf(payload, sizeof payload, "%s-%d", stamp, i);
    status = publish(linked, f, f->link_stream, payload, NULL, message,
                     sizeof message);
    if (status == FELIX_STATUS_OK) {
      acked_add(&acked, payload);
    } else {
      CHECK(!stall, "publish %d failed during a stall: %d (%s)", i, (int)status,
            message);
    }
  }

  /* Once the fault is over, the client has to publish again. */
  double wait = healed_at - seconds_since(&began);
  if (healed_at > 0 && wait > 0) {
    pause_ms((long)(wait * 1000));
  }
  char payload[PAYLOAD_CAP];
  snprintf(payload, sizeof payload, "%s-after", stamp);
  struct timespec settling;
  clock_gettime(CLOCK_MONOTONIC, &settling);
  bool recovered = false;
  while (!recovered && seconds_since(&settling) < SETTLE_MS / 1000.0) {
    if (publish(linked, f, f->link_stream, payload, NULL, NULL, 0) ==
        FELIX_STATUS_OK) {
      acked_add(&acked, payload);
      recovered = true;
    } else {
      pause_ms(POLL_MS);
    }
  }
  CHECK(posted, "the fault was never injected");
  CHECK(recovered, "the client never published again after the fault");
  felix_client_free(linked);

  size_t missing = unread(f, f->link_stream, &acked);
  CHECK(missing == 0, "%zu of %zu acknowledged records were not in the stream",
        missing, acked.count);
}

static void subscribe_through_fault(const struct fixture *f,
                                    const struct step *step,
                                    const char *stamp) {
  FelixClient *linked = NULL, *direct = NULL;
  FelixStatus status =
      connect_sync(f->link_addr, f->tenant, f->token, f->ca_file, &linked);
  CHECK(status == FELIX_STATUS_OK, "connect through the link gave %d",
        (int)status);
  status = connect_sync(f->addrs, f->tenant, f->token, f->ca_file, &direct);
  CHECK(status == FELIX_STATUS_OK, "direct connect gave %d", (int)status);
  CHECK(step->records <= MAX_ACKED, "%d records is more than this test tracks",
        step->records);
  FelixSubscription *sub = NULL;
  if (linked && direct && step->records <= MAX_ACKED) {
    struct fixture q = *f;
    q.stream = f->link_stream;
    status = subscribe(linked, &q, FELIX_START_LATEST, 0, &sub);
    CHECK(status == FELIX_STATUS_OK, "subscribe through the link gave %d",
          (int)status);
  }
  if (sub) {
    char payloads[MAX_ACKED][PAYLOAD_CAP];
    int total = step->records;
    for (int i = 0; i < total; i++) {
      snprintf(payloads[i], sizeof payloads[i], "%s-%d", stamp, i);
    }
    for (int i = 0; i < step->after_records; i++) {
      status = publish(direct, f, f->link_stream, payloads[i], NULL, NULL, 0);
      CHECK(status == FELIX_STATUS_OK, "publish %d gave %d", i, (int)status);
    }
    bool stall = strcmp(step->fault, "stall") == 0;
    bool injected = false, posted = false, has_previous = false;
    uint64_t previous = 0;
    int received = 0;
    struct timespec began;
    clock_gettime(CLOCK_MONOTONIC, &began);
    double deadline = SETTLE_MS / 1000.0;
    while (received < total) {
      if (!injected && received == step->after_records) {
        posted = inject_fault(f, step);
        injected = true;
        deadline = seconds_since(&began) + step->hold_ms / 1000.0 +
                   SETTLE_MS / 1000.0;
        for (int i = step->after_records; i < total; i++) {
          status = publish(direct, f, f->link_stream, payloads[i], NULL, NULL, 0);
          CHECK(status == FELIX_STATUS_OK, "publish %d gave %d", i, (int)status);
        }
      }
      if (seconds_since(&began) >= deadline) {
        CHECK(0, "neither resumed nor reported the loss: %d of %d records, "
                 "then nothing",
              received, total);
        break;
      }
      char got[PAYLOAD_CAP];
      bool has_offset = false;
      uint64_t offset = 0;
      status = read_event(sub, POLL_MS, got, sizeof got, &has_offset, &offset);
      if (status == FELIX_STATUS_OK) {
        if (strncmp(got, stamp, strlen(stamp)) != 0) {
          /* Not ours: another run's record on the shared stream. */
          has_previous = false;
          continue;
        }
        if (strcmp(got, payloads[received]) != 0) {
          CHECK(0, "record %d was %s: a gap, a duplicate or a reordering",
                received, got);
          break;
        }
        CHECK(has_offset, "a durable stream delivered an event without an offset");
        CHECK(!has_previous || !has_offset || offset == previous + 1,
              "offsets jumped: %" PRIu64 " then %" PRIu64, previous, offset);
        has_previous = has_offset;
        previous = offset;
        received++;
      } else if (status == FELIX_STATUS_TIMEOUT) {
        continue;
      } else if (status == FELIX_STATUS_CLOSED) {
        CHECK(0, "the subscription ended cleanly after %d of %d records; a "
                 "dead connection must be resumed or reported, not read as "
                 "the end of the stream",
              received, total);
        break;
      } else {
        /* An error is the other right answer to a dead connection, but only
           once one was caused: and never during a stall. */
        CHECK(injected && !stall,
              "the subscription failed with no loss to report: %d",
              (int)status);
        break;
      }
    }
    CHECK(posted, "the fault was never injected");
  }
  felix_subscription_free(sub);
  felix_client_free(direct);
  felix_client_free(linked);
}

static void cache_through_fault(const struct fixture *f,
                                const struct step *step, const char *stamp) {
  FelixClient *linked = NULL;
  FelixStatus status =
      connect_sync(f->link_addr, f->tenant, f->token, f->ca_file, &linked);
  CHECK(status == FELIX_STATUS_OK, "connect through the link gave %d",
        (int)status);
  CHECK(step->records < MAX_ACKED, "%d records is more than this test tracks",
        step->records);
  if (!linked || step->records >= MAX_ACKED) {
    felix_client_free(linked);
    return;
  }
  struct cctx c = {linked, f, f->cache};
  struct acked acked;
  memset(&acked, 0, sizeof acked);
  char counter[PAYLOAD_CAP];
  snprintf(counter, sizeof counter, "%s-count", stamp);
  int added = 0;
  bool posted = false;
  for (int i = 0; i < step->records; i++) {
    if (i == step->after_records) {
      posted = inject_fault(f, step);
    }
    char key[PAYLOAD_CAP];
    snprintf(key, sizeof key, "%s-%d", stamp, i);
    if (c_put(&c, key, key, 0) == FELIX_STATUS_OK) {
      acked_add(&acked, key);
    }
    struct cres r;
    if (c_add(&c, counter, 1, &r) == FELIX_STATUS_OK) {
      added++;
    }
  }

  char after[PAYLOAD_CAP];
  snprintf(after, sizeof after, "%s-after", stamp);
  struct timespec settling;
  clock_gettime(CLOCK_MONOTONIC, &settling);
  bool recovered = false;
  while (!recovered && seconds_since(&settling) < SETTLE_MS / 1000.0) {
    if (c_put(&c, after, after, 0) == FELIX_STATUS_OK) {
      acked_add(&acked, after);
      recovered = true;
    } else {
      pause_ms(POLL_MS);
    }
  }
  CHECK(posted, "the fault was never injected");
  CHECK(recovered, "the client never wrote to the cache again after the fault");

  for (size_t i = 0; i < acked.count; i++) {
    struct cres r;
    status = c_get(&c, acked.payload[i], false, &r);
    CHECK(status == FELIX_STATUS_OK && r.found &&
              strcmp(r.value, acked.payload[i]) == 0,
          "the acknowledged put of %s did not read back (%d)",
          acked.payload[i], (int)status);
  }
  struct cres r;
  status = c_counter(&c, counter, &r);
  CHECK(status == FELIX_STATUS_OK, "reading the counter gave %d", (int)status);
  int64_t count = r.has_counter ? r.counter : 0;
  CHECK(count >= added && count <= step->records,
        "the counter is %" PRId64 " after %d acknowledged adds of %d", count,
        added, step->records);
  felix_client_free(linked);
}

static void link_faults(const struct fixture *f, const char *run) {
  static const char *const ids[] = {
      "fault.publish_through_a_dropped_link",
      "fault.publish_through_a_reset_link",
      "fault.publish_through_a_stalled_link",
      "fault.subscription_through_a_dropped_link",
      "fault.subscription_through_a_reset_link",
      "fault.subscription_through_a_stalled_link",
      "fault.cache_through_a_reset_link"};
  for (size_t i = 0; i < sizeof ids / sizeof *ids; i++) {
    const char *id = ids[i];
    if (!f->link_addr[0] || !f->link_stream[0] || !f->control_url[0]) {
      SKIP(id, "the fixture file has no link interposer");
      continue;
    }
    SCENARIO_BEGIN(id);
    struct step step;
    memset(&step, 0, sizeof step);
    if (!load_step(f, id, &step)) {
      CHECK(0, "the fixture file has no usable step for %s", id);
      SCENARIO_END(id);
      continue;
    }
    char stamp[160];
    snprintf(stamp, sizeof stamp, "c-%s-%s", run, id + strlen("fault."));
    /* A call may take a hold plus the idle timeout to give up. */
    wait_floor_ms = SETTLE_MS + 15000;
    if (strcmp(step.during, "publish") == 0) {
      publish_through_fault(f, &step, stamp);
    } else if (strcmp(step.during, "cache") == 0) {
      cache_through_fault(f, &step, stamp);
    } else {
      subscribe_through_fault(f, &step, stamp);
    }
    wait_floor_ms = 0;
    /* A client that resumed elsewhere can finish inside a drop's hold, and
       the next scenario would connect into it. */
    char body[2048];
    int code = control_post(f, "/heal", body, sizeof body);
    CHECK(code == 200, "heal answered %d: %s", code, body);
    SCENARIO_END(id);
  }
}

/* ---- losing brokers, on a cluster of the scenario's own ----------------- */

/* Kills the broker listening on `addr`, and only that process. */
static bool kill_broker(const struct fixture *f, const char *addr) {
  if (!f->kill_helper[0]) {
    return false;
  }
  char command[2 * MAX_LINE + 64];
  int n = snprintf(command, sizeof command, "'%s' '%s' >/dev/null 2>&1",
                   f->kill_helper, addr);
  return n > 0 && (size_t)n < sizeof command && system(command) == 0;
}

static void skip_unkillable(const char *id, const char *addr) {
  char reason[256];
  snprintf(reason, sizeof reason,
           "could not identify the broker process listening on %.100s", addr);
  SCENARIO_SKIP(id, reason);
}

/* Kill the broker the client was configured with; the next call must still
   succeed once the cluster has given its shards a new leader. */
static void broker_loss(const struct fixture *f, const char *run) {
  const char *id = "reconnect.survives_broker_loss";
  if (addr_count(f->addrs) < 2) {
    SKIP(id, "the disposable fixture has one broker");
    return;
  }
  SCENARIO_BEGIN(id);
  FelixClient *client = NULL;
  FelixStatus status =
      connect_sync(f->addrs, f->tenant, f->token, f->ca_file, &client);
  CHECK(status == FELIX_STATUS_OK, "connect gave %d", (int)status);
  if (client) {
    char seed[128], payload[PAYLOAD_CAP];
    CHECK(addr_at(f->addrs, 0, seed, sizeof seed), "no first address in %s",
          f->addrs);
    snprintf(payload, sizeof payload, "c-%s-before-loss", run);
    status = publish(client, f, f->stream, payload, NULL, NULL, 0);
    CHECK(status == FELIX_STATUS_OK, "publish before the loss gave %d",
          (int)status);
    if (!kill_broker(f, seed)) {
      skip_unkillable(id, seed);
      felix_client_free(client);
      return;
    }
    /* Generous: the client has to notice the broker is gone, and the cluster
       has to give its shards a new leader. Only the first is the client's. */
    snprintf(payload, sizeof payload, "c-%s-after-loss", run);
    bool recovered = false;
    char message[256] = "";
    for (int attempt = 0; attempt < 60 && !recovered; attempt++) {
      status = publish(client, f, f->stream, payload, NULL, message,
                       sizeof message);
      if (status == FELIX_STATUS_OK) {
        recovered = true;
      } else {
        pause_ms(1000);
      }
    }
    CHECK(recovered, "the client never recovered from losing %s: %d (%s)",
          seed, (int)status, message);
    felix_client_free(client);
  }
  SCENARIO_END(id);
}

/* A sharded subscription is told when a shard's broker dies. */
static void lost_shard(const struct fixture *f, const char *run) {
  const char *id = "sharded.a_lost_shard_is_surfaced";
  size_t brokers = addr_count(f->addrs);
  if (brokers < 2) {
    SKIP(id, "the disposable fixture has one broker");
    return;
  }
  SCENARIO_BEGIN(id);
  FelixClient *client = NULL;
  FelixStatus status =
      connect_sync(f->addrs, f->tenant, f->token, f->ca_file, &client);
  CHECK(status == FELIX_STATUS_OK, "connect gave %d", (int)status);
  FelixShardedSubscription *sub = NULL;
  uint32_t width = 0;
  if (client) {
    struct waiter w;
    waiter_init(&w);
    status = felix_client_subscribe_sharded(client, f->tenant, f->ns, f->stream,
                                            FELIX_START_LATEST, 0, NULL,
                                            on_sharded_subscribe, &w);
    if (status == FELIX_STATUS_OK) {
      status = wait_for(&w, WAIT_MS);
      sub = w.handle;
    }
    waiter_destroy(&w);
    CHECK(status == FELIX_STATUS_OK, "sharded subscribe gave %d", (int)status);
  }
  if (sub) {
    CHECK(felix_sharded_shards(sub, &width) == FELIX_STATUS_OK && width > 1,
          "the subscription covers %u shards", (unsigned)width);
    /* Every feed is live before anything is killed. */
    enum { KEYED = 8 };
    char prefix[64];
    snprintf(prefix, sizeof prefix, "c-%s-lost-", run);
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
    bool live = read_prefixed(sub, prefix, KEYED, NULL, &forbidden, &seen);

    /* One broker at a time, never the last: whichever owns a shard ends that
       shard's feed, and the first kill almost always does. */
    bool lost = false;
    for (size_t k = 0; live && k + 1 < brokers && !lost; k++) {
      char addr[128] = "";
      if (!addr_at(f->addrs, k, addr, sizeof addr) || !kill_broker(f, addr)) {
        skip_unkillable(id, addr);
        felix_sharded_subscription_free(sub);
        felix_client_free(client);
        return;
      }
      struct timespec began;
      clock_gettime(CLOCK_MONOTONIC, &began);
      while (!lost && seconds_since(&began) < 30.0) {
        struct item_waiter it;
        status = next_item(sub, 1000, &it);
        if (status == FELIX_STATUS_TIMEOUT) {
          continue;
        }
        if (status != FELIX_STATUS_OK) {
          CHECK(0, "a read after the kill gave %d (%s)", (int)status,
                it.w.message);
          break;
        }
        if (it.kind == FELIX_SHARD_LOST) {
          CHECK(it.shard < width, "shard %u is not one of %u", (unsigned)it.shard,
                (unsigned)width);
          CHECK(it.w.message[0] != '\0', "a lost shard carried no message");
          lost = true;
        }
      }
    }
    CHECK(!live || lost,
          "no shard was reported lost after a broker was killed");
  }
  felix_sharded_subscription_free(sub);
  felix_client_free(client);
  SCENARIO_END(id);
}

int main(int argc, char **argv) {
  if (argc == 2 && strcmp(argv[1], "offline") == 0) {
    offline();
  } else if (argc == 3 && strcmp(argv[1], "fixture") == 0) {
    struct fixture f;
    memset(&f, 0, sizeof f);
    if (!load_fixture(argv[2], &f)) {
      return 2;
    }
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
    connect_options(&f, run);
    token_provider(&f, run);
    sharded_groups(&f, run);
    caches(&f, run);
    commits(&f, run);
    commits_negotiated(&f, run);
    producers(&f, run);
    concurrency(&f, run);
    delivery(&f, run);
    redirects(&f, run);
    discovery(&f);
    link_faults(&f, run);
    /* Last: they break the cluster, then heal and wait for it. */
    faults(&f, run);
  } else if (argc == 4 && strcmp(argv[1], "disposable") == 0) {
    /* The scenarios that kill a broker, on a cluster run.sh started for this
       one scenario alone. */
    struct fixture f;
    memset(&f, 0, sizeof f);
    if (!load_fixture(argv[2], &f)) {
      return 2;
    }
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
    char run[32];
    snprintf(run, sizeof run, "%ld-%09ld", (long)ts.tv_sec, (long)ts.tv_nsec);
    if (strcmp(argv[3], "broker_loss") == 0) {
      broker_loss(&f, run);
    } else if (strcmp(argv[3], "lost_shard") == 0) {
      lost_shard(&f, run);
    } else {
      fprintf(stderr, "unknown disposable scenario %s\n", argv[3]);
      return 2;
    }
  } else {
    fprintf(stderr, "usage: %s offline | fixture FILE | disposable FILE SCENARIO\n", argv[0]);
    return 2;
  }
  ledger_close_all();
  if (failures) {
    fprintf(stderr, "%d check(s) failed\n", failures);
    return 1;
  }
  printf("ok\n");
  return 0;
}
