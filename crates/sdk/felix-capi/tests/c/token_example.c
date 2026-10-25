/* Compile-only: the example in docs-site/src/content/docs/clients/c.md,
   "Getting and refreshing a token". Keep the two in step; run.sh builds this
   file when libcurl's header is installed. */
#include <curl/curl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "felix.h"

/* EXAMPLE BEGIN */
struct session {
  pthread_mutex_t mu;
  char refresh_token[256]; /* the newest one; guarded by mu */
};

struct body {
  char data[4096];
  size_t len;
};

static size_t collect(char *chunk, size_t size, size_t n, void *out) {
  struct body *body = out;
  size_t bytes = size * n;
  if (body->len + bytes >= sizeof body->data) return 0; /* curl fails the call */
  memcpy(body->data + body->len, chunk, bytes);
  body->len += bytes;
  body->data[body->len] = '\0';
  return bytes;
}

/* Copies a string value out of a flat JSON object. Use a JSON parser in real code. */
static bool json_string(const char *json, const char *key, char *out, size_t cap) {
  char needle[64];
  snprintf(needle, sizeof needle, "\"%s\":\"", key);
  const char *start = strstr(json, needle);
  if (!start) return false;
  start += strlen(needle);
  const char *end = strchr(start, '"');
  if (!end || (size_t)(end - start) >= cap) return false;
  memcpy(out, start, (size_t)(end - start));
  out[end - start] = '\0';
  return true;
}

/* Spends the refresh token for a new access token and stores the
   replacement, because every refresh token works once. */
static bool refresh(struct session *s, char *token, size_t cap, char *error,
                    size_t error_cap) {
  char request[400];
  pthread_mutex_lock(&s->mu);
  snprintf(request, sizeof request, "{\"refresh_token\":\"%s\"}", s->refresh_token);
  pthread_mutex_unlock(&s->mu);

  struct body body = {.len = 0};
  CURL *curl = curl_easy_init();
  if (!curl) {
    snprintf(error, error_cap, "could not start curl");
    return false;
  }
  struct curl_slist *headers = curl_slist_append(NULL, "Content-Type: application/json");
  curl_easy_setopt(curl, CURLOPT_URL, "https://controlplane:8443/v1/tenants/t1/token/refresh");
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, collect);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 20L); /* the library gives up at 30 s */
  CURLcode rc = curl_easy_perform(curl);
  long status = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  if (rc != CURLE_OK) {
    snprintf(error, error_cap, "control plane unreachable: %s", curl_easy_strerror(rc));
    return false;
  }
  if (status != 200) {
    snprintf(error, error_cap, "token refresh answered %ld", status); /* 403: sign in again */
    return false;
  }
  char next[sizeof s->refresh_token];
  if (!json_string(body.data, "felix_token", token, cap) ||
      !json_string(body.data, "refresh_token", next, sizeof next)) {
    snprintf(error, error_cap, "token refresh answered something unexpected");
    return false;
  }
  pthread_mutex_lock(&s->mu);
  memcpy(s->refresh_token, next, sizeof next);
  pthread_mutex_unlock(&s->mu);
  return true;
}

struct fetch_job {
  struct session *session;
  FelixTokenReply *reply;
};

/* This thread outlives felix_client_free, and that is safe: the library keeps
 * the session until every reply has been answered, then calls release_session.
 * So use the session only before answering, and answer on every path. */
static void *answer(void *arg) {
  struct fetch_job *job = arg;
  char token[2048], error[160];
  if (refresh(job->session, token, sizeof token, error, sizeof error)) {
    felix_token_reply_ok(job->reply, token); /* the string is copied */
  } else {
    felix_token_reply_err(job->reply, error); /* reaches your caller unchanged */
  }
  free(job); /* the reply is consumed: do not touch it again */
  return NULL;
}

/* Called on a library thread: start the work and return. */
static void fetch_token(void *user_data, FelixTokenReply *reply) {
  struct fetch_job *job = malloc(sizeof *job);
  pthread_t thread;
  if (!job) {
    felix_token_reply_err(reply, "out of memory");
    return;
  }
  job->session = user_data;
  job->reply = reply;
  if (pthread_create(&thread, NULL, answer, job) != 0) {
    free(job);
    felix_token_reply_err(reply, "could not start a thread");
    return;
  }
  pthread_detach(thread);
}

static void release_session(void *user_data) {
  struct session *s = user_data;
  pthread_mutex_destroy(&s->mu);
  free(s);
}

/* Connects with no token in hand: the first connect calls fetch_token. */
static FelixStatus connect_with_refresh(const char *addrs, const char *ca_file,
                                        const char *refresh_token,
                                        FelixConnectCallback on_connect,
                                        void *connect_data) {
  struct session *s = calloc(1, sizeof *s);
  if (!s) return FELIX_STATUS_INVALID_ARGUMENT;
  pthread_mutex_init(&s->mu, NULL);
  snprintf(s->refresh_token, sizeof s->refresh_token, "%s", refresh_token);

  FelixConnectOptions options = {.size = sizeof options,
                                 .token_fetch = fetch_token,
                                 .token_user_data = s,
                                 .token_release = release_session};
  FelixStatus status = felix_client_connect_with(
      addrs, "t1", NULL /* first fetch gets it */, NULL, ca_file, &options,
      NULL, on_connect, connect_data);
  if (status != FELIX_STATUS_OK) release_session(s); /* refused: still ours */
  return status;
}
/* EXAMPLE END */

static void on_connect(void *ud, FelixStatus s, const char *msg, FelixClient *c) {
  (void)ud; (void)s; (void)msg; (void)c;
}

int main(void) {
  return connect_with_refresh("127.0.0.1:5000", NULL, "refresh", on_connect, NULL) ==
         FELIX_STATUS_OK ? 0 : 1;
}
