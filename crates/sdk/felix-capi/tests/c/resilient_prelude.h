/* Prepended to each fragment on docs-site/src/content/docs/clients/resilient-clients.mdx
   that has no #include of its own: the headers the page says fragments leave
   out, the waiter and callbacks of the minimal example on the C page, and the
   application functions the examples call. */
#define _POSIX_C_SOURCE 200809L
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "felix.h"

struct waiter {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool done;
    FelixStatus status;
    void *handle;
    char message[256];
};

static void finish(struct waiter *w, FelixStatus status, const char *message, void *handle) {
    pthread_mutex_lock(&w->mu);
    w->done = true;
    w->status = status;
    w->handle = handle;
    snprintf(w->message, sizeof w->message, "%s", message ? message : "");
    pthread_cond_signal(&w->cv);
    pthread_mutex_unlock(&w->mu);
}

static FelixStatus wait_for(struct waiter *w, FelixStatus started) {
    if (started != FELIX_STATUS_OK) {
        snprintf(w->message, sizeof w->message, "%s", felix_last_error_message());
        return started;
    }
    pthread_mutex_lock(&w->mu);
    while (!w->done) pthread_cond_wait(&w->cv, &w->mu);
    w->done = false;
    FelixStatus status = w->status;
    pthread_mutex_unlock(&w->mu);
    return status;
}

static void on_connect(void *user_data, FelixStatus status, const char *message,
                       FelixClient *client) {
    finish(user_data, status, message, client);
}
static void on_subscribe(void *user_data, FelixStatus status, const char *message,
                         FelixSubscription *subscription) {
    finish(user_data, status, message, subscription);
}
static void on_publish(void *user_data, FelixStatus status, const char *message,
                       bool has_offset, uint64_t offset) {
    (void)has_offset;
    (void)offset;
    finish(user_data, status, message, NULL);
}
static void on_event(void *user_data, FelixStatus status, const char *message,
                     FelixEvent *event) {
    finish(user_data, status, message, event);
}
static void on_done(void *user_data, FelixStatus status, const char *message) {
    finish(user_data, status, message, NULL);
}

/* The application's own code. */
void handle_payload(const uint8_t *data, size_t len);
int save_checkpoint(uint64_t offset);
int charge(const uint8_t *payload, size_t len);
