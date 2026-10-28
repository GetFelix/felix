---
title: "Error Codes Reference"
description: "Every client-facing error code, retry class and detail reason a Felix broker sends, with the cause, what a client should do, and how each language client surfaces it."
---

A broker that refuses a request says why with a code and says what the client may do about it with a retry class. This page lists every code, retry class and reason, what usually causes it, what to do, and what you catch in each client. The frame layout is in the [wire protocol](/architecture/wire-protocol/#error) and the [protocol specification](https://github.com/GetFelix/felix/blob/main/docs/protocol.md#error-codes). A script checks this page against `crates/protocol/felix-wire/src/client/error_code.rs` and `message/fields.rs` (`scripts/check_error_codes_reference.py`, run by `task docs:evidence`), so a code, class or reason added there without a row here fails CI.

## How errors reach a client

Codes are sent only to a client that offered `FEATURE_ERROR_CODES` when it authenticated. Every official client does. Without the feature the broker sends the same failures as plain text and the client falls back to reading the message, which is why a missing code does not mean success.

The code and the retry class travel separately. A client that does not know a code still knows from the class whether sending again is safe, so an unknown code never fails the frame. Act on the class first and on the code second. A broker may send a class other than the default for a code when it knows better, and the class it sends is the one that counts.

An `error` can also be the last frame on a subscription's event stream or a cache watch, for example when the credential it was opened with is revoked. The broker writes the `error` before it ends the stream.

## Retry classes

| Class | Number | Meaning | What to do |
| --- | --- | --- | --- |
| `retry` | 1 | Nothing was applied. Sending the request again is safe. | Retry, ideally against a refreshed route. |
| `retry_after` | 2 | Nothing was applied. Wait before sending again. `detail.retry_after_ms` may say how long. | Back off for at least the hint, then retry. |
| `redirect` | 3 | Nothing was applied. Another broker owns the shard. | Send it to the owner. |
| `outcome_unknown` | 4 | The request may have been applied. | Resend only an idempotent request. Otherwise read back the state, or accept a possible duplicate. |
| `fatal` | 5 | Sending it again fails the same way. | Stop and fix the request or the credential. |

A class this version does not know is read as `fatal`, the one reading that cannot duplicate a write. The number is what a binary publish ack carries.

## Error codes

The class is the default the broker sends. `Number` is the `u16` a binary publish ack carries.

| Code | Number | Default class | Meaning | Typical cause | What the client should do |
| --- | --- | --- | --- | --- | --- |
| `unauthenticated` | 1 | `fatal` | The stream has not authenticated, or the credential was refused. | A request before `auth`, or a token that does not verify or has expired. | Get a new token. A client with a token provider does this once per refused stream. Do not retry with the same token. |
| `forbidden` | 2 | `fatal` | The credential does not grant this operation. | A missing permission, or a tenant other than the token's. | Fix the grant. Retrying cannot succeed. |
| `not_found` | 3 | `retry_after` | The tenant, namespace, stream, cache or group does not exist on this broker. | A typo, or a stream that was never created. On a cluster, a broker promoted a moment ago has not yet learned a stream it is about to serve. | Retry briefly, as the Rust cluster client does for 5 seconds. If it persists, create the resource or fix the name. |
| `invalid_request` | 4 | `fatal` | The request is malformed or can never succeed as sent. | A malformed frame or batch, unknown frame flags, a missing `request_id`, a second `auth`, a bad watch filter, a shard at or past the stream's shard count. | Fix the request. Nothing was written. |
| `shard_unavailable` | 5 | `retry` | Nobody can serve the shard right now. | A shard being moved, opened or failed over. `detail.reason` says which; see [shard unavailable reasons](#shard-unavailable-reasons). | Retry with backoff, honoring `retry_after_ms` when present. Nothing was applied. |
| `not_leader` | 6 | `redirect` | Another broker owns the shard. | The client went to a broker that does not lead the shard, and the dedicated `not_leader` message could not be sent: to a client without `FEATURE_REDIRECT`, or for a publish the broker cannot forward. | Go to the owner. The cluster client does this itself. |
| `quorum_timeout` | 7 | `outcome_unknown` | The leader wrote the batch and a majority did not confirm it in time. It may survive. | A write to a `Quorum` stream or cache while followers are slow or down. | Do not resend a plain publish. Use an idempotent producer, or reconcile by reading the stream. |
| `leadership_lost` | 8 | `outcome_unknown` | Leadership moved after the leader wrote the batch and before a majority held it. | A write to a `Quorum` stream or cache during a failover or move. | As for `quorum_timeout`. |
| `unacknowledged` | 9 | `outcome_unknown` | The broker stopped waiting for the write's outcome. | A forwarded batch whose answer never came, a commit that outlasted the ack wait, or a publish whose shard log was reset while it waited behind earlier publishes. | As for `quorum_timeout`. The last case reached no reader. |
| `overloaded` | 10 | `retry_after` | The broker is shedding load. | A full ingress queue or ack path, a full disk under durable storage, a full per-tenant publish queue, or a tenant over its quota. See [overloaded reasons](#overloaded-reasons). | Back off and retry. Sent as `outcome_unknown` when the batch was already queued before the broker ran out of room to track its ack. |
| `limit_exceeded` | 11 | `fatal` | The request exceeds a configured limit. | Too many subscriptions on one connection, or a payload over the size limit. | Reduce the request or raise the limit. It will keep failing. |
| `draining` | 12 | `retry` | The broker is shutting down and takes no new work. | A new control stream opened while the connection drains. | Connect to another broker. |
| `internal` | 13 | `outcome_unknown` | Something failed inside the broker. | Anything not covered by another code. Sent as `retry` for reads and failures before any write, and `fatal` for configuration the request cannot change. | Follow the class sent. Report a persistent one with the broker log. |
| `storage` | 14 | `outcome_unknown` | The storage layer failed. | A durable write, a group's state or a cache log could not be written or read. Sent as `retry` for reads. | Follow the class sent. Check the broker's disk and the `felix_storage_*` [metrics](/reference/metrics/#storage). |
| `stale_claim` | 15 | `retry` | The group has no record of handing this offset out. Nothing was applied, and the record will be delivered again. | A `group_ack` or `group_nack` after the group's in-memory state was rebuilt, by eviction while idle or a shard failover. Sent as `fatal` to a `group_extend`, or a `group_nack` naming `attempts`, whose claim no longer stands. | Drop the settlement and poll again. The record comes back. |
| `revoked` | 16 | `fatal` | The credential was revoked: its principal, or the actor of a delegated token. | Answer to `auth` with such a token, or the last frame of an open stream when the revocation reaches the broker. | Stop using the token. A token minted after the revocation may be accepted, so a client with a token provider can authenticate again. |
| `stale_width` | 17 | `retry` | The keyed batch was routed by a width the stream does not have on this broker. Nothing was written. | The stream was deleted and created again with a different shard count after the client last asked for its width. | Ask `stream_shards` again, then retry. Only a client that sent `publish_width` gets it. |

A code this client does not know is kept as an unknown code with its retry class. `0` is never sent as a number.

## Shard unavailable reasons

`detail.reason` on a `shard_unavailable` error. It is a string, so a reason added later reaches an older client as text and the error is handled like any other `shard_unavailable`.

| Reason | Meaning | What to do |
| --- | --- | --- |
| `not_assigned` | Placement has not assigned the shard. | Retry. If it persists, check that the control plane is placing shards. |
| `owner_unavailable` | The owner is known but unreachable or not live. | Retry. The shard is served again once the owner returns or placement moves it. |
| `not_ready` | The owner has not finished opening the shard. Also what a client sees when a forwarded write was refused because the owner's epoch was fenced, since the broker-to-broker answer does not say which. | Retry. |
| `stale` | This broker's routing view is behind the request's. | Retry. The view catches up from the control plane. |
| `fenced` | The owner's epoch was superseded, or its lease lapsed, so it no longer leads the shard. A write refused for this reason was not written. | Retry against the new owner. |
| `moving` | The shard is moving to another broker and the move had not cut over in time. | Retry after `detail.retry_after_ms`. |
| `region_not_routable` | The leader is in a region the broker has no bridge to, so it will not forward there. Connecting to the leader directly is not refused. | Connect to a broker in the leader's region, or configure the bridge. |

## Overloaded reasons

`detail.reason` on an `overloaded` error, when the broker has one. `detail.retry_after_ms` suggests how long to wait.

| Reason | Meaning |
| --- | --- |
| `publish_queue_full` | The broker's publish queue had no room for the publish's tenant. Nothing was queued. |
| `tenant_quota` | The publish's tenant is over its publish quota. Nothing was queued. `retry_after_ms` says when the tenant is back under. |

## Publish refusal reasons

A conditional or idempotent write that the broker will not append is answered with `publish_refused`, not an error code, because each reason names a different remedy. It answers a `publish_idempotent`, a `publish_if`, or a `commit` that carried `expected_offset`. In the Rust client it arrives as a `PublishRefused` error carrying the reason. The Python and TypeScript bindings have no idempotent or conditional publish, so they never see it. The C ABI reports a refused conditional write as `FELIX_STATUS_OK` with `written` false and the shard's tail.

| Reason | Carries | Meaning | What to do |
| --- | --- | --- | --- |
| `sequence_gap` | `expected` | The sequence is past the next one expected. What was skipped is lost to the broker. | Stop. Do not carry on as if nothing was skipped. |
| `unknown_producer` | none | The broker holds no sequence for this producer on this shard and the batch was not its first. | Start again with a new producer id. |
| `sequence_expired` | none | The sequence is older than the window the broker keeps, so whether it was appended cannot be told. | Treat the outcome as unknown and reconcile. |
| `sequence_reused` | none | The broker already holds a batch under this sequence with different payloads. Only sent to a client that offered `FEATURE_SEQUENCE_REUSED`. | The producer reused a number it had spent. Fix the producer. |
| `offset_mismatch` | `tail` | A `publish_if` or conditional `commit` expected a different next offset. Nothing was written and no offset consumed. | Read the shard, then try again from `tail` if you still own it. |
| `not_leader` | `node_id`, `addr` | This broker does not lead the shard, and only the leader holds the sequences. | Send the batch to the broker named. |

## Cursor error reasons

A subscribe that cannot start where it asked is answered with `subscribe_cursor_error`, also not an error code. It carries the offset requested and the nearest `available` one. The Rust client returns it as a `SubscribeCursorError` with both offsets. Python raises `CursorError`, TypeScript `CursorError` (kind `FELIX_CURSOR`) and C and C# report `FELIX_STATUS_CURSOR`; those three carry the class and not the offsets.

| Reason | Meaning | What to do |
| --- | --- | --- |
| `too_old` | Retention discarded the offset, or it fell out of an in-memory stream's replay ring. `available` is the oldest retained. | Decide whether to accept the gap and resume from `earliest` or `available`. |
| `in_future` | The offset is past the end of the stream. `available` is the current tail. | Usually a checkpoint written for a different stream. Fix the checkpoint. |

## How each client surfaces them

All clients pick the exception class from the code, except that an `outcome_unknown` class always wins. Whatever went wrong, "this may have been written" decides what the caller does next. Rows below show the default class; `internal` and `storage` become the outcome-unknown type when the broker sends them that way, as it does for a write.

| Code | Rust | Python | TypeScript | C and C# status |
| --- | --- | --- | --- | --- |
| `unauthenticated` | `BrokerError` | `AuthError` | `AuthError` | `FELIX_STATUS_AUTH` (`Auth`) |
| `forbidden` | `BrokerError` | `AuthError` | `AuthError` | `FELIX_STATUS_AUTH` (`Auth`) |
| `not_found` | `BrokerError` | `NotFoundError` | `NotFoundError` | `FELIX_STATUS_NOT_FOUND` (`NotFound`) |
| `invalid_request` | `BrokerError` | `FelixError` | `FelixError` | `FELIX_STATUS_ERROR` (`Error`) |
| `shard_unavailable` | `BrokerError` | `ShardUnavailableError` | `ShardUnavailableError` | `FELIX_STATUS_SHARD_UNAVAILABLE` (`ShardUnavailable`) |
| `not_leader` | `BrokerError`, or `NotLeaderError` from a redirect | `ShardUnavailableError` | `ShardUnavailableError` | `FELIX_STATUS_SHARD_UNAVAILABLE` (`ShardUnavailable`) |
| `quorum_timeout` | `BrokerError` | `OutcomeUnknownError` | `OutcomeUnknownError` | `FELIX_STATUS_OUTCOME_UNKNOWN` (`OutcomeUnknown`) |
| `leadership_lost` | `BrokerError` | `OutcomeUnknownError` | `OutcomeUnknownError` | `FELIX_STATUS_OUTCOME_UNKNOWN` (`OutcomeUnknown`) |
| `unacknowledged` | `BrokerError` | `OutcomeUnknownError` | `OutcomeUnknownError` | `FELIX_STATUS_OUTCOME_UNKNOWN` (`OutcomeUnknown`) |
| `overloaded` | `BrokerError` | `OverloadedError` | `OverloadedError` | `FELIX_STATUS_OVERLOADED` (`Overloaded`) |
| `limit_exceeded` | `BrokerError` | `FelixError` | `FelixError` | `FELIX_STATUS_ERROR` (`Error`) |
| `draining` | `BrokerError` | `ConnectionError` | `ConnectionError` | `FELIX_STATUS_CONNECTION` (`Connection`) |
| `internal` | `BrokerError` | `OutcomeUnknownError` | `OutcomeUnknownError` | `FELIX_STATUS_OUTCOME_UNKNOWN` (`OutcomeUnknown`) |
| `storage` | `BrokerError` | `OutcomeUnknownError` | `OutcomeUnknownError` | `FELIX_STATUS_OUTCOME_UNKNOWN` (`OutcomeUnknown`) |
| `stale_claim` | `BrokerError` | `FelixError` | `FelixError` | `FELIX_STATUS_ERROR` (`Error`) |
| `revoked` | `BrokerError` | `AuthError` | `AuthError` | `FELIX_STATUS_AUTH` (`Auth`) |
| `stale_width` | `BrokerError` | `FelixError` | `FelixError` | `FELIX_STATUS_ERROR` (`Error`) |

A few errors come from the client rather than the broker and have no code:

| Failure | Rust | Python | TypeScript | C and C# status |
| --- | --- | --- | --- | --- |
| Token source failed, or the broker refused the credential | `AuthError` | `AuthError` | `AuthError` | `FELIX_STATUS_AUTH` |
| Start offset gone or ahead of the tail | `SubscribeCursorError` | `CursorError` | `CursorError` | `FELIX_STATUS_CURSOR` |
| Subscription connection dropped | `SubscriptionLost` | `ConnectionError` | `ConnectionError` | `FELIX_STATUS_CONNECTION` |
| Subscription fell behind and records were dropped | `SubscriptionLagged` | `SubscriptionLagged` | Rust client only | Rust client only | Rust client only |
| Idempotent publish refused (a conditional publish returns its refusal as a result) | `PublishRefused` | `PublishRefusedError` with `reason` | `PublishRefusedError` with `reason` (`FELIX_PUBLISH_REFUSED` kind) | No typed case: the status is read from the refusal's message as for an error without a code, so it is `FELIX_STATUS_ERROR` unless the message names a class (for example "not leader" reads as `FELIX_STATUS_CONNECTION`). C# surfaces that status in `FelixException.Status` and has no refusal reason |
| Commit refused before sending | `CommitError` | `CommitError`, `NotOnOwningShardError`, `EventCountError` | `FELIX_COMMIT`, `FELIX_NOT_ON_OWNING_SHARD`, `FELIX_EVENT_COUNT` kinds | `FELIX_STATUS_UNSUPPORTED` or `FELIX_STATUS_INVALID_ARGUMENT` |
| Bad argument to the client | none | `ValueError` | `InvalidArgumentError` | `FELIX_STATUS_INVALID_ARGUMENT` |

### Rust

Calls return `anyhow::Result`. Recover a broker refusal with `err.downcast_ref::<BrokerError>()`, which carries `code`, `retry`, `detail` and `message`, and `reason()` for the detail reason. Branch on `retry`. `ClusterClient` acts on the class itself before an error reaches you: it returns `fatal` at once, never resends an `outcome_unknown` publish except through `publish_at_least_once` or an idempotent producer, and retries `retry`, `redirect` and `retry_after`. See [Errors you can act on](/clients/rust/#errors-you-can-act-on).

### Python

Every exception carries `code`, `retry` and `detail`, all `None` when the broker sent no code or the failure was local. `felix.ConnectionError` is Felix's own class, not the built-in. See [Errors you can act on](/clients/python/#errors-you-can-act-on).

### TypeScript

Every error carries `code`, `retry`, `detail` and a `retryable` flag that follows `retry` when the broker sent one. `err.kind` is the client's own name for the class (`FELIX_AUTH`, `FELIX_SHARD_UNAVAILABLE` and so on) and is set whether or not the broker sent a code. See [Errors you can act on](/clients/typescript/#errors-you-can-act-on).

### C and C#

The C ABI reports a status per call, not the broker's code, so several codes share a status. The statuses are in `crates/sdk/felix-capi/include/felix.h` and listed with their values under [Status codes](/clients/c/#status-codes). The C# client's `FelixException.Status` is the same value, `IsRetryable` is true for `Connection`, `ShardUnavailable` and `Overloaded`, and a cancelled call is an `OperationCanceledException`.

| Status | Value | Raised for |
| --- | --- | --- |
| `FELIX_STATUS_OK` | 0 | The call succeeded. |
| `FELIX_STATUS_TIMEOUT` | 1 | A bounded wait ended with nothing. The handle is still usable. |
| `FELIX_STATUS_CLOSED` | 2 | The broker ended the subscription, or the producer is closed. |
| `FELIX_STATUS_CANCELLED` | 3 | The call was cancelled. A publish cancelled after it may have been sent reports `FELIX_STATUS_OUTCOME_UNKNOWN` instead. |
| `FELIX_STATUS_INVALID_ARGUMENT` | 10 | A null pointer, non-UTF-8 string or out-of-range value. Nothing was sent. |
| `FELIX_STATUS_PANIC` | 11 | The library panicked. A bug worth reporting. |
| `FELIX_STATUS_ERROR` | 20 | A failure with no more specific class: `invalid_request`, `limit_exceeded`, `stale_claim`, `stale_width` and unknown codes. |
| `FELIX_STATUS_CONNECTION` | 21 | `draining`, an unreachable broker, or a lost connection. |
| `FELIX_STATUS_AUTH` | 22 | `unauthenticated`, `forbidden` and `revoked`. |
| `FELIX_STATUS_NOT_FOUND` | 23 | `not_found`. |
| `FELIX_STATUS_CURSOR` | 24 | A cursor error. |
| `FELIX_STATUS_SHARD_UNAVAILABLE` | 25 | `shard_unavailable` and `not_leader`. |
| `FELIX_STATUS_OVERLOADED` | 26 | `overloaded`. |
| `FELIX_STATUS_OUTCOME_UNKNOWN` | 27 | Any code sent with the `outcome_unknown` class. |
| `FELIX_STATUS_UNSUPPORTED` | 28 | The broker does not advertise atomic commits. |
