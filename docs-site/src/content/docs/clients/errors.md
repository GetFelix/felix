---
title: "Errors Across Clients"
description: "Every broker error code and retry class, and the exception or status each Felix client turns it into: Rust, Python, TypeScript, C# and C."
---

A broker that refuses a request sends two things: a **code** that says what went wrong, and a **retry class** that says what the caller may do about it. This page shows what each client gives you for each of them, so you can write the same error handling in any language.

The [Error Codes Reference](/reference/error-codes/) explains each code, its cause and the detail reasons. This page only maps them to client types.

## Act on the retry class first

Branch on the class, not the code. A client that does not know a newer code still knows the class.

| Retry class | What it means | Rust | Python | TypeScript | C# | C |
| --- | --- | --- | --- | --- | --- | --- |
| `retry` | Nothing was applied. Send it again. | `BrokerError.retry == RetryClass::Retry` | `err.retry == "retry"` | `err.retry === "retry"`, `err.retryable` | `IsRetryable` | n/a |
| `retry_after` | Nothing was applied. Wait, then send it again. | `RetryClass::RetryAfter`, and `detail.retry_after_ms` | `err.retry == "retry_after"`, `err.detail` | `err.retry === "retry_after"`, `err.detail` | `IsRetryable` | n/a |
| `redirect` | Nothing was applied. Another broker owns the shard. | `NotLeaderError`, or `RetryClass::Redirect` | `ShardUnavailableError` | `ShardUnavailableError` | `FelixStatus.ShardUnavailable` | `FELIX_STATUS_SHARD_UNAVAILABLE` |
| `outcome_unknown` | It may have been applied. Resend only an idempotent request. | `outcome_unknown(&err)`, `AckLost` | `OutcomeUnknownError` | `OutcomeUnknownError` | `OutcomeUnknown` property | `FELIX_STATUS_OUTCOME_UNKNOWN` |
| `fatal` | Sending it again fails the same way. | `RetryClass::Fatal` | `err.retry == "fatal"` | `err.retry === "fatal"` | other statuses | other statuses |

The clients retry `retry`, `retry_after` and `redirect` themselves before an error reaches you. What you see is what was left after that. See [Building Resilient Clients](/clients/resilient-clients/).

In C#, `IsRetryable` is true for `Connection`, `ShardUnavailable` and `Overloaded`. `NotFound` is not retryable there, even though the broker sends `not_found` with the `retry_after` class.

The C ABI reports one status per call and has no field for the broker's class. It folds the class into the status, as the table shows.

## Error codes

The Python and TypeScript exception names are the same, so they share a column.

The class column is the usual one. When the broker sends the class `outcome_unknown`, Python, TypeScript, C# and C report that, whatever the code.

| Code | Usual class | Rust | Python and TypeScript | C# `FelixStatus` | C status |
| --- | --- | --- | --- | --- | --- |
| `unauthenticated` | `fatal` | `BrokerError`; `AuthError` when the handshake or the token source fails | `AuthError` | `Auth` | `FELIX_STATUS_AUTH` |
| `forbidden` | `fatal` | `BrokerError` | `AuthError` | `Auth` | `FELIX_STATUS_AUTH` |
| `revoked` | `fatal` | `BrokerError` | `AuthError` | `Auth` | `FELIX_STATUS_AUTH` |
| `not_found` | `retry_after` | `BrokerError` | `NotFoundError` | `NotFound` | `FELIX_STATUS_NOT_FOUND` |
| `invalid_request` | `fatal` | `BrokerError` | `FelixError` | `Error` | `FELIX_STATUS_ERROR` |
| `shard_unavailable` | `retry` | `BrokerError` | `ShardUnavailableError` | `ShardUnavailable` | `FELIX_STATUS_SHARD_UNAVAILABLE` |
| `not_leader` | `redirect` | `NotLeaderError` (a `Client` follows it) or `BrokerError` | `ShardUnavailableError` | `ShardUnavailable` | `FELIX_STATUS_SHARD_UNAVAILABLE` |
| `quorum_timeout` | `outcome_unknown` | `BrokerError` | `OutcomeUnknownError` | `OutcomeUnknown` | `FELIX_STATUS_OUTCOME_UNKNOWN` |
| `leadership_lost` | `outcome_unknown` | `BrokerError` | `OutcomeUnknownError` | `OutcomeUnknown` | `FELIX_STATUS_OUTCOME_UNKNOWN` |
| `unacknowledged` | `outcome_unknown` | `BrokerError` | `OutcomeUnknownError` | `OutcomeUnknown` | `FELIX_STATUS_OUTCOME_UNKNOWN` |
| `overloaded` | `retry_after` | `BrokerError` | `OverloadedError` | `Overloaded` | `FELIX_STATUS_OVERLOADED` |
| `limit_exceeded` | `fatal` | `BrokerError` | `FelixError` | `Error` | `FELIX_STATUS_ERROR` |
| `draining` | `retry` | `BrokerError` | `ConnectionError` | `Connection` | `FELIX_STATUS_CONNECTION` |
| `internal` | `outcome_unknown` | `BrokerError` | `OutcomeUnknownError`, or `FelixError` when the broker sends another class | `OutcomeUnknown`, or `Error` | `FELIX_STATUS_OUTCOME_UNKNOWN`, or `FELIX_STATUS_ERROR` |
| `storage` | `outcome_unknown` | `BrokerError` | `OutcomeUnknownError`, or `FelixError` when the broker sends another class | `OutcomeUnknown`, or `Error` | `FELIX_STATUS_OUTCOME_UNKNOWN`, or `FELIX_STATUS_ERROR` |
| `stale_claim` | `retry` | `BrokerError`; the record is delivered again | `FelixError` | `Error` | `FELIX_STATUS_ERROR` |
| `stale_width` | `retry` | `BrokerError`; a `Client` re-asks the width itself | `FelixError` | `Error` | `FELIX_STATUS_ERROR` |
| `feature_not_enabled` | `fatal` | `BrokerError` | `FelixError` | `Error` | `FELIX_STATUS_ERROR` |
| `stream_has_commits` | `fatal` | `BrokerError` | `FelixError` | `Error` | `FELIX_STATUS_ERROR` |
| a code from a newer broker | the class sent | `BrokerError` with the unknown code | `FelixError` | `Error` | `FELIX_STATUS_ERROR` |

In Rust every row is a `BrokerError` with `code`, `retry` and `detail`. Recover it with `err.downcast_ref::<BrokerError>()`. In Python and TypeScript every exception carries `code`, `retry` and `detail`, so you can read the broker's code even when the class is generic. C# and C do not carry the code.

## Errors that do not come from a code

These come from the client, before or after the broker is involved.

| Situation | Rust | Python and TypeScript | C# | C |
| --- | --- | --- | --- | --- |
| The broker is unreachable or the connection dropped | `SubscriptionLost` on a subscription; a `Client` reconnects | `ConnectionError` | `Connection` | `FELIX_STATUS_CONNECTION` |
| The publish was sent and the acknowledgement was lost | `AckLost` | `OutcomeUnknownError` | `OutcomeUnknown` | `FELIX_STATUS_OUTCOME_UNKNOWN` |
| A start offset was trimmed or is past the tail | `SubscribeCursorError` | `CursorError` | `Cursor`, with `Cursor` details | `FELIX_STATUS_CURSOR` |
| An idempotent producer was refused | `PublishRefused` | `PublishRefusedError` with `reason` | n/a | n/a |
| A subscription fell behind and records were dropped | `SubscriptionLagged` | n/a, the client resubscribes | n/a | n/a |
| A commit was refused before it was sent | `CommitError` | `CommitError`, `NotOnOwningShardError`, `EventCountError` | `Unsupported` or `InvalidArgument` | same |
| A head trim was refused before it was sent | `TrimError` | `TrimError` | `Unsupported` | `FELIX_STATUS_UNSUPPORTED` |
| A bad argument | an `anyhow` error | `ValueError` in Python, `InvalidArgumentError` in TypeScript | `InvalidArgument` | `FELIX_STATUS_INVALID_ARGUMENT` |
| The call was cancelled | the future is dropped | n/a | `OperationCanceledException` | `FELIX_STATUS_CANCELLED` |

C# raises one exception class, `FelixException`, and you read `Status`. Its `IsRetryable` is true for `Connection`, `ShardUnavailable` and `Overloaded`, and `OutcomeUnknown` is true for `FelixStatus.OutcomeUnknown`. The full status list is under [Status codes](/clients/c/#status-codes).

## Where each client documents it

- [Rust](/clients/rust/publishing/#errors-you-can-act-on)
- [Python](/clients/python/#errors-you-can-act-on)
- [TypeScript](/clients/typescript/#errors-you-can-act-on)
- [C#](/clients/csharp/#errors)
- [C](/clients/c/#status-codes)
