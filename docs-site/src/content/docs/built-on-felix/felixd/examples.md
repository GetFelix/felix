---
title: "felixd example workloads"
description: "Six complete felixd workloads that compile against contract 0.2: usage metering, an order lifecycle, idempotency keys, fraud scoring, a tumbling-window aggregate and a simulation. Each has its spec, its code, and how to submit it and read its outputs."
---

Each example below is a whole workload: the code, the spec it runs under, and the
commands to submit it and read what it produces. They are in the
`workloads/` directory of the [felixd repository](https://github.com/GetFelix/felixd)
and build against [contract 0.2](/built-on-felix/felixd/contract/). Read
[Submitting work](/built-on-felix/felixd/submitting-work/) first for what `felixd run`
does step by step.

| Example | What it shows |
|---|---|
| [Usage metering with a quota](#usage-metering-with-a-quota) | Keyed outputs, a second output under a condition, rejecting bad input |
| [An order lifecycle as event-sourced state](#an-order-lifecycle-as-event-sourced-state) | State machines, rejecting an input that is valid text but not allowed now |
| [Idempotency keys](#idempotency-keys) | Returning no outputs, bounded memory |
| [Scoring transactions](#scoring-transactions) | Using the input's append time instead of a clock |
| [A tumbling-window aggregate](#a-tumbling-window-aggregate) | Binary input, JSON output, a state that is a fixed record |
| [A deterministic simulation](#a-deterministic-simulation) | Time that moves only on input, integer arithmetic |

## What every example shares

A workload exports `step` and `state` and imports nothing. Every example below
has the same shape:

- `step` reads the input record and returns a list of outputs. Each output has a
  name and an optional key. The name is `out` or a name the workload chooses, in
  lowercase letters, digits and `-`.
- A bad input returns `Reject` with a code a program can compare and a detail for
  people. The record is skipped and the workload keeps running.
- An input that is not a record, which is an effect response, returns `Poison`.
  No version of felixd delivers one yet, so the branch is never taken, but a
  workload must handle it.
- The state is kept in a `thread_local!` and saved by `snapshot` and
  `snapshot_chunk`. `restore` and `restore_chunk` read it back on a fresh
  instance. A restore that gets bytes it does not understand panics, which fails
  the restore loudly instead of continuing with the wrong state.
- Nothing depends on the host: no clock, no randomness, no iteration order of a
  hash map. The examples use `BTreeMap` for that reason.

Build and submit any of them like the `counter` in
[Submitting work](/built-on-felix/felixd/submitting-work/#writing-a-workload). Replace
`counter` with the crate name:

```bash
(cd workloads && cargo build --release --target wasm32-unknown-unknown -p metering)
wasm-tools component new \
  workloads/target/wasm32-unknown-unknown/release/metering.wasm -o metering.wasm
```

The input stream must exist and be durable before `felixd run`, and
`--replication-factor 1` is for a single development broker:

```bash
felixctl stream create usage-events
felixd run --id metering --input usage-events --replication-factor 1 metering.wasm
```

Outputs are the records of `wl.<id>.out`. Each one carries a header with the
input offset, the output's name and its key, followed by the payload, so
`felixctl sub wl.metering.out` shows the payload text at the end of each record.
The layout is in [Record formats](/built-on-felix/felixd/records/#output-stream-version-4).
`felixd replay` and `felixd check` read a workload without a decoder.

## Usage metering with a quota

Keeps a running total of units used per customer. Each input is `<customer>,<units>`. The workload outputs the new total under `usage`, keyed by customer, and the first time a customer passes the quota of 1000 it also outputs the total under `quota-exceeded`. A line that does not parse is rejected and the workload carries on.

The key on each output is the customer, so a consumer that reads `wl.metering.out` can tell customers apart without parsing the payload.

### Spec

```bash
felixctl stream create usage-events
felixd run --id metering --input usage-events --replication-factor 1 metering.wasm
```

`felixd run` writes this to the `workloads` cache:

```json
{
  "module": "<sha256 of the component>",
  "input": "usage-events",
  "output": "wl.metering.out",
  "snapshot_every": 1000,
  "state": "running",
  "limits": {
    "fuel_per_call": 50000000,
    "max_memory_bytes": 67108864,
    "snapshot_fuel": 500000000
  }
}
```

### Code

`workloads/metering/src/lib.rs`:

```rust
//! A usage ledger. An input is `<customer>,<units>`. Each step outputs the
//! customer's new total under `usage`, keyed by customer, and the first time a
//! total passes `QUOTA` it also outputs under `quota-exceeded`.

use std::cell::RefCell;
use std::collections::BTreeMap;

wit_bindgen::generate!({
    path: "../../wit",
    world: "workload",
});

use exports::felixd::workload::state;
use exports::felixd::workload::step::{
    self, Addressed, ErrorCode, Input, InputBody, Output, StepError,
};

const QUOTA: u64 = 1_000;
const MALFORMED: u32 = 1;

thread_local! {
    static TOTALS: RefCell<BTreeMap<String, u64>> = const { RefCell::new(BTreeMap::new()) };
}

struct Metering;

fn record(name: &str, customer: &str, payload: String) -> Output {
    Output::Record(Addressed {
        name: name.to_string(),
        key: Some(customer.as_bytes().to_vec()),
        payload: payload.into_bytes(),
    })
}

fn parse(payload: &[u8]) -> Option<(&str, u64)> {
    let (customer, units) = std::str::from_utf8(payload).ok()?.split_once(',')?;
    let customer = customer.trim();
    if customer.is_empty() || customer.contains(char::is_whitespace) {
        return None;
    }
    Some((customer, units.trim().parse().ok()?))
}

impl step::Guest for Metering {
    fn step(input: Input) -> Result<Vec<Output>, StepError> {
        let InputBody::Record(rec) = input.body else {
            return Err(StepError::Poison("metering takes no effect responses".into()));
        };
        let Some((customer, units)) = parse(&rec.payload) else {
            return Err(StepError::Reject(ErrorCode {
                code: MALFORMED,
                detail: "an input is `<customer>,<units>`".to_string(),
            }));
        };
        TOTALS.with_borrow_mut(|totals| {
            let total = totals.entry(customer.to_string()).or_insert(0);
            let before = *total;
            *total = before.saturating_add(units);
            let mut outputs = vec![record("usage", customer, total.to_string())];
            if before <= QUOTA && *total > QUOTA {
                outputs.push(record("quota-exceeded", customer, total.to_string()));
            }
            Ok(outputs)
        })
    }
}

impl state::Guest for Metering {
    fn snapshot() -> u32 {
        1
    }

    fn snapshot_chunk(_index: u32) -> Vec<u8> {
        TOTALS.with_borrow(|totals| {
            totals.iter().map(|(c, n)| format!("{c}\t{n}\n")).collect::<String>().into_bytes()
        })
    }

    fn restore(chunks: u32) {
        assert_eq!(chunks, 1, "metering state is one chunk");
    }

    fn restore_chunk(_index: u32, chunk: Vec<u8>) {
        let text = String::from_utf8(chunk).expect("metering state is text");
        let totals = text
            .lines()
            .map(|line| {
                let (customer, total) = line.split_once('\t').expect("a customer and a total");
                (customer.to_string(), total.parse().expect("a total"))
            })
            .collect();
        TOTALS.set(totals);
    }

    fn inspect() -> String {
        TOTALS.with_borrow(|totals| {
            let items: Vec<String> = totals.iter().map(|(c, n)| format!("\"{c}\":{n}")).collect();
            format!("{{{}}}", items.join(","))
        })
    }
}

export!(Metering);
```

### Submit it and read the outputs

Build the component as shown above, run the command in the spec, then publish:

```bash
felixctl pub usage-events 'acme,600'
felixctl pub usage-events 'acme,500'
felixctl pub usage-events 'globex,5'
felixctl pub usage-events 'acme,1'
felixctl pub usage-events 'acme,x'
```

What the workload does with each input:

| Input | Outputs |
|---|---|
| `acme,600` | `usage` for `acme`: `600` |
| `acme,500` | `usage` for `acme`: `1100`, and `quota-exceeded` for `acme`: `1100` |
| `globex,5` | `usage` for `globex`: `5` |
| `acme,1` | `usage` for `acme`: `1101` |
| `acme,x` | a rejection with code 1. No output |

Read the state it holds without a decoder:

```bash
felixd replay --id metering --to 3     # {"acme":1101,"globex":5}
felixd check --complete
```

The totals saturate instead of wrapping, so an enormous input cannot make a customer's usage go down. The state is one text chunk with a line per customer.

## An order lifecycle as event-sourced state

Tracks each order through `created`, `paid`, `shipped` and `delivered`, with `cancel` allowed before shipping. An input is `<order> <verb>`. A valid transition outputs the new status under `order-state`, keyed by order. An invalid one, such as shipping an order that was not paid, is rejected with code 2 and changes nothing.

Because every state change is an input and every accepted one is an output, `felixd replay` shows what the system believed about any order at any past offset.

### Spec

```bash
felixctl stream create order-events
felixd run --id orders --input order-events --replication-factor 1 orders.wasm
```

`felixd run` writes this to the `workloads` cache:

```json
{
  "module": "<sha256 of the component>",
  "input": "order-events",
  "output": "wl.orders.out",
  "snapshot_every": 1000,
  "state": "running",
  "limits": {
    "fuel_per_call": 50000000,
    "max_memory_bytes": 67108864,
    "snapshot_fuel": 500000000
  }
}
```

### Code

`workloads/orders/src/lib.rs`:

```rust
//! Order lifecycle as event-sourced state. An input is `<order> <verb>` with a
//! verb of `create`, `pay`, `ship`, `deliver` or `cancel`. A valid transition
//! outputs the new status under `order-state`, keyed by order. An invalid one
//! is rejected with `INVALID_TRANSITION` and changes nothing.

use std::cell::RefCell;
use std::collections::BTreeMap;

wit_bindgen::generate!({
    path: "../../wit",
    world: "workload",
});

use exports::felixd::workload::state;
use exports::felixd::workload::step::{
    self, Addressed, ErrorCode, Input, InputBody, Output, StepError,
};

const MALFORMED: u32 = 1;
const INVALID_TRANSITION: u32 = 2;

#[derive(Clone, Copy, PartialEq)]
enum Status {
    Created,
    Paid,
    Shipped,
    Delivered,
    Cancelled,
}

impl Status {
    fn name(self) -> &'static str {
        match self {
            Status::Created => "created",
            Status::Paid => "paid",
            Status::Shipped => "shipped",
            Status::Delivered => "delivered",
            Status::Cancelled => "cancelled",
        }
    }

    fn from_name(name: &str) -> Option<Status> {
        [Status::Created, Status::Paid, Status::Shipped, Status::Delivered, Status::Cancelled]
            .into_iter()
            .find(|s| s.name() == name)
    }
}

/// The status after `verb`, or `None` when the order may not do that now.
fn next(current: Option<Status>, verb: &str) -> Option<Status> {
    match (current, verb) {
        (None, "create") => Some(Status::Created),
        (Some(Status::Created), "pay") => Some(Status::Paid),
        (Some(Status::Paid), "ship") => Some(Status::Shipped),
        (Some(Status::Shipped), "deliver") => Some(Status::Delivered),
        (Some(Status::Created | Status::Paid), "cancel") => Some(Status::Cancelled),
        _ => None,
    }
}

thread_local! {
    static ORDERS: RefCell<BTreeMap<String, Status>> = const { RefCell::new(BTreeMap::new()) };
}

struct Orders;

impl step::Guest for Orders {
    fn step(input: Input) -> Result<Vec<Output>, StepError> {
        let InputBody::Record(rec) = input.body else {
            return Err(StepError::Poison("orders takes no effect responses".into()));
        };
        let text = std::str::from_utf8(&rec.payload).unwrap_or_default();
        let mut words = text.split_whitespace();
        let (Some(order), Some(verb), None) = (words.next(), words.next(), words.next()) else {
            return Err(StepError::Reject(ErrorCode {
                code: MALFORMED,
                detail: "an input is `<order> <verb>`".to_string(),
            }));
        };
        ORDERS.with_borrow_mut(|orders| {
            let Some(status) = next(orders.get(order).copied(), verb) else {
                return Err(StepError::Reject(ErrorCode {
                    code: INVALID_TRANSITION,
                    detail: format!("order {order} cannot {verb}"),
                }));
            };
            orders.insert(order.to_string(), status);
            Ok(vec![Output::Record(Addressed {
                name: "order-state".to_string(),
                key: Some(order.as_bytes().to_vec()),
                payload: status.name().as_bytes().to_vec(),
            })])
        })
    }
}

impl state::Guest for Orders {
    fn snapshot() -> u32 {
        1
    }

    fn snapshot_chunk(_index: u32) -> Vec<u8> {
        ORDERS.with_borrow(|orders| {
            orders.iter().map(|(id, s)| format!("{id}\t{}\n", s.name())).collect::<String>().into_bytes()
        })
    }

    fn restore(chunks: u32) {
        assert_eq!(chunks, 1, "orders state is one chunk");
    }

    fn restore_chunk(_index: u32, chunk: Vec<u8>) {
        let text = String::from_utf8(chunk).expect("orders state is text");
        let orders = text
            .lines()
            .map(|line| {
                let (id, status) = line.split_once('\t').expect("an order and a status");
                (id.to_string(), Status::from_name(status).expect("a known status"))
            })
            .collect();
        ORDERS.set(orders);
    }

    fn inspect() -> String {
        ORDERS.with_borrow(|orders| {
            let items: Vec<String> =
                orders.iter().map(|(id, s)| format!("\"{id}\":\"{}\"", s.name())).collect();
            format!("{{{}}}", items.join(","))
        })
    }
}

export!(Orders);
```

### Submit it and read the outputs

Build the component as shown above, run the command in the spec, then publish:

```bash
felixctl pub order-events 'o1 create'
felixctl pub order-events 'o1 ship'
felixctl pub order-events 'o1 pay'
felixctl pub order-events 'o1 ship'
felixctl pub order-events 'o1 cancel'
felixctl pub order-events 'o1 deliver'
```

What the workload does with each input:

| Input | Outcome |
|---|---|
| `o1 create` | `order-state` for `o1`: `created` |
| `o1 ship` | rejected, code 2: `order o1 cannot ship` |
| `o1 pay` | `order-state` for `o1`: `paid` |
| `o1 ship` | `order-state` for `o1`: `shipped` |
| `o1 cancel` | rejected, code 2: `order o1 cannot cancel` |
| `o1 deliver` | `order-state` for `o1`: `delivered` |

Read the state it holds without a decoder:

```bash
felixd replay --id orders --to 1     # {"o1":"created"}
felixd replay --id orders --to 5     # {"o1":"delivered"}
felixd check --complete
```

Rejection is how a workload says no to one input. Use it for input that is wrong, and keep `poison` for a workload that cannot go on, since poison fails it.

## Idempotency keys

Passes each request on once. An input is `<key> <body>`. The first input with a key outputs its body under `unique`, keyed by the idempotency key. A later input with the same key outputs nothing. The last 10,000 keys are kept.

The count is part of the workload's behaviour. If you change it, a replay no longer matches what was recorded, so treat it like a change of code and submit it under a new id.

### Spec

```bash
felixctl stream create requests
felixd run --id dedup --input requests --replication-factor 1 dedup.wasm
```

`felixd run` writes this to the `workloads` cache:

```json
{
  "module": "<sha256 of the component>",
  "input": "requests",
  "output": "wl.dedup.out",
  "snapshot_every": 1000,
  "state": "running",
  "limits": {
    "fuel_per_call": 50000000,
    "max_memory_bytes": 67108864,
    "snapshot_fuel": 500000000
  }
}
```

### Code

`workloads/dedup/src/lib.rs`:

```rust
//! Idempotency keys. An input is `<key> <body>`. The first input with a key
//! outputs its body under `unique`, keyed by the idempotency key. Later ones
//! with the same key output nothing. Only the last `REMEMBER` keys are kept.

use std::cell::RefCell;
use std::collections::{BTreeSet, VecDeque};

wit_bindgen::generate!({
    path: "../../wit",
    world: "workload",
});

use exports::felixd::workload::state;
use exports::felixd::workload::step::{
    self, Addressed, ErrorCode, Input, InputBody, Output, StepError,
};

const MALFORMED: u32 = 1;
/// Keys remembered. The count is part of the behaviour, so changing it
/// changes what a replay outputs.
const REMEMBER: usize = 10_000;

#[derive(Default)]
struct Seen {
    order: VecDeque<String>,
    set: BTreeSet<String>,
}

impl Seen {
    /// Returns true when `key` is new.
    fn insert(&mut self, key: &str) -> bool {
        if !self.set.insert(key.to_string()) {
            return false;
        }
        self.order.push_back(key.to_string());
        while self.order.len() > REMEMBER {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }
        true
    }
}

thread_local! {
    static SEEN: RefCell<Seen> = RefCell::new(Seen::default());
}

struct Dedup;

impl step::Guest for Dedup {
    fn step(input: Input) -> Result<Vec<Output>, StepError> {
        let InputBody::Record(rec) = input.body else {
            return Err(StepError::Poison("dedup takes no effect responses".into()));
        };
        let text = std::str::from_utf8(&rec.payload).unwrap_or_default();
        let Some((key, body)) = text.split_once(' ').filter(|(key, _)| !key.is_empty()) else {
            return Err(StepError::Reject(ErrorCode {
                code: MALFORMED,
                detail: "an input is `<key> <body>`".to_string(),
            }));
        };
        if !SEEN.with_borrow_mut(|seen| seen.insert(key)) {
            return Ok(Vec::new());
        }
        Ok(vec![Output::Record(Addressed {
            name: "unique".to_string(),
            key: Some(key.as_bytes().to_vec()),
            payload: body.as_bytes().to_vec(),
        })])
    }
}

impl state::Guest for Dedup {
    fn snapshot() -> u32 {
        1
    }

    // About 40 bytes a key keeps this far below the 4 MiB chunk limit.
    fn snapshot_chunk(_index: u32) -> Vec<u8> {
        SEEN.with_borrow(|seen| {
            seen.order.iter().map(|k| format!("{k}\n")).collect::<String>().into_bytes()
        })
    }

    fn restore(chunks: u32) {
        assert_eq!(chunks, 1, "dedup state is one chunk");
    }

    fn restore_chunk(_index: u32, chunk: Vec<u8>) {
        let text = String::from_utf8(chunk).expect("dedup state is text");
        let mut seen = Seen::default();
        for key in text.lines() {
            seen.insert(key);
        }
        SEEN.set(seen);
    }

    fn inspect() -> String {
        SEEN.with_borrow(|seen| format!("{{\"remembered\":{}}}", seen.order.len()))
    }
}

export!(Dedup);
```

### Submit it and read the outputs

Build the component as shown above, run the command in the spec, then publish:

```bash
felixctl pub requests 'k1 hello'
felixctl pub requests 'k2 world'
felixctl pub requests 'k1 hello'
felixctl pub requests 'k3 x y'
felixctl pub requests 'nokey'
```

What the workload does with each input:

| Input | Outcome |
|---|---|
| `k1 hello` | `unique` for `k1`: `hello` |
| `k2 world` | `unique` for `k2`: `world` |
| `k1 hello` | nothing. The key was seen |
| `k3 x y` | `unique` for `k3`: `x y` |
| `nokey` | rejected, code 1 |

Read the state it holds without a decoder:

```bash
felixd replay --id dedup --to 4     # {"remembered":3}
felixd check --complete
```

Skipping an input is not an error, so a step may return an empty list. The input still counts: its offset is consumed and the next snapshot covers it.

## Scoring transactions

Scores card transactions. An input is `<account> <amount_cents>`. Every step outputs the score under `scored`, keyed by account. Five or more transactions on one account within a minute add 30, an amount of 500000 cents or more adds 30, and both together add 10 more. A score of 50 or more also outputs a line under `alert`.

The minute is measured with the input's `timestamp-micros`, the time the broker appended the record. A workload has no clock, and this field is the same on a live run, after a failover and in a replay.

### Spec

```bash
felixctl stream create transactions
felixd run --id fraud --input transactions --replication-factor 1 fraud.wasm
```

`felixd run` writes this to the `workloads` cache:

```json
{
  "module": "<sha256 of the component>",
  "input": "transactions",
  "output": "wl.fraud.out",
  "snapshot_every": 1000,
  "state": "running",
  "limits": {
    "fuel_per_call": 50000000,
    "max_memory_bytes": 67108864,
    "snapshot_fuel": 500000000
  }
}
```

### Code

`workloads/fraud/src/lib.rs`:

```rust
//! Scores card transactions. An input is `<account> <amount_cents>`. Every
//! step outputs the score under `scored`, keyed by account. A score of
//! `ALERT_AT` or more also outputs under `alert`. Time is the input's append
//! time, never a clock.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};

wit_bindgen::generate!({
    path: "../../wit",
    world: "workload",
});

use exports::felixd::workload::state;
use exports::felixd::workload::step::{
    self, Addressed, ErrorCode, Input, InputBody, Output, StepError,
};

const MALFORMED: u32 = 1;
const WINDOW_MICROS: u64 = 60_000_000;
const VELOCITY: usize = 5;
const LARGE_CENTS: u64 = 500_000;
const ALERT_AT: u32 = 50;

thread_local! {
    /// Append times of each account's transactions in the last minute.
    static RECENT: RefCell<BTreeMap<String, VecDeque<u64>>> = const { RefCell::new(BTreeMap::new()) };
}

struct Fraud;

fn record(name: &str, account: &str, payload: String) -> Output {
    Output::Record(Addressed {
        name: name.to_string(),
        key: Some(account.as_bytes().to_vec()),
        payload: payload.into_bytes(),
    })
}

impl step::Guest for Fraud {
    fn step(input: Input) -> Result<Vec<Output>, StepError> {
        let InputBody::Record(rec) = input.body else {
            return Err(StepError::Poison("fraud takes no effect responses".into()));
        };
        let text = std::str::from_utf8(&rec.payload).unwrap_or_default();
        let mut words = text.split_whitespace();
        let (Some(account), Some(Ok(cents)), None) =
            (words.next(), words.next().map(str::parse::<u64>), words.next())
        else {
            return Err(StepError::Reject(ErrorCode {
                code: MALFORMED,
                detail: "an input is `<account> <amount_cents>`".to_string(),
            }));
        };
        let now = input.timestamp_micros;
        let recent = RECENT.with_borrow_mut(|all| {
            let times = all.entry(account.to_string()).or_default();
            while times.front().is_some_and(|t| now.saturating_sub(*t) > WINDOW_MICROS) {
                times.pop_front();
            }
            times.push_back(now);
            times.len()
        });
        let mut score = 0;
        if recent >= VELOCITY {
            score += 30;
        }
        if cents >= LARGE_CENTS {
            score += 30;
        }
        if recent >= VELOCITY && cents >= LARGE_CENTS {
            score += 10;
        }
        let mut outputs = vec![record("scored", account, score.to_string())];
        if score >= ALERT_AT {
            outputs.push(record(
                "alert",
                account,
                format!("score {score}: {recent} transactions in a minute, latest {cents} cents"),
            ));
        }
        Ok(outputs)
    }
}

impl state::Guest for Fraud {
    fn snapshot() -> u32 {
        1
    }

    fn snapshot_chunk(_index: u32) -> Vec<u8> {
        RECENT.with_borrow(|all| {
            all.iter()
                .map(|(account, times)| {
                    let times: Vec<String> = times.iter().map(u64::to_string).collect();
                    format!("{account}\t{}\n", times.join(","))
                })
                .collect::<String>()
                .into_bytes()
        })
    }

    fn restore(chunks: u32) {
        assert_eq!(chunks, 1, "fraud state is one chunk");
    }

    fn restore_chunk(_index: u32, chunk: Vec<u8>) {
        let text = String::from_utf8(chunk).expect("fraud state is text");
        let all = text
            .lines()
            .map(|line| {
                let (account, times) = line.split_once('\t').expect("an account and its times");
                let times = times.split(',').filter(|t| !t.is_empty());
                (account.to_string(), times.map(|t| t.parse().expect("a time")).collect())
            })
            .collect();
        RECENT.set(all);
    }

    fn inspect() -> String {
        RECENT.with_borrow(|all| {
            let items: Vec<String> =
                all.iter().map(|(a, times)| format!("\"{a}\":{}", times.len())).collect();
            format!("{{{}}}", items.join(","))
        })
    }
}

export!(Fraud);
```

### Submit it and read the outputs

Build the component as shown above, run the command in the spec, then publish:

```bash
for i in 1 2 3 4 5; do felixctl pub transactions 'a 100'; done
felixctl pub transactions 'a 600000'
felixctl pub transactions 'b zz'
```

What the workload does with each input:

If the six `a` records arrive within a minute:

| Input | Outcome |
|---|---|
| the first four `a 100` | `scored` for `a`: `0` |
| the fifth `a 100` | `scored` for `a`: `30`, five in a minute |
| `a 600000` | `scored` for `a`: `70`, and `alert` for `a`: `score 70: 6 transactions in a minute, latest 600000 cents` |
| `b zz` | rejected, code 1 |

Read the state it holds without a decoder:

```bash
felixd replay --id fraud --to 5     # {"a":6}
felixd check --complete
```

`felixd replay --to 2026-10-29T14:02:00Z` rebuilds the same state as of a time, which is how you answer what the model knew when it scored a transaction.

## A tumbling-window aggregate

Groups readings into 10-second windows. An input is 16 bytes: a little-endian `u64` timestamp in milliseconds and a little-endian `i64` value. A reading that starts a later window closes the open one, and that step outputs the closed window's count, sum, minimum and maximum as a JSON line under `out`. Readings inside the open window output nothing. A reading that is not 16 bytes is rejected with code 1 and counted in the state.

It windows on the timestamp inside the record, not on the append time, so late readings land in the right window as long as the window is still open.

### Spec

```bash
felixctl stream create readings
felixd run --id window --input readings --snapshot-every 100 --replication-factor 1 window.wasm
```

`felixd run` writes this to the `workloads` cache:

```json
{
  "module": "<sha256 of the component>",
  "input": "readings",
  "output": "wl.window.out",
  "snapshot_every": 100,
  "state": "running",
  "limits": {
    "fuel_per_call": 50000000,
    "max_memory_bytes": 67108864,
    "snapshot_fuel": 500000000
  }
}
```

### Code

`workloads/window/src/lib.rs`:

```rust
//! A tumbling-window aggregate over `(timestamp_ms, value)` records.
//!
//! An input is 16 bytes: a little-endian `u64` timestamp in milliseconds and a
//! little-endian `i64` value. Records are grouped into windows of
//! `WINDOW_MS`. A record that starts a later window closes the open one, and
//! that step outputs the closed window's summary as a JSON line. Steps inside
//! one window output nothing. A record with an earlier timestamp than the open
//! window is counted in it. A malformed record is rejected with code
//! [`MALFORMED`] and counted.

use std::cell::RefCell;

wit_bindgen::generate!({
    path: "../../wit",
    world: "workload",
});

use exports::felixd::workload::state;
use exports::felixd::workload::step::{
    self, Addressed, ErrorCode, Input, InputBody, Output, StepError,
};

const WINDOW_MS: u64 = 10_000;
/// The rejection code for an input that is not 16 bytes.
const MALFORMED: u32 = 1;
const STATE_LEN: usize = 1 + 7 * 8;

#[derive(Clone, Copy, Default, PartialEq)]
struct State {
    open: Option<Window>,
    emitted: u64,
    rejected: u64,
}

#[derive(Clone, Copy, PartialEq)]
struct Window {
    start: u64,
    count: u64,
    sum: i64,
    min: i64,
    max: i64,
}

impl Window {
    fn new(start: u64, value: i64) -> Self {
        Window {
            start,
            count: 1,
            sum: value,
            min: value,
            max: value,
        }
    }

    fn add(&mut self, value: i64) {
        self.count += 1;
        self.sum = self.sum.wrapping_add(value);
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }

    fn summary(&self) -> Vec<u8> {
        format!(
            "{{\"window_start\":{},\"count\":{},\"sum\":{},\"min\":{},\"max\":{}}}",
            self.start, self.count, self.sum, self.min, self.max
        )
        .into_bytes()
    }
}

impl State {
    fn encode(&self) -> Vec<u8> {
        let w = self.open.unwrap_or(Window::new(0, 0));
        let mut out = Vec::with_capacity(STATE_LEN);
        out.push(self.open.is_some() as u8);
        out.extend(w.start.to_le_bytes());
        out.extend(w.count.to_le_bytes());
        out.extend(w.sum.to_le_bytes());
        out.extend(w.min.to_le_bytes());
        out.extend(w.max.to_le_bytes());
        out.extend(self.emitted.to_le_bytes());
        out.extend(self.rejected.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Option<State> {
        if bytes.len() != STATE_LEN {
            return None;
        }
        let word = |i: usize| -> [u8; 8] { bytes[1 + i * 8..9 + i * 8].try_into().unwrap() };
        let open = match bytes[0] {
            0 => None,
            1 => Some(Window {
                start: u64::from_le_bytes(word(0)),
                count: u64::from_le_bytes(word(1)),
                sum: i64::from_le_bytes(word(2)),
                min: i64::from_le_bytes(word(3)),
                max: i64::from_le_bytes(word(4)),
            }),
            _ => return None,
        };
        Some(State {
            open,
            emitted: u64::from_le_bytes(word(5)),
            rejected: u64::from_le_bytes(word(6)),
        })
    }
}

thread_local! {
    static STATE: RefCell<State> = RefCell::new(State::default());
}

struct WindowAggregate;

fn out(payload: Vec<u8>) -> Output {
    Output::Record(Addressed {
        name: "out".to_string(),
        key: None,
        payload,
    })
}

impl step::Guest for WindowAggregate {
    fn step(input: Input) -> Result<Vec<Output>, StepError> {
        let InputBody::Record(record) = input.body else {
            return Err(StepError::Poison(
                "window takes no effect responses".to_string(),
            ));
        };
        STATE.with_borrow_mut(|state| {
            let Ok(bytes) = <[u8; 16]>::try_from(record.payload.as_slice()) else {
                state.rejected += 1;
                return Err(StepError::Reject(ErrorCode {
                    code: MALFORMED,
                    detail: format!("an input is 16 bytes, not {}", record.payload.len()),
                }));
            };
            let ts = u64::from_le_bytes(bytes[..8].try_into().unwrap());
            let value = i64::from_le_bytes(bytes[8..].try_into().unwrap());
            let start = ts - ts % WINDOW_MS;
            match &mut state.open {
                Some(w) if start <= w.start => {
                    w.add(value);
                    Ok(Vec::new())
                }
                Some(w) => {
                    let closed = w.summary();
                    *w = Window::new(start, value);
                    state.emitted += 1;
                    Ok(vec![out(closed)])
                }
                None => {
                    state.open = Some(Window::new(start, value));
                    Ok(Vec::new())
                }
            }
        })
    }
}

impl state::Guest for WindowAggregate {
    fn snapshot() -> u32 {
        1
    }

    fn snapshot_chunk(_index: u32) -> Vec<u8> {
        STATE.with_borrow(State::encode)
    }

    fn restore(chunks: u32) {
        assert_eq!(chunks, 1, "window state is one chunk");
    }

    fn restore_chunk(_index: u32, chunk: Vec<u8>) {
        let decoded = State::decode(&chunk).expect("window state is malformed");
        STATE.set(decoded);
    }

    fn inspect() -> String {
        STATE.with_borrow(|s| {
            let open = match &s.open {
                Some(w) => format!(
                    "{{\"start\":{},\"count\":{},\"sum\":{},\"min\":{},\"max\":{}}}",
                    w.start, w.count, w.sum, w.min, w.max
                ),
                None => "null".to_string(),
            };
            format!(
                "{{\"open\":{open},\"emitted\":{},\"rejected\":{}}}",
                s.emitted, s.rejected
            )
        })
    }
}

export!(WindowAggregate);
```

### Submit it and read the outputs

Build the component as shown above, run the command in the spec, then publish:

```bash
reading() { python3 -I -c 'import struct, sys; sys.stdout.buffer.write(struct.pack("<Qq", int(sys.argv[1]), int(sys.argv[2])))' "$@"; }
reading 1000 5 | felixctl pub readings --whole
reading 2000 7 | felixctl pub readings --whole
reading 12000 3 | felixctl pub readings --whole
```

What the workload does with each input:

The third reading starts the window at 10000 and closes the one at 0:

```json
{"window_start":0,"count":2,"sum":12,"min":5,"max":7}
```

The first two steps output nothing. The window at 10000 stays open until a reading in a later window arrives.

Read the state it holds without a decoder:

```bash
felixd replay --id window --to 2     # {"open":{"start":10000,"count":1,"sum":3,"min":3,"max":3},"emitted":1,"rejected":0}
felixd check --complete
```

The state is a fixed 57-byte chunk. A workload with a large state returns several chunks, and only the ones that changed are written at the next snapshot.

## A deterministic simulation

A bouncing-ball world on a 1000 by 1000 grid. The inputs are `spawn <id> <x> <y>`, `push <id> <dvx> <dvy>` and `tick`. Time advances only on a `tick`, which moves every body one step in integer coordinates, bounces it off the walls and outputs the frame under `frame`. A `push` of a body that does not exist is rejected with code 2.

All of the arithmetic is on integers and the bodies are kept in an ordered map, so the same inputs give the same frames on any node. A game or simulation server written this way can be replayed afterwards from the commands alone.

### Spec

```bash
felixctl stream create sim-commands
felixd run --id sim --input sim-commands --snapshot-every 500 --replication-factor 1 sim.wasm
```

`felixd run` writes this to the `workloads` cache:

```json
{
  "module": "<sha256 of the component>",
  "input": "sim-commands",
  "output": "wl.sim.out",
  "snapshot_every": 500,
  "state": "running",
  "limits": {
    "fuel_per_call": 50000000,
    "max_memory_bytes": 67108864,
    "snapshot_fuel": 500000000
  }
}
```

### Code

`workloads/sim/src/lib.rs`:

```rust
//! A bouncing-ball world. Inputs are `spawn <id> <x> <y>`, `push <id> <dvx>
//! <dvy>` and `tick`. A tick advances every body one step in integer
//! coordinates and outputs the frame under `frame`. Time passes only on `tick`
//! inputs, so a replay runs the same simulation.

use std::cell::RefCell;
use std::collections::BTreeMap;

wit_bindgen::generate!({
    path: "../../wit",
    world: "workload",
});

use exports::felixd::workload::state;
use exports::felixd::workload::step::{
    self, Addressed, ErrorCode, Input, InputBody, Output, StepError,
};

const MALFORMED: u32 = 1;
const UNKNOWN_BODY: u32 = 2;
const SIZE: i32 = 1_000;

#[derive(Clone, Copy)]
struct Body {
    x: i32,
    y: i32,
    vx: i32,
    vy: i32,
}

/// Moves one axis and reflects off the walls at 0 and `SIZE`.
fn bounce(pos: i32, vel: i32) -> (i32, i32) {
    let moved = pos.saturating_add(vel);
    if (0..=SIZE).contains(&moved) {
        (moved, vel)
    } else {
        (moved.clamp(0, SIZE), vel.saturating_neg())
    }
}

thread_local! {
    static WORLD: RefCell<BTreeMap<u32, Body>> = const { RefCell::new(BTreeMap::new()) };
}

struct Sim;

fn reject(code: u32, detail: &str) -> StepError {
    StepError::Reject(ErrorCode { code, detail: detail.to_string() })
}

impl step::Guest for Sim {
    fn step(input: Input) -> Result<Vec<Output>, StepError> {
        let InputBody::Record(rec) = input.body else {
            return Err(StepError::Poison("sim takes no effect responses".into()));
        };
        let text = std::str::from_utf8(&rec.payload).unwrap_or_default();
        let words: Vec<&str> = text.split_whitespace().collect();
        let num = |i: usize| words.get(i).and_then(|w| w.parse::<i32>().ok());
        WORLD.with_borrow_mut(|world| match words.as_slice() {
            ["tick"] => {
                for body in world.values_mut() {
                    (body.x, body.vx) = bounce(body.x, body.vx);
                    (body.y, body.vy) = bounce(body.y, body.vy);
                }
                let frame: Vec<String> =
                    world.iter().map(|(id, b)| format!("{id}:{},{}", b.x, b.y)).collect();
                Ok(vec![Output::Record(Addressed {
                    name: "frame".to_string(),
                    key: None,
                    payload: frame.join(" ").into_bytes(),
                })])
            }
            ["spawn", ..] => {
                let (Some(id), Some(x), Some(y)) = (num(1), num(2), num(3)) else {
                    return Err(reject(MALFORMED, "spawn <id> <x> <y>"));
                };
                let id = u32::try_from(id).map_err(|_| reject(MALFORMED, "id is not negative"))?;
                let (x, y) = (x.clamp(0, SIZE), y.clamp(0, SIZE));
                world.insert(id, Body { x, y, vx: 0, vy: 0 });
                Ok(Vec::new())
            }
            ["push", ..] => {
                let (Some(id), Some(dx), Some(dy)) = (num(1), num(2), num(3)) else {
                    return Err(reject(MALFORMED, "push <id> <dvx> <dvy>"));
                };
                let body = u32::try_from(id).ok().and_then(|id| world.get_mut(&id));
                let body = body.ok_or_else(|| reject(UNKNOWN_BODY, "no such body"))?;
                body.vx = body.vx.saturating_add(dx);
                body.vy = body.vy.saturating_add(dy);
                Ok(Vec::new())
            }
            _ => Err(reject(MALFORMED, "spawn, push or tick")),
        })
    }
}

impl state::Guest for Sim {
    fn snapshot() -> u32 {
        1
    }

    fn snapshot_chunk(_index: u32) -> Vec<u8> {
        WORLD.with_borrow(|world| {
            world
                .iter()
                .map(|(id, b)| format!("{id} {} {} {} {}\n", b.x, b.y, b.vx, b.vy))
                .collect::<String>()
                .into_bytes()
        })
    }

    fn restore(chunks: u32) {
        assert_eq!(chunks, 1, "sim state is one chunk");
    }

    fn restore_chunk(_index: u32, chunk: Vec<u8>) {
        let text = String::from_utf8(chunk).expect("sim state is text");
        let world = text
            .lines()
            .map(|line| {
                let n: Vec<i32> = line.split(' ').map(|w| w.parse().expect("a number")).collect();
                (n[0] as u32, Body { x: n[1], y: n[2], vx: n[3], vy: n[4] })
            })
            .collect();
        WORLD.set(world);
    }

    fn inspect() -> String {
        WORLD.with_borrow(|world| format!("{{\"bodies\":{}}}", world.len()))
    }
}

export!(Sim);
```

### Submit it and read the outputs

Build the component as shown above, run the command in the spec, then publish:

```bash
felixctl pub sim-commands 'spawn 1 990 10'
felixctl pub sim-commands 'push 1 15 -20'
felixctl pub sim-commands 'tick'
felixctl pub sim-commands 'tick'
felixctl pub sim-commands 'push 9 1 1'
```

What the workload does with each input:

| Input | Outcome |
|---|---|
| `spawn 1 990 10` | nothing |
| `push 1 15 -20` | nothing |
| `tick` | `frame`: `1:1000,0`. The body hit the right and top walls |
| `tick` | `frame`: `1:985,20` |
| `push 9 1 1` | rejected, code 2: `no such body` |

Read the state it holds without a decoder:

```bash
felixd replay --id sim --to 3     # {"bodies":1}
felixd check --complete
```

`felixd replay --diff --module <hash>` runs a changed build of the simulation over the recorded commands and reports the first output that differs, so you can check a new physics step against what was recorded before you switch to it.
