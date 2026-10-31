#!/usr/bin/env python3
"""Verify that `reference/error-codes.md` lists every code, class and reason.

A client decides what to do from these names, so a code the broker can send
that the page omits is one nobody can look up. The page is checked against the
wire crate that defines them:

* `ErrorCode` and `RetryClass` in `client/error_code.rs`, from their `as_str` arms,
* the `shard_unavailable_reason` and `trim_reason` constants in the same file,
* `PublishRefusalReason` and `CursorErrorReason` in `client/message/fields.rs`,
  from their variants.

Each set is looked up in its own section of the page, as the first cell of a
table row, so a name that only appears in prose does not count. Rows naming
something the code does not define are stale and fail too.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WIRE = REPO / "crates/protocol/felix-wire/src/client"
PAGE = REPO / "docs-site/src/content/docs/reference/error-codes.md"

# Reasons the broker service sends in `detail.reason` for `overloaded`. They are
# constants in the service rather than the wire crate, so they are named here
# and checked against it below.
OVERLOADED_REASONS_SOURCE = (
    REPO / "services/felix-broker-service/src/serving/quic/client_error.rs"
)


def snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def enum_variants(source: str, enum: str) -> list[str]:
    header = f"pub enum {enum} {{"
    start = source.index(header)
    depth = 1
    for offset, char in enumerate(source[start + len(header) :], start + len(header)):
        depth += char == "{"
        depth -= char == "}"
        if depth == 0:
            body = source[start + len(header) : offset]
            break
    else:
        raise SystemExit(f"could not find the end of `enum {enum}`")
    # Variants sit one level deep; strip nested braces before matching names.
    flat, level = [], 0
    for char in body:
        if char == "{":
            level += 1
        elif char == "}":
            level -= 1
        elif level == 0:
            flat.append(char)
    return re.findall(r"^\s*([A-Z][A-Za-z0-9]*)\b", "".join(flat), re.M)


def sections(text: str) -> dict[str, str]:
    parts = re.split(r"^## ", text, flags=re.M)[1:]
    return {part.split("\n", 1)[0].strip(): part for part in parts}


def rows(section: str) -> set[str]:
    return set(re.findall(r"^\| `([a-z_0-9]+)` \|", section, re.M))


def module_constants(source: str, module: str) -> set[str]:
    """The string values of the `pub const`s in `pub mod <module> { ... }`."""
    match = re.search(rf"pub mod {module} \{{(.*?)^\}}", source, re.S | re.M)
    if match is None:
        raise SystemExit(f"could not find `pub mod {module}`")
    return set(re.findall(r'pub const [A-Z_]+: &str = "([a-z_]+)"', match.group(1)))


def expected() -> dict[str, set[str]]:
    codes = (WIRE / "error_code.rs").read_text()
    fields = (WIRE / "message/fields.rs").read_text()
    overloaded = OVERLOADED_REASONS_SOURCE.read_text()
    return {
        "Error codes": set(re.findall(r'ErrorCode::[A-Za-z]+ => "([a-z_]+)"', codes)),
        "Retry classes": set(re.findall(r'RetryClass::[A-Za-z]+ => "([a-z_]+)"', codes)),
        "Shard unavailable reasons": module_constants(codes, "shard_unavailable_reason"),
        "Head trim reasons": module_constants(codes, "trim_reason"),
        "Overloaded reasons": set(
            re.findall(r'(?:TENANT_QUOTA|QUEUE_FULL)_REASON: &str = "([a-z_]+)"', overloaded)
        ),
        "Publish refusal reasons": {
            snake(v) for v in enum_variants(fields, "PublishRefusalReason")
        },
        "Cursor error reasons": {
            snake(v) for v in enum_variants(fields, "CursorErrorReason")
        },
    }


def main() -> int:
    page = sections(PAGE.read_text())
    failures = 0
    checked = 0
    for title, names in expected().items():
        if not names:
            print(f"\033[31mFAIL\033[0m found no names for `{title}`; the extractor is stale")
            failures += 1
            continue
        if title not in page:
            print(f"\033[31mFAIL\033[0m the page has no `## {title}` section")
            failures += 1
            continue
        listed = rows(page[title])
        checked += len(names)
        for name in sorted(names - listed):
            print(f"\033[31mFAIL\033[0m `{name}` is defined in the code and has no row under `{title}`")
            failures += 1
        for name in sorted(listed - names):
            print(f"\033[31mFAIL\033[0m `{name}` has a row under `{title}` and the code does not define it")
            failures += 1
    print(f"{checked} error code, class and reason name(s) checked, {failures} failure(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
