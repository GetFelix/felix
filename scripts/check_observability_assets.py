#!/usr/bin/env python3
"""Check the shipped Grafana dashboard and Prometheus alert rules against the code.

A dashboard panel that queries a metric nobody registers stays empty forever and
looks like "no traffic". So every `felix_*` name in deploy/observability/ has to
appear as a string literal in non-test Rust under crates/ or services/.

Also checks that:
  * the chart's copy of the rules is byte-identical to the shipped file,
  * every alert carries a runbook_url that points at a real docs page and, when
    it names a heading, at a heading that exists,
  * a metric the code records only through the telemetry-gated t_*! macros is
    named only in the allowlist below, which the docs call telemetry-only,
  * the Markdown page production.md names only registered metrics too,
  * no query uses `_bucket`: the exporter renders histograms as summaries
    (quantile series plus _sum and _count) except the few it is given buckets for.

Uses PyYAML to read the rules when it is installed (the Helm job has it). Without
it, a small line reader handles the shipped one-line `expr:` form and a
block-scalar `expr` is refused, so the check still runs in the Rust CI job with
the other docs:evidence checks.
"""

from __future__ import annotations

import json
import pathlib
import re
import sys

try:
    import yaml
except ImportError:  # the fallback reader below covers the shipped shape
    yaml = None

ROOT = pathlib.Path(__file__).resolve().parent.parent
OBS = ROOT / "deploy" / "observability"
RULES = OBS / "prometheus" / "felix-alerts.yaml"
DASHBOARD = OBS / "grafana" / "felix-dashboard.json"
CHART_RULES = ROOT / "deploy" / "helm" / "felix" / "files" / "felix-alerts.yaml"
DOCS = ROOT / "docs-site" / "src" / "content" / "docs"
DOCS_HOST = "https://docs.getfelix.dev"
GUIDE = DOCS / "deployment" / "production.md"

# Recorded only on builds with the `telemetry` feature. A default build never
# exports them, so each use has to say so (dashboard panel descriptions and the
# guide's text do).
TELEMETRY_ONLY_OK = {"felix_publish_latency_ms", "felix_broker_ingress_queue_depth"}
# Files that read metrics by name without registering them.
CONSUMERS = ("bin/soak/",)

NAME = re.compile(r"\bfelix_[a-z0-9_]+")
LITERAL = re.compile(r'"(felix_[a-z0-9_]+)"')
TELEMETRY_MACRO = re.compile(r't_(?:counter|gauge|histogram)!\(\s*"(felix_[a-z0-9_]+)"')
SUFFIXES = ("_sum", "_count")

failures: list[str] = []


def fail(message: str) -> None:
    failures.append(message)
    print(f"  FAIL {message}")


def registered_names() -> tuple[set[str], set[str]]:
    """Every metric name in non-test code, and those only the telemetry macros record."""
    names: set[str] = set()
    gated: set[str] = set()
    for base in ("crates", "services"):
        for path in (ROOT / base).rglob("*.rs"):
            rel = path.relative_to(ROOT)
            parts = rel.parts
            if "tests" in parts or "testing" in parts or path.name == "tests.rs":
                continue
            if any(marker in rel.as_posix() for marker in CONSUMERS):
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
            gated.update(TELEMETRY_MACRO.findall(text))
            plain = TELEMETRY_MACRO.sub("", text)
            names.update(LITERAL.findall(plain))
    gated -= names
    return names | gated, gated


def dashboard_queries(node: object) -> list[str]:
    found: list[str] = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key in ("expr", "query") and isinstance(value, str):
                found.append(value)
            else:
                found.extend(dashboard_queries(value))
    elif isinstance(node, list):
        for item in node:
            found.extend(dashboard_queries(item))
    return found


def parse_rules(text: str) -> list[tuple[str, str, str]]:
    """(alert, expr, runbook_url) for every alerting rule."""
    if yaml is not None:
        out = []
        for group in yaml.safe_load(text)["groups"]:
            for rule in group["rules"]:
                if "alert" not in rule:
                    fail("a recording rule is not expected in the alert file")
                    continue
                url = (rule.get("annotations") or {}).get("runbook_url", "")
                out.append((rule["alert"], str(rule["expr"]), url))
        return out
    if re.search(r"^\s*expr:\s*[|>]", text, flags=re.M):
        fail("a block-scalar expr needs PyYAML to be read; install it or keep exprs on one line")
    alerts = re.findall(r"^\s*- alert:\s*(\S+)", text, flags=re.M)
    exprs = re.findall(r"^\s*expr:\s*(.+)$", text, flags=re.M)
    urls = re.findall(r'^\s*runbook_url:\s*"([^"]+)"', text, flags=re.M)
    if not (len(alerts) == len(exprs) == len(urls)):
        fail("alert, expr and runbook_url counts differ")
        return []
    return list(zip(alerts, exprs, urls))


def check_names(source: str, texts: list[str], known: set[str], gated: set[str]) -> None:
    for text in texts:
        if "_bucket" in text:
            fail(f"{source}: uses _bucket in `{text}`; most histograms are summaries")
        for raw in NAME.findall(text):
            name = raw
            for suffix in SUFFIXES:
                if name.endswith(suffix) and name[: -len(suffix)] in known:
                    name = name[: -len(suffix)]
            if name not in known:
                fail(f"{source}: `{raw}` is not registered anywhere in crates/ or services/")
            elif name in gated and name not in TELEMETRY_ONLY_OK:
                fail(f"{source}: `{raw}` is only recorded on telemetry builds; allowlist it and say so in the docs")


def slug(heading: str) -> str:
    text = re.sub(r"[`*_]", "", heading.strip().lower())
    text = re.sub(r"[^\w\s-]", "", text)
    return re.sub(r"\s", "-", text)


def check_runbooks(rules: list[tuple[str, str, str]]) -> None:
    alerts = [name for name, _, _ in rules]
    urls = [url for _, _, url in rules]
    if not all(urls):
        fail("an alert has no runbook_url")
    if len(set(alerts)) != len(alerts):
        fail("an alert name is used twice")
    for url in sorted(set(urls)):
        if not url.startswith(DOCS_HOST + "/"):
            fail(f"runbook_url {url} is not under {DOCS_HOST}")
            continue
        path, _, anchor = url[len(DOCS_HOST) + 1 :].partition("#")
        page = DOCS / (path.strip("/") + ".md")
        if not page.is_file():
            fail(f"runbook_url {url}: no page at {page.relative_to(ROOT)}")
            continue
        if anchor:
            headings = re.findall(r"^#{2,6}\s+(.+)$", page.read_text(encoding="utf-8"), flags=re.M)
            if anchor not in {slug(h) for h in headings}:
                fail(f"runbook_url {url}: no heading with that anchor")


def check_guide(known: set[str], gated: set[str]) -> None:
    text = re.sub(r"\]\([^)]*\)", "]", GUIDE.read_text(encoding="utf-8"))  # link targets
    check_names(str(GUIDE.relative_to(ROOT)), [text], known, gated)
    lines = text.splitlines()
    for number, line in enumerate(lines, 1):
        for name in NAME.findall(line):
            nearby = " ".join(lines[max(0, number - 3) : number + 2]).lower()
            if name in gated and "telemetry" not in nearby:
                fail(f"{GUIDE.name}:{number}: `{name}` is telemetry-only; say so next to it")


def main() -> int:
    known, gated = registered_names()
    if len(known) < 100:
        fail(f"found only {len(known)} registered metrics; the scan is broken")

    rules_text = RULES.read_text(encoding="utf-8")
    rules = parse_rules(rules_text)
    exprs = [expr for _, expr, _ in rules]
    check_names(str(RULES.relative_to(ROOT)), exprs, known, gated)
    check_runbooks(rules)
    check_guide(known, gated)

    dash = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    queries = dashboard_queries(dash)
    if not queries:
        fail("the dashboard has no queries")
    check_names(str(DASHBOARD.relative_to(ROOT)), queries, known, gated)

    if not CHART_RULES.is_file() or CHART_RULES.read_text() != rules_text:
        fail(f"{CHART_RULES.relative_to(ROOT)} differs from {RULES.relative_to(ROOT)}; copy it over")

    if failures:
        print(f"{len(failures)} problem(s)")
        return 1
    print(f"ok: {len(queries)} dashboard queries and {len(rules)} alert rules use registered metrics")
    return 0


if __name__ == "__main__":
    sys.exit(main())
