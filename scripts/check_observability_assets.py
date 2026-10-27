#!/usr/bin/env python3
"""Check the shipped Grafana dashboard and Prometheus alert rules against the code.

A dashboard panel that queries a metric nobody registers stays empty forever and
looks like "no traffic". So every `felix_*` name in deploy/observability/ has to
appear as a string literal in non-test Rust under crates/ or services/.

Also checks that:
  * the chart's copy of the rules is byte-identical to the shipped file,
  * every alert carries a runbook_url that points at a real docs page and, when
    it names a heading, at a heading that exists,
  * no query uses `_bucket`: the exporter renders histograms as summaries
    (quantile series plus _sum and _count) except the few it is given buckets for.

Python standard library only, so it runs in the Rust CI job with the other
docs:evidence checks.
"""

from __future__ import annotations

import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
OBS = ROOT / "deploy" / "observability"
RULES = OBS / "prometheus" / "felix-alerts.yaml"
DASHBOARD = OBS / "grafana" / "felix-dashboard.json"
CHART_RULES = ROOT / "deploy" / "helm" / "felix" / "files" / "felix-alerts.yaml"
DOCS = ROOT / "docs-site" / "src" / "content" / "docs"
DOCS_HOST = "https://docs.getfelix.dev"

NAME = re.compile(r"\bfelix_[a-z0-9_]+")
LITERAL = re.compile(r'"(felix_[a-z0-9_]+)"')
SUFFIXES = ("_sum", "_count")

failures: list[str] = []


def fail(message: str) -> None:
    failures.append(message)
    print(f"  FAIL {message}")


def registered_names() -> set[str]:
    names: set[str] = set()
    for base in ("crates", "services"):
        for path in (ROOT / base).rglob("*.rs"):
            parts = path.relative_to(ROOT).parts
            if "tests" in parts or "testing" in parts or path.name == "tests.rs":
                continue
            names.update(LITERAL.findall(path.read_text(encoding="utf-8", errors="replace")))
    return names


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


def alert_exprs(text: str) -> list[str]:
    return re.findall(r"^\s*expr:\s*(.+)$", text, flags=re.M)


def check_names(source: str, texts: list[str], known: set[str]) -> None:
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


def slug(heading: str) -> str:
    text = re.sub(r"[`*_]", "", heading.strip().lower())
    text = re.sub(r"[^\w\s-]", "", text)
    return re.sub(r"\s", "-", text)


def check_runbooks(text: str) -> None:
    alerts = re.findall(r"^\s*- alert:\s*(\S+)", text, flags=re.M)
    urls = re.findall(r'^\s*runbook_url:\s*"([^"]+)"', text, flags=re.M)
    if len(alerts) != len(urls):
        fail(f"{len(alerts)} alerts but {len(urls)} runbook_url annotations")
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


def main() -> int:
    known = registered_names()
    if len(known) < 100:
        fail(f"found only {len(known)} registered metrics; the scan is broken")

    rules = RULES.read_text(encoding="utf-8")
    check_names(str(RULES.relative_to(ROOT)), alert_exprs(rules), known)
    check_runbooks(rules)

    dash = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    queries = dashboard_queries(dash)
    if not queries:
        fail("the dashboard has no queries")
    check_names(str(DASHBOARD.relative_to(ROOT)), queries, known)

    if not CHART_RULES.is_file() or CHART_RULES.read_text() != rules:
        fail(f"{CHART_RULES.relative_to(ROOT)} differs from {RULES.relative_to(ROOT)}; copy it over")

    if failures:
        print(f"{len(failures)} problem(s)")
        return 1
    print(f"ok: {len(queries)} dashboard queries and {len(alert_exprs(rules))} alert rules use registered metrics")
    return 0


if __name__ == "__main__":
    sys.exit(main())
