#!/usr/bin/env python3
"""Decide whether a finished CI run should update the README badge, and render it.

  ci_badge.py --runs runs.json --run-id ID --sha SHA --out DIR [--dry-run]

runs.json is the `gh api .../actions/workflows/ci.yml/runs?branch=main&event=push&status=completed`
response. The badge shows the newest completed run that was not cancelled or skipped, and
only that run's own workflow_run event publishes it, so a late older run cannot overwrite it.
"""
import argparse
import json
import os
import sys

SKIPPED = {"cancelled", "skipped"}


def newest_finished(runs):
    done = [r for r in runs if r.get("conclusion") and r["conclusion"] not in SKIPPED]
    return max(done, key=lambda r: r["run_number"], default=None)


def render_svg(passing):
    # Shields flat style: 20px tall, grey label, coloured value.
    value, color = ("passing", "#4c1") if passing else ("failing", "#e05d44")
    lw, vw = 30, 50 if passing else 46
    w = lw + vw
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="20" role="img" '
        f'aria-label="CI: {value}"><title>CI: {value}</title>'
        '<linearGradient id="s" x2="0" y2="100%"><stop offset="0" stop-color="#bbb" stop-opacity=".1"/>'
        '<stop offset="1" stop-opacity=".1"/></linearGradient>'
        f'<clipPath id="r"><rect width="{w}" height="20" rx="3" fill="#fff"/></clipPath>'
        f'<g clip-path="url(#r)"><rect width="{lw}" height="20" fill="#555"/>'
        f'<rect x="{lw}" width="{vw}" height="20" fill="{color}"/>'
        f'<rect width="{w}" height="20" fill="url(#s)"/></g>'
        '<g fill="#fff" text-anchor="middle" '
        'font-family="Verdana,Geneva,DejaVu Sans,sans-serif" font-size="11">'
        f'<text x="{lw / 2}" y="15" fill="#010101" fill-opacity=".3">CI</text>'
        f'<text x="{lw / 2}" y="14">CI</text>'
        f'<text x="{lw + vw / 2}" y="15" fill="#010101" fill-opacity=".3">{value}</text>'
        f'<text x="{lw + vw / 2}" y="14">{value}</text></g></svg>\n'
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--runs", required=True)
    p.add_argument("--run-id", type=int, required=True)
    p.add_argument("--sha", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()

    with open(a.runs) as f:
        runs = json.load(f)["workflow_runs"]
    newest = newest_finished(runs)
    if newest is None or newest["id"] != a.run_id:
        print(f"skip: run {a.run_id} is not the newest finished main run")
        return 0

    status = {
        "conclusion": newest["conclusion"],
        "sha": a.sha,
        "run_url": newest["html_url"],
    }
    svg = render_svg(newest["conclusion"] == "success")
    if a.dry_run:
        print("would publish ci-status.json:")
        print(json.dumps(status, indent=2))
        print("would publish ci.svg:")
        print(svg)
        return 0
    os.makedirs(a.out, exist_ok=True)
    with open(os.path.join(a.out, "ci.svg"), "w") as f:
        f.write(svg)
    with open(os.path.join(a.out, "ci-status.json"), "w") as f:
        json.dump(status, f, indent=2)
        f.write("\n")
    print(f"publish: {status['conclusion']} {a.sha}")
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write("publish=true\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
