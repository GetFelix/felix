#!/usr/bin/env python3
"""CPU profiles for the parity matrix's PROFILE=1 mode (see run.sh).

    profile.py fold < perf-script-output > folded     one line per stack: frames;...;leaf count
    profile.py top <folded>... [--top N]              Markdown: top frames by self and
                                                      inclusive samples, and by category

fold does what inferno-collapse-perf does for `perf script` output, so the boxes need only
perf. Rust symbol hashes and closure markers are trimmed so one function reads the same in
both engines' profiles.
"""
import re
import sys
from collections import Counter

# Checked against a stack leaf first; the first category with a matching frame takes it.
CATEGORIES = [
    ("openraft core", re.compile(r"^openraft::")),
    ("raft engine (felix)", re.compile(r"felix_broker::.*raft|felix_raft|raft_engine|raft::", re.I)),
    ("disk log / fsync", re.compile(r"felix_storage|fsync|fdatasync|pwrite|io_uring", re.I)),
    ("quic / tls", re.compile(r"^(quinn|rustls|ring|aws_lc)", re.I)),
    ("serde / json", re.compile(r"^serde|json", re.I)),
    ("tracing / metrics", re.compile(r"^(tracing|metrics|prometheus)", re.I)),
    ("alloc / copy", re.compile(r"malloc|^free$|cfree|realloc|_int_|__rust_(alloc|dealloc|realloc)|memcpy|memmove")),
    ("tokio runtime", re.compile(r"^tokio::")),
]


def clean(frame):
    frame = re.sub(r"\+0x[0-9a-f]+$", "", frame)
    frame = re.sub(r"::h[0-9a-f]{16}$", "", frame)
    frame = frame.replace("::{{closure}}", "")
    return frame


def fold():
    stacks = Counter()
    frames = []
    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line.strip():
            if frames:
                stacks[";".join(reversed(frames))] += 1
            frames = []
            continue
        if not line[0].isspace():
            frames = []  # the sample header: comm pid time event
            continue
        parts = line.split(None, 1)
        if len(parts) < 2:
            continue
        sym = re.sub(r" \([^()]*\)$", "", parts[1]).strip()
        if sym == "[unknown]":
            m = re.search(r"\(([^()]*)\)$", parts[1])
            sym = "[%s]" % (m.group(1).rsplit("/", 1)[-1] if m else "unknown")
        frames.append(clean(sym).replace(";", ":"))
    if frames:
        stacks[";".join(reversed(frames))] += 1
    for s, n in stacks.items():
        print("%s %d" % (s, n))


def top(paths, limit):
    self_c, incl_c, cat_c = Counter(), Counter(), Counter()
    total = 0
    for path in paths:
        with open(path) as f:
            for line in f:
                stack, _, n = line.rstrip("\n").rpartition(" ")
                if not stack:
                    continue
                n = int(n)
                frames = stack.split(";")
                total += n
                self_c[frames[-1]] += n
                for fr in set(frames):
                    incl_c[fr] += n
                for name, rx in CATEGORIES:
                    if any(rx.search(fr) for fr in reversed(frames)):
                        cat_c[name] += n
                        break
                else:
                    cat_c["other"] += n
    if not total:
        print("no samples")
        return

    def table(title, counter, n):
        print("#### %s (%d samples)\n\n| %% | frame |\n|---|---|" % (title, total))
        for fr, c in counter.most_common(n):
            print("| %.1f | `%s` |" % (100.0 * c / total, fr[:140].replace("|", "/")))
        print()

    table("Category", cat_c, len(cat_c))
    table("Self", self_c, limit)
    table("Inclusive", Counter({k: v for k, v in incl_c.items() if v < total}), limit)


def main():
    if sys.argv[1:2] == ["fold"]:
        fold()
    elif sys.argv[1:2] == ["top"]:
        args = sys.argv[2:]
        limit = 30
        if "--top" in args:
            i = args.index("--top")
            limit = int(args[i + 1])
            del args[i:i + 2]
        top(args, limit)
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
