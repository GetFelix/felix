#!/usr/bin/env python3
"""CPU profiles for the parity matrix's PROFILE=1 mode (see run.sh).

    profile.py fold < perf-script-output > folded     one line per stack: frames;...;leaf count
    profile.py top <folded>... [--top N]              Markdown: samples by category, the
                                                      allocator share by caller, and the top
                                                      frames by self and inclusive samples

fold does what inferno-collapse-perf does for `perf script` output, so the boxes need only
perf. Rust symbol hashes and closure markers are trimmed so one function reads the same in
both engines' profiles.
"""
import re
import sys
from collections import Counter

# Allocator and copy frames: malloc and free, Vec growth, clones that memcpy.
ALLOC = re.compile(r"malloc|^free$|cfree|realloc|_int_|__rust_(alloc|dealloc|realloc)|memcpy|memmove|"
                   r"RawVec.*grow|alloc::raw_vec|drop_in_place")

# Each sample goes to the category of the frame nearest its leaf that matches one; a frame
# that matches several takes the first listed.
CATEGORIES = [
    ("allocator (malloc, free, clones)", ALLOC),
    ("tracing / metrics", re.compile(r"^(tracing|metrics|prometheus|opentelemetry)")),
    ("fsync / IO", re.compile(r"fsync|fdatasync|pwrite|pread|io_uring|felix_storage::io|ext4|xfs_|blk_|nvme|jbd2")),
    ("peer encode and RPC", re.compile(r"felix_replication::(peer|raft::(network|wire|peer))")),
    ("apply / state machine", re.compile(r"raft::machine|Machine|apply", re.I)),
    ("log append and codec", re.compile(r"felix_storage|raft::codec|raft_log|disk_log")),
    ("release / fanout / wakes", re.compile(r"completion|deliver|fanout|release|felix_broker::stream|wake|futex")),
    ("openraft core", re.compile(r"^openraft::")),
    ("client QUIC / TLS", re.compile(r"^(quinn|rustls|ring|aws_lc)|udp|sendmsg|recvmsg", re.I)),
    ("tokio scheduling", re.compile(r"^tokio::|epoll|schedule|park")),
]


def clean(frame):
    frame = re.sub(r"\+0x[0-9a-f]+$", "", frame)
    frame = re.sub(r"::h[0-9a-f]{16}$", "", frame)
    frame = frame.replace("::{{closure}}", "")
    return frame


def category(frames):
    for fr in reversed(frames):
        for name, rx in CATEGORIES:
            if rx.search(fr):
                return name
    return "other"


def fold():
    stacks = Counter()
    frames = []
    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line.strip():
            if len(frames) > 1:
                stacks[";".join([frames[0]] + frames[:0:-1])] += 1
            frames = []
            continue
        if not line[0].isspace():
            # The sample header (comm pid/tid time: event): the thread name is the root frame,
            # with its number dropped so a pool's threads add up.
            comm = re.sub(r"[-_]?\d+$", "", line.split(None, 1)[0])
            frames = ["[thread %s]" % comm]
            continue
        parts = line.split(None, 1)
        if len(parts) < 2:
            continue
        sym = re.sub(r" \([^()]*\)$", "", parts[1]).strip()
        if sym == "[unknown]":
            m = re.search(r"\(([^()]*)\)$", parts[1])
            sym = "[%s]" % (m.group(1).rsplit("/", 1)[-1] if m else "unknown")
        frames.append(clean(sym).replace(";", ":"))
    if len(frames) > 1:
        stacks[";".join([frames[0]] + frames[:0:-1])] += 1
    for s, n in stacks.items():
        print("%s %d" % (s, n))


def top(paths, limit):
    self_c, incl_c, cat_c, alloc_c, thread_c = Counter(), Counter(), Counter(), Counter(), Counter()
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
                thread_c[frames[0]] += n
                # An allocator leaf is charged to the nearest caller outside the allocator.
                if ALLOC.search(frames[-1]):
                    caller = next((fr for fr in reversed(frames) if not ALLOC.search(fr)), "?")
                    alloc_c[caller] += n
                for fr in set(frames):
                    incl_c[fr] += n
                cat_c[category(frames)] += n
    if not total:
        print("no samples")
        return

    def table(title, counter, n):
        print("#### %s (%d samples)\n\n| %% | frame |\n|---|---|" % (title, total))
        for fr, c in counter.most_common(n):
            print("| %.1f | `%s` |" % (100.0 * c / total, fr[:140].replace("|", "/")))
        print()

    table("Category", cat_c, len(cat_c))
    table("Thread", thread_c, 12)
    print("Allocator and copy leaves: %.1f%% of samples\n" % (100.0 * sum(alloc_c.values()) / total))
    table("Allocator and copy leaves by caller", alloc_c, 15)
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
