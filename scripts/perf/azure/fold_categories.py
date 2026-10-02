#!/usr/bin/env python3
"""Share of profile samples by category, from collapsed stacks.

    fold_categories.py <broker>.folded[.gz] [--cores 7.47] [--mbs 1905]
                       [--top 8] [--by-thread] [--callers]

Input is `perf script | stackcollapse-perf.pl` output: one line per unique
stack, `frame;frame;...;leaf count`, root first. A leading `comm` or
`comm-pid/tid` frame (stackcollapse --pid/--tid) is detected and used for
--by-thread; it is not required.

Every sample lands in exactly one category. The rules, in order:

  1. Kernel network softirq anywhere in the stack (net_rx_action, NAPI,
     mlx5e/mana/netvsc, GRO): "kernel: softirq / NIC rx". Interrupts are
     charged to whatever thread they land on, so this is checked first.
  2. A syscall frame in the stack: classified by the syscall --
     recvmmsg/recvmsg   -> "kernel: UDP recv syscall"
     sendmsg/sendmmsg   -> "kernel: UDP send syscall"
     epoll/futex/sched  -> "tokio runtime / park"
     write/pwrite/fsync/io_uring -> "storage I/O (kernel)"
     anything else      -> "kernel: other"
     Other kernel stacks without a syscall frame (page faults, scheduler
     ticks) go to "kernel: other".
  3. User space, leaf first: AEAD/header-protection primitives are "QUIC
     crypto (AEAD)"; memcpy/memmove/malloc/free and Bytes refcounting are
     "memcpy / alloc". Otherwise the nearest frame (walking up from the leaf)
     that names a known owner decides: felix_wire, the broker and its
     publish path, felix_storage, quinn-proto, quinn's drivers, tokio.
  4. A bare [libc.so.6] leaf: "libc, unsymbolized (mostly memcpy/malloc)".
  5. Nothing matched: "other" ([unknown] frames land here; the script prints
     how many stacks are mostly unsymbolized so a high "other" is read as a
     symbol-coverage problem, not a CPU sink).

Also printed, because they answer the per-listener question directly:
  - "quinn endpoint driver, inclusive": samples with EndpointDriver /
    drive_recv / poll_socket anywhere in the stack (incl. its recvmmsg and the
    copies it does). With --cores, that share x cores is how many cores the
    single endpoint task burns; near 1.0 per listener means it is saturated.
  - "connection drivers, inclusive": ConnectionDriver / quinn-proto
    Connection::handle_event etc.

Standard library only.
"""

import argparse
import collections
import gzip
import re
import sys

# --- frame classifiers -----------------------------------------------------

SOFTIRQ = re.compile(
    r"^(net_rx_action|__napi_poll|napi_poll|handle_softirqs|__do_softirq|do_softirq|"
    r"irq_exit_rcu|__irq_exit_rcu|common_interrupt|asm_common_interrupt|"
    r"mlx5e?_\w+|mana_\w+|netvsc_\w+|gro_\w+|dev_gro_receive|napi_gro_\w+|"
    r"netif_receive_skb\w*|__netif_receive_skb\w*|ip_list_rcv|ip_sublist_rcv\w*|"
    r"udp_gro_\w+|udp4_gro_\w+|udp_queue_rcv\w*|__udp4_lib_rcv|udp_rcv)(_\[k\])?$"
)
SYSCALL_ENTRY = re.compile(r"^(entry_SYSCALL_64\w*|do_syscall_64|x64_sys_call)(_\[k\])?$")
SYS_RECV = re.compile(r"^(__x64_sys_recvmmsg|__x64_sys_recvmsg|__x64_sys_recvfrom|do_recvmmsg|__sys_recvmsg|___sys_recvmsg|udp_recvmsg)(_\[k\])?$")
SYS_SEND = re.compile(r"^(__x64_sys_sendmsg|__x64_sys_sendmmsg|__x64_sys_sendto|__sys_sendmsg|___sys_sendmsg|__sys_sendmmsg|udp_sendmsg)(_\[k\])?$")
SYS_PARK = re.compile(r"^(__x64_sys_epoll_wait|__x64_sys_epoll_pwait\w*|do_epoll_wait|__x64_sys_futex\w*|do_futex|futex_\w+|__x64_sys_sched_yield|__x64_sys_nanosleep|__x64_sys_clock_nanosleep|__x64_sys_eventfd\w*|eventfd_\w+)(_\[k\])?$")
SYS_STORAGE = re.compile(r"^(__x64_sys_(p?write\w*|fsync|fdatasync|io_uring_\w+|fallocate|sync_file_range|pread\w*)|ext4_\w+|xfs_\w+|io_uring\w*|blk_\w+|nvme_\w+)(_\[k\])?$")
SYS_WRITE_ON_EVENTFD = re.compile(r"^(eventfd_write|eventfd_\w+)(_\[k\])?$")
KERNEL_HINT = re.compile(r"_\[k\]$|^(entry_SYSCALL|do_syscall|asm_|exc_page_fault|__schedule|schedule|irq_|__irq|ret_from_fork|page_fault)")

# User-space leaf categories.
CRYPTO = re.compile(
    r"aes_gcm|aesni_gcm|gcm_ghash|gcm_gmult|gcm_init|ghash|aes_hw_|aes_nohw|vpaes_|"
    r"aesni_(ctr|encrypt|ecb)|aes_ctr|chacha|poly1305|CRYPTO_gcm128|EVP_AEAD|EVP_aead|"
    r"aws_lc_rs::aead|aws_lc_\w*aes|ring_core_\w*(aes|gcm|chacha|poly)|"
    r"HeaderProtection|header_protection|PacketKey|::decrypt_packet|::encrypt_packet|"
    r"rustls::quic|quinn_proto::crypto",
    re.I,
)
MEM = re.compile(
    r"^(__memmove\w*|__memcpy\w*|memcpy\w*|memmove\w*|__memset\w*|memset\w*|"
    r"malloc|free|cfree|calloc|realloc|_int_malloc|_int_free\w*|malloc_consolidate|"
    r"__libc_malloc|__libc_free|__libc_calloc|__libc_realloc|tcache_\w+|unlink_chunk\w*|"
    r"sysmalloc|__default_morecore|__rust_alloc\w*|__rust_dealloc|__rust_realloc|"
    r"__rdl_\w+|__rg_\w+|alloc::alloc::\w+|alloc::raw_vec::\w+|"
    r"je_\w+|_rjem_\w+|mi_\w+|tikv_jemallocator\w*)$"
    r"|bytes::bytes_mut::\w*(promote|shared_v_|reserve|extend_from_slice|from)\w*"
    r"|bytes::bytes::(shared_|promotable_)\w*(clone|drop|to_vec)"
    r"|<bytes::bytes::Bytes as core::ops::drop::Drop>|<bytes::bytes_mut::BytesMut as core::ops::drop::Drop>"
    r"|<alloc::vec::Vec<\w+> as core::clone::Clone>|<\[u8\]>::to_vec|alloc::slice::<impl \[T\]>::to_vec"
)

# Owners, checked from the leaf upward; the first frame that matches decides.
OWNERS = [
    ("felix-wire decode/encode", re.compile(r"felix_wire::")),
    ("broker publish / scheduler", re.compile(
        r"felix_broker_service::serving::quic::handlers::publish|felix_broker_service::serving::quic::streams|"
        r"felix_broker::broker::publish|felix_broker::stream|felix_broker::broker|felix_broker_service::|felix_broker::")),
    ("storage (user space)", re.compile(r"felix_storage::")),
    ("quinn-proto packet processing", re.compile(r"quinn_proto::")),
    ("quinn drivers / udp", re.compile(r"quinn::|quinn_udp::")),
    ("tokio runtime / park", re.compile(r"tokio::|mio::|parking_lot|std::sys::\w+::(futex|thread_parking)|std::thread::park")),
    ("tracing / metrics", re.compile(r"tracing::|tracing_core::|tracing_subscriber::|metrics::|prometheus|opentelemetry")),
]

ENDPOINT_DRIVER = re.compile(r"quinn::endpoint::EndpointDriver|quinn::endpoint::State>?::drive_recv|quinn::endpoint::RecvState>?::poll_socket|EndpointDriver as core::future::future::Future")
CONNECTION_DRIVER = re.compile(r"quinn::connection::ConnectionDriver|quinn::connection::State>?::(process_conn_events|drive_transmit)|quinn_proto::connection::Connection>?::handle_event")

CATEGORY_ORDER = [
    "QUIC crypto (AEAD)",
    "quinn-proto packet processing",
    "quinn drivers / udp",
    "kernel: UDP recv syscall",
    "kernel: UDP send syscall",
    "kernel: softirq / NIC rx",
    "memcpy / alloc",
    "libc, unsymbolized (mostly memcpy/malloc)",
    "felix-wire decode/encode",
    "broker publish / scheduler",
    "storage (user space)",
    "storage I/O (kernel)",
    "tokio runtime / park",
    "tracing / metrics",
    "kernel: other",
    "other",
]

UNKNOWN = re.compile(r"^\[unknown\]$|^0x[0-9a-f]+$|^\[[^\]]+\.so[^\]]*\]$")
THREAD_FRAME = re.compile(r"^[\w\-\.: /]+?(-\d+(/\d+)?)?$")


def strip_k(frame):
    return frame[:-4] if frame.endswith("_[k]") else frame


def classify(frames):
    """frames: root..leaf. Returns a category name."""
    names = [strip_k(f) for f in frames]
    # 1. softirq anywhere.
    for f in names:
        if SOFTIRQ.match(f):
            return "kernel: softirq / NIC rx"
    # 2. syscalls.
    if any(SYSCALL_ENTRY.match(f) for f in names):
        for f in reversed(names):
            if SYS_RECV.match(f):
                return "kernel: UDP recv syscall"
            if SYS_SEND.match(f):
                return "kernel: UDP send syscall"
            if SYS_PARK.match(f):
                return "tokio runtime / park"
            if SYS_STORAGE.match(f):
                return "storage I/O (kernel)"
        return "kernel: other"
    leaf = names[-1] if names else ""
    if any(f.endswith("_[k]") for f in frames) or KERNEL_HINT.search(leaf):
        if re.match(r"^(__schedule|schedule|finish_task_switch\S*|pick_next_task\S*)$", leaf):
            return "tokio runtime / park"
        return "kernel: other"
    # 3. user space, leaf first.
    if CRYPTO.search(leaf):
        return "QUIC crypto (AEAD)"
    if MEM.search(leaf):
        return "memcpy / alloc"
    if re.match(r"^\[libc[\w.\-]*\.so[\w.]*\]$", leaf):
        # A stripped libc frame in a Rust process is nearly always memmove,
        # memcpy, malloc or free. Kept apart so it is not mistaken for either.
        return "libc, unsymbolized (mostly memcpy/malloc)"
    for f in reversed(names):
        if CRYPTO.search(f) and not f.startswith("quinn_proto::connection"):
            return "QUIC crypto (AEAD)"
        for cat, rx in OWNERS:
            if rx.search(f):
                return cat
    return "other"


def nearest_owner(frames):
    """For memcpy/alloc samples: the nearest frame that names a crate."""
    for f in reversed(frames):
        f = strip_k(f)
        if MEM.search(f) or UNKNOWN.match(f) or f.startswith("[") or f.startswith("__"):
            continue
        m = re.search(r"(felix_\w+|quinn_proto|quinn_udp|quinn|tokio|bytes|rustls|aws_lc_rs|std|alloc|core)::[\w:<> ]{0,80}", f)
        if m:
            return m.group(0)[:90]
    return "(unattributed)"


def open_any(path):
    if path == "-":
        return sys.stdin
    if path.endswith(".gz"):
        return gzip.open(path, "rt", errors="replace")
    return open(path, errors="replace")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("folded")
    ap.add_argument("--cores", type=float, help="process cores over the profile window (e.g. ss_cores from summarize.py); converts shares to cores")
    ap.add_argument("--mbs", type=float, help="ingress MB/s over the same window; prints cores per GB/s per category")
    ap.add_argument("--top", type=int, default=0, help="also print the N heaviest leaf symbols per category")
    ap.add_argument("--by-thread", action="store_true", help="split by the leading comm/thread frame")
    ap.add_argument("--callers", action="store_true", help="attribute memcpy/alloc samples to their nearest crate frame")
    args = ap.parse_args()

    totals = collections.Counter()
    leaves = collections.defaultdict(collections.Counter)
    threads = collections.defaultdict(collections.Counter)
    callers = collections.Counter()
    endpoint_incl = conn_incl = unsymbolized = endpoint_proxy = 0
    total = 0
    lines = bad = 0

    with open_any(args.folded) as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line:
                continue
            stack, _, count = line.rpartition(" ")
            try:
                n = int(float(count))
            except ValueError:
                bad += 1
                continue
            lines += 1
            frames = stack.split(";")
            thread = None
            # stackcollapse puts the comm (optionally -pid/tid) first; a Rust or
            # kernel frame never looks like "tokio-rt-worker" or "felix-broker-123".
            if frames and not ("::" in frames[0] or frames[0].endswith("_[k]")) and THREAD_FRAME.match(frames[0]) and len(frames) > 1:
                thread = re.sub(r"-\d+(/\d+)?$", "", frames[0])
                frames = frames[1:]
            cat = classify(frames)
            totals[cat] += n
            total += n
            if args.top:
                leaves[cat][strip_k(frames[-1])] += n
            if args.by_thread:
                threads[thread or "(no thread frame)"][cat] += n
            if args.callers and cat == "memcpy / alloc":
                callers[nearest_owner(frames)] += n
            joined = stack
            if ENDPOINT_DRIVER.search(joined):
                endpoint_incl += n
            # recvmmsg has one caller in the broker (the endpoint driver), so
            # its syscall time belongs to the driver even when the user frames
            # above it did not unwind.
            if ENDPOINT_DRIVER.search(joined) or cat == "kernel: UDP recv syscall":
                endpoint_proxy += n
            if CONNECTION_DRIVER.search(joined):
                conn_incl += n
            unknown = sum(1 for f in frames if UNKNOWN.match(f))
            if frames and unknown * 2 >= len(frames):
                unsymbolized += n

    if not total:
        sys.exit("no samples parsed")

    def cores(share):
        return f"{share * args.cores:6.2f}" if args.cores else ""

    print(f"{args.folded}: {total:,} samples, {lines:,} stacks" + (f", {bad} unparsable lines" if bad else ""))
    hdr = f"{'category':42} {'share':>7}"
    if args.cores:
        hdr += f" {'cores':>6}"
    if args.cores and args.mbs:
        hdr += f" {'cores/GB/s':>10}"
    print(hdr)
    for cat in CATEGORY_ORDER + sorted(set(totals) - set(CATEGORY_ORDER)):
        if cat not in totals:
            continue
        share = totals[cat] / total
        row = f"{cat:42} {100 * share:6.2f}%"
        if args.cores:
            row += f" {cores(share)}"
        if args.cores and args.mbs:
            row += f" {share * args.cores / (args.mbs / 1000):10.3f}"
        print(row)
        if args.top:
            for sym, c in leaves[cat].most_common(args.top):
                print(f"      {100 * c / total:6.2f}%  {sym[:110]}")
    print()
    for label, n in (("quinn endpoint driver, inclusive", endpoint_incl),
                     ("  ... or its recvmmsg (lower bound)", endpoint_proxy),
                     ("quinn connection drivers, inclusive", conn_incl)):
        share = n / total
        extra = f" = {share * args.cores:.2f} cores" if args.cores else ""
        print(f"{label:42} {100 * share:6.2f}%{extra}")
    if args.cores:
        print("  (an endpoint driver is one task: inclusive cores near 1.0 per listener means it is saturated)")
    share_unk = unsymbolized / total
    print(f"{'mostly-unsymbolized stacks':42} {100 * share_unk:6.2f}%"
          + ("   <-- high: build with frame pointers / debuginfo before trusting 'other'" if share_unk > 0.10 else ""))

    if args.callers and callers:
        print("\nmemcpy / alloc by nearest crate frame:")
        csum = sum(callers.values())
        for who, c in callers.most_common(12):
            print(f"  {100 * c / total:6.2f}% of all ({100 * c / csum:5.1f}% of memcpy/alloc)  {who}")

    if args.by_thread and threads:
        print("\nby thread (share of all samples):")
        for th, cats in sorted(threads.items(), key=lambda kv: -sum(kv[1].values())):
            tsum = sum(cats.values())
            top = ", ".join(f"{c} {100 * v / tsum:.0f}%" for c, v in cats.most_common(4))
            print(f"  {th:28} {100 * tsum / total:6.2f}%  ({top})")


if __name__ == "__main__":
    main()
