#!/bin/bash
# usage: gate.sh full|bs
ulimit -n 65536
mkdir -p gate
r(){ name=$1; shift; "$@" > gate/$name.log 2>&1; ec=$?; echo "$name exit=$ec" | tee -a gate/summary.txt; }
: > gate/summary.txt
if [ "$1" = full ]; then
r repl cargo test -p felix-replication --features raft --lib
r build cargo build -p felix-broker-service --features raft --bin felix-broker
r shards flock /mnt/build/cluster.lock env FELIX_TEST_RAFT_SHARDS=1 cargo test -p felix-cluster --test raft_shards
r sim cargo check -p felix-sim --features raft --all-targets
r demo task demo:check
fi
for i in 1 2 3 4 5; do r bs$i env RUST_BACKTRACE=1 cargo test -p felix-broker-service --features raft --lib -- --nocapture; done
for f in gate/*.log; do echo "== $f"; grep -h "test result" $f; grep -h "^    [a-z].* FAILED\|^failures:$\|panicked at" $f | head -20; done
