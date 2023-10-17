#!/usr/bin/env bash
if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

trap 'kill -INT -$pid; exit 1' INT
runs=$1
for i in $(seq 1 $runs); do
    echo '***' Start TESTING in TRIAL $i
    timeout -k 2s 900s go test -run 2A -race &
    pid=$!
    if ! wait $pid; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    fi
done
echo '***' PASSED ALL $i TESTING TRIALS

