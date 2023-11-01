#!/usr/bin/env bash
workList=(
  "TestSnapshotRPC3B"
  "TestSnapshotSize3B"
  "TestSpeed3B"
  "TestSnapshotRecover3B"
  "TestSnapshotRecoverManyClients3B"
  "TestSnapshotUnreliable3B"
  "TestSnapshotUnreliableRecover3B"
  "TestSnapshotUnreliableRecoverConcurrentPartition3B"
  "TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B"
);
if [ $# -ne 2 ]; then
    echo "Usage: $0 numTrials or FileName"
    exit 1
fi

trap 'kill -INT -$pid; exit 1' INT
runs=$1
file=$2
for i in $(seq 1 $runs); do
    echo '***' Start TESTING in TRIAL $i '***'
    for work in ${workList[@]}
    do
      echo Start $work
      timeout -k 2s 900s go test -run $work -race> ./log/lab3B/$file &
      pid=$!
      if ! wait $pid; then
          echo '***' FAILED TEST $work IN TRIAL $i '***'
          exit 1
      fi
    done
done
echo '***' PASSED ALL $i TESTING TRIALS '***'


