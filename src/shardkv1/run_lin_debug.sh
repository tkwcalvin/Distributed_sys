#!/bin/bash
# Run linearizability test and save operation history to file when it fails.
# Usage: ./run_lin_debug.sh [test name]
#   test name: TestOneConcurrentClerkReliable5A (default), TestManyConcurrentClerkReliable5A, etc.

TEST="${1:-TestOneConcurrentClerkReliable5A}"
OUT="${LIN_DEBUG_OUT:-lin_fail.log}"
export LIN_DEBUG_FILE="$OUT"
echo "Running $TEST (on failure debug dump -> $OUT)"
go test -v -run "$TEST" -count=1 2>&1 | tee lin_test_out.txt
echo "Exit: $?"
