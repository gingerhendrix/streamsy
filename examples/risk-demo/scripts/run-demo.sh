#!/usr/bin/env bash

# Keep the demo supervisor outside the terminal's foreground process group so
# package-runner Ctrl-C handling cannot terminate it before its cleanup runs.
set -uo pipefail

demo_pid=""

stop_demo() {
  trap - INT TERM
  if [[ -n "$demo_pid" ]] && kill -0 "$demo_pid" 2>/dev/null; then
    kill -INT "$demo_pid" 2>/dev/null || true
  fi
  wait "$demo_pid" 2>/dev/null || true
  exit 0
}

trap stop_demo INT TERM

setsid bun scripts/demo.ts &
demo_pid=$!
wait "$demo_pid"
