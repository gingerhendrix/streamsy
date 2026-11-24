---
packages:
  "@streamsy/views": minor
---

## Read live Views snapshots in a browser

Declare views with `View.live` and open them with `at(params)` in an Effect scope. The reader builds its own memory host. Use `snapshot` to read all committed views and their input offsets together, `snapshots` to subscribe, and `caughtUp` to wait for the initial replay. A browser can supply its State services through `RemoteStreams` and compare an input's offset with the offset of a write to that stream. [Read live snapshots](https://streamsy.dev/docs/views/live)
