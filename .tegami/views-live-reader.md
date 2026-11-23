---
packages:
  "@streamsy/views": minor
---

## Read live Views snapshots in a browser

Declare views with `View.live` and open them with `at(params)` in an Effect scope. Use `rows(name)` to read the committed rows, `changes(name)` to subscribe to snapshots, and `caughtUp` to wait for the initial replay. A browser can provide its State inputs through `RemoteStreams` and keep its view state in `ViewBackend.layerMemoryHost`. [Read live snapshots](https://streamsy.dev/docs/views/live)
