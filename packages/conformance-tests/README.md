# conformance-tests

Runs the official Durable Streams conformance suite against Streamsy hosts.
Private; not published.

```bash
bun run build                                              # build the packages first
bun run --cwd packages/conformance-tests test:memory       # memory Layer on Bun
bun run --cwd packages/conformance-tests test:sqlite       # Bun SQLite
bun run --cwd packages/conformance-tests test:workerd      # Durable Object SQLite in local Miniflare
bun run --cwd packages/conformance-tests test:fetch        # the fetch Layer against a Bun host
bun run --cwd packages/conformance-tests test:workerd:ownership
```

`test:workerd` builds `dist/worker/worker.js` from the public package exports
and writes `bundle-report.json` beside it; `measure:bundle` prints that
report. The workerd run places every suite stream in one Durable Object with
`Placement.byKey(() => "conformance")` so forks and chains share an object.
It is local evidence on Miniflare, not a hosted deployment.

From the repository root, `bun run test:conformance` runs all of these.
