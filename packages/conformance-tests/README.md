# conformance-tests

Runs the official Durable Streams conformance suite against Streamsy hosts.
Private; not published.

```bash
bun run build                                              # build the packages first
bun run --cwd packages/conformance-tests test:memory       # memory Layer on Bun
bun run --cwd packages/conformance-tests test:sqlite       # Bun SQLite
bun run --cwd packages/conformance-tests test:workerd      # Durable Object SQLite in local Miniflare
bun run --cwd packages/conformance-tests test:fetch        # the fetch Layer against a Bun host
bun run test:e2e:deployed                                  # the workerd artifact deployed to Cloudflare
```

`test:workerd` builds `dist/worker/worker.js` from the public package exports
and writes `bundle-report.json` beside it; `measure:bundle` prints that
report. The workerd run places every suite stream in one Durable Object with
`Placement.byKey(() => "conformance")` so forks and chains share an object.
It is local evidence on Miniflare.

`test:deployed` runs the same suite against `SERVER_BASE_URL`. Do not run it
by hand. The `e2e/` package deploys the worker, sets the URL, runs this script
and destroys the worker. See `e2e/README.md`.

From the repository root, `bun run test:conformance` runs all of the local
suites. It does not deploy anything.
