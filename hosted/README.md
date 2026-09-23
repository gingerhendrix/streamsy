# hosted

A private package that prepares hosted Cloudflare runs of the conformance
Worker with Alchemy v2. It is not published and does not deploy anything yet.

Two stacks are declared. `alchemy.run.ts` deploys the prebuilt conformance
artifact from `packages/conformance-tests`. `alchemy.source.run.ts` typechecks
the Effect-native Worker and Durable Object in `src/source-worker.ts`, built on
`@streamsy/serve/alchemy` with Durable Object SQLite and single-object
placement. `Host.objectHandlers` owns the lazy scoped acquisition and compiles
`Http.routes({ prefix: "/streams" })` once. It installs mutation reconciliation
and shares the same services with alarms; no application copy of that lifecycle
helper remains.

```bash
bun run --cwd hosted typecheck   # the stack declarations
bun run --cwd hosted test        # validation, polling, measurement, cleanup, reports
bun run --cwd hosted evidence    # prints the current hosted status and exits 2
```

Hosted execution is disabled. `evidence` reports that status even when
credentials are present. Local conformance on workerd runs from
`packages/conformance-tests`; see its README.
