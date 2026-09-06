# Streamsy 0.4.0 package verification

The private root version and the three publishable package versions are `0.4.0`:

| Package           | Directory        |
| ----------------- | ---------------- |
| `@streamsy/core`  | `packages/core`  |
| `@streamsy/views` | `packages/views` |
| `@streamsy/serve` | `packages/serve` |

Conformance and examples remain private. Core exposes only `.`, `./http`,
`./storage`, `./testing` and has exactly one runtime dependency:
`effect@4.0.0-rc.112`. Serve uses a workspace dependency on core; Bun rewrites it
to the package version when packing. Existing Effect pins and root overrides stay
unchanged. Views and serve retain their existing curated subpaths.

## Review gate

From a clean checkout, remove only generated `packages/*/dist` outputs before
verification so retired artifacts cannot mask missing exports:

```sh
git clean -fdx -- packages/*/dist
bun install
bun run build
bun run typecheck
bun run lint
bun run lint:policy
bun run format:check
bun run test:unit
bun run test:conformance
bun run check:perimeter
bun run pack:dry-run
git diff --check
```

Inspect each dry-run file list and resolve every types/import/default export.
Core has four public entry pairs; the bundler may emit shared implementation and
declaration chunks required by those entries. No retired entry, test fixture or
stale source should enter its tarball. The testing entry intentionally imports
`bun:test`; ordinary core/http/storage must not.

The official memory suite uses the approved Vitest-under-Bun runner and expects
332 pass plus six skips. Authored tests use Bun. SQL, filesystem, client and hosted
DO conformance are absent from this gate. Filesystem retirement preserves its
confirmed, unfixed defect evidence; a green memory gate says nothing about that
backend. Site validation joins Step 1 acceptance in Batch 7.

## Release boundary

This is package preparation only. Publishing, tags, pushes, deprecations,
deployments and releases require separate authorization. The existing tag workflow
is kept aligned with the three package directories and memory-only checks; this
batch does not execute it. Review the final site/support matrix and release
measurements before authorizing release operations.
