# Streamsy 0.4.0 package verification

The private root version and the four publishable package versions are `0.4.0`:

| Package             | Directory          |
| ------------------- | ------------------ |
| `@streamsy/core`    | `packages/core`    |
| `@streamsy/storage` | `packages/storage` |
| `@streamsy/views`   | `packages/views`   |
| `@streamsy/serve`   | `packages/serve`   |

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
bun install --frozen-lockfile
bun run build
bun run typecheck
bun run lint
bun run lint:policy
bun run format:check
bun run test:unit
bun run test:conformance
bun run check:perimeter
bun run pack:dry-run
bun run site:validate
git diff --check
```

Inspect each dry-run file list and resolve every types/import/default export.
Core has four public entry pairs; the bundler may emit shared implementation and
declaration chunks required by those entries. No retired entry, test fixture or
stale source should enter its tarball. The testing entry intentionally imports
`bun:test`; ordinary core/http/storage must not.

The official suite uses the approved Vitest-under-Bun runner and expects 332 pass
plus six skips independently on memory and Bun SQLite. Authored tests use Bun.
The final Step 2 authored baseline is 525 passes and seven expected skips: the
Fold SQLite CLI skip was enabled, while five core capability skips and two SQL
host-capability skips remain declared.
Filesystem, client and hosted DO conformance are absent from this gate. Filesystem retirement preserves its
confirmed, unfixed defect evidence; a green memory gate says nothing about that
backend. Site validation includes prerequisite builds, compiled excerpt equality,
four bounded snippet executions, terms, site check/build, OG and rendered links.

## Release boundary

This is package preparation only. Publishing, tags, pushes, deprecations,
deployments and releases require separate authorization. The existing tag workflow
is kept aligned with the four package directories and memory-plus-SQLite checks; this
batch does not execute it. Review the final site/support matrix and release
measurements before authorizing release operations.

## Operational prerequisites

These are release-planning instructions, not authorization to perform a release.
Before requesting approval, identify the exact reviewed commit and package
versions, complete the gate above, review all package file lists (including core
README and LICENSE), and record the remaining site/support and release-measurement
checks. Work from a clean checkout and preserve the reviewed lockfile.

The existing workflow uses Node 22 and npm 11.11.0; the prior runbook baseline is
Node 22.14.0 or newer and npm 11.5.1 or newer for provenance/trusted publishing.
Verify tool versions before an authorized release, alongside Bun and Git. The
perimeter gate requires Bun and Git and performs its text scans in-process.
Release tooling additionally needs npm account access to the `@streamsy` scope,
GitHub access to `gingerhendrix/streamsy`, and permission to create the intended
release tag. Establish credentials only for the approved release operation.

The private root and all four public manifests must agree on 0.4.0. Check the
final packed dependency versions as well as workspace manifests. CI and the tag
workflow run package dry runs before any publication step. A dry run does not
publish, create a tag or prove npm account configuration.

## Manual first publication and trusted publisher setup

For a package name that has never been published, plan a manual first publication
from the reviewed release commit. After explicit authorization, check registry
state for each of core, storage, views and serve; do not assume a name is absent or that an
existing version may be overwritten. Authenticate using the approved account,
create and inspect its Bun tarball, then publish that exact tarball with public
access. Verify the returned name, version and integrity and retain the evidence.
Do not run manual publication during this package-preparation task.

After a name exists, configure its npm trusted publisher for:

- GitHub organization/user: `gingerhendrix`.
- Repository: `streamsy`.
- Workflow filename: `publish.yml`.
- Environment: none, unless a separately reviewed workflow adds one.
- Publishing action: `npm publish`.

Record the successful first publication and publisher setup before approving a
release tag. The existing workflow validates tag/package versions, skips versions
already present, and publishes missing versions with provenance and public access.
A partial release needs inspection and an explicit recovery decision; do not bump
versions or republish blindly. Tag pushes and GitHub release creation are release
operations and remain permission-gated.

## Retired-package deprecation planning

Inventory published versions and consumers before proposing deprecation wording.
The retired graph includes `@streamsy/http-client`, `@streamsy/streams`,
`@streamsy/projection` and `@streamsy/state`; earlier names
include `@streamsy/client`, `@streamsy/experimental`, and `@streamsy/storage-memory`.
Do not imply that the four prepared packages replace every retired capability.
Local persistent Bun protocol storage is available through `@streamsy/storage`,
but hosted Durable Object protocol and the Effect fetch transport remain later
work.

Prepare package-specific messages, replacement links and affected version ranges
for review. Deprecation is a separate approved registry operation after suitable
replacement guidance exists; do not unpublish historical versions or change their
metadata during this task. The filesystem backend's confirmed CAS defect remains
unfixed despite retirement, and release/deprecation language must preserve that
fact. No registry lookup, publish, publisher setup or deprecation is executed here.

## Documentation target identity and state

`STREAMSY_DOCS_DEPLOYMENT` selects `production` (the default) or `preview`.
`preview` is a management alias for existing infrastructure, not a resource
rename. The historical selector `experimental` is also accepted as an alias.
Unknown selectors fail before Alchemy runs.

| Selector                        | Alchemy app ID               | Website resource ID          | Explicit worker name         | Domains                                |
| ------------------------------- | ---------------------------- | ---------------------------- | ---------------------------- | -------------------------------------- |
| `production`                    | `streamsy-docs`              | `streamsy-docs`              | Unset, as before             | `streamsy.gandrew.com`, `streamsy.dev` |
| `preview` (also `experimental`) | `streamsy-docs-experimental` | `streamsy-docs-experimental` | `streamsy-docs-experimental` | `experimental.streamsy.dev`            |

The compatibility mapping lives in `scripts/docs-deployment-targets.ts`, outside
the public site's authored-language scan. `site/alchemy.run.ts` consumes that
mapping without changing app/resource identities, domains, stage selection or
state-store configuration. Public site prose and source use the preview name.
The Website build remains under the same resource, preserving its child resource
path as well. This is static documentation hosting, not protocol-storage hosting.

The owner inspected local state read-only and found Website/build state under
`example-apps-base/site/.alchemy/streamsy-docs-experimental/gareth/` in the Streamsy
repository. This worktree has no `site/.alchemy`. The finding proves retained
local state exists; remote resource status was not queried. Retain the matching
state context for any separately authorized management operation. This correction
does not copy, adopt, edit or destroy state, create a replacement target, or query
an external account. Do not execute deployment or finalization as a validation
probe: Alchemy finalization manages resource lifecycle.

## Source citations before release

Public GitHub source citations currently target the `effect-first-live-perimeter`
branch so they identify the reviewed implementation during preparation. Before
release, retarget them to the approved public release commit or tag and verify
the resulting URLs. Local site validation verifies the cited files and compiled
excerpts; it does not prove that an unpushed branch is publicly accessible. This
pre-release citation task does not authorize a push, tag or publication.
