# issue-tracker-projections

A projection-heavy issue tracker built on the experimental Streamsy mesh. It
proves one real multi-step durable path:

```text
IssueEvents(issueId)
  -> IssueDetail(issueId)        recovered single-source State projection
  -> ProjectBoard(projectId)     deterministic dynamic fan-in State projection
  -> browser
```

`examples/issue-tracker-demo` remains the simple baseline. This example is the
projection demo and is deliberately separate.

## The workspace UI

`src/` is a React workspace served as a static bundle from `dist/assets`:

- a project rail with durable counts and inline project creation;
- a three-column board with drag-and-drop **and** a per-card status select, so
  every movement is completable from the keyboard;
- an issue drawer (a full-screen sheet on mobile) for title, status, priority,
  assignee, and comments;
- optimistic patches that are display overlays only — they expire against the
  durable row, or after a bounded hold, and never become accepted state;
- a projection inspector that labels the three durable identities of the latest
  command and reports `Proven` / `Not yet` / `Incomparable` from server lineage;
- visible failures with a `Retry sync` action that replays the _same_
  `commandId`, so a retry reconciles instead of duplicating.

The browser reads durable **State streams** directly over the Durable Streams
HTTP endpoint (catch-up read, then long-poll live reads). Nothing on screen is
reconstructed from command responses, so a reload — or a second window — is
rebuilt from the board State stream alone.

The URL (`?workspace=…&project=…&issue=…`) is the shareable source of truth.

## Streams

```text
workspaces/{workspaceId}/projects
workspaces/{workspaceId}/issues/{issueId}/events
workspaces/{workspaceId}/issues/{issueId}/detail
workspaces/{workspaceId}/projects/{projectId}/membership
workspaces/{workspaceId}/projects/{projectId}/board
```

Stream identity and stream id are kept equal so durable lineage is readable by
inspection. Issue truth is split per issue; the board fans in over the project's
active issue-detail streams.

## Semantics worth knowing

- **Exact acknowledgement.** Every command carries a `commandId`, which becomes a
  producer lane on the issue events stream. A retried command returns
  `duplicate` and the API reports the _original_ offset with `reconciled: true`.
  Payload equality is never claimed.
- **Chained coverage.** A mutation reports `proven` only when durable lineage at
  both hops covers the accepted source position. A wake receipt or elapsed delay
  can never produce `proven`.
- **Wake is latency, repair is the guarantee.** The mutation request runs both
  projections immediately for low latency. `POST .../projects/{id}/repair` and
  the Cloudflare queue consumer run the same bounded work, so a lost pass
  converges.
- **Recovery cost.** Both kernels restore application state by scanning complete
  target history. That is O(history) and intentional; snapshots are deferred.
- **Issue keys.** The display key is derived from the count of durable join facts
  in the project. Concurrent creation can repeat a display key; key uniqueness is
  not a correctness law here.

## Local development

```bash
bun install
bun run --cwd examples/issue-tracker-projections dev      # http://localhost:8787
curl -X POST http://localhost:8787/api/workspaces/main/seed
open 'http://localhost:8787/?workspace=main&project=launch'
```

`dev` builds the browser bundle into `dist/assets` first; the local host serves
that directory, so re-run `build` after changing anything under `src/`. Seeding
is idempotent — the "Seed demo workspace" button in an empty workspace runs the
same command path.

The local host also serves the Durable Streams HTTP routes under `/streams/`, so
the board State stream is readable directly at
`/streams/workspaces/main/projects/launch/board`.

## Checks

```bash
bun run --cwd examples/issue-tracker-projections typecheck
bun run --cwd examples/issue-tracker-projections test       # vitest + bun sqlite
bun run --cwd examples/issue-tracker-projections build
bun run --cwd examples/issue-tracker-projections smoke:http
bun run --cwd examples/issue-tracker-projections seed:check
```

`smoke:ui` drives a production build in a real browser: keyboard issue creation,
drawer edits, comments, the accessible status control, inspector coverage, a
reload rebuilt from durable State, second-window convergence, and the mobile
sheet. It fails on any console error or failed application request. Playwright
is not a repository dependency, so the script skips when it is unavailable and
`scripts/ui-smoke.ts` is excluded from `typecheck`:

```bash
bun add -g playwright-core && bunx playwright install chromium
bun run --cwd examples/issue-tracker-projections build
PLAYWRIGHT_EXECUTABLE=<chrome binary> bun run --cwd examples/issue-tracker-projections smoke:ui
```

## Deployment

| Script             | Contract                                                                               |
| ------------------ | -------------------------------------------------------------------------------------- |
| `build`            | produce browser assets and the Worker bundle                                           |
| `deploy:check`     | typecheck the Alchemy program, build, and report credentials — changes nothing         |
| `deploy:demo`      | deploy an isolated named stage (`STAGE=...`) and print the URL                         |
| `smoke:deployment` | exercise health, mutations, projections, durable read, and re-entry against `DEMO_URL` |
| `destroy:demo`     | destroy only that stage                                                                |

```bash
STAGE=demo bun run --cwd examples/issue-tracker-projections deploy:demo
DEMO_URL=https://... bun run --cwd examples/issue-tracker-projections smoke:deployment
STAGE=demo bun run --cwd examples/issue-tracker-projections destroy:demo
```

Alchemy owns one Worker, one SQLite-backed `StreamStorage` Durable Object
namespace, one wake queue with its consumer, and the static assets. It owns no
workspace, project, issue, cursor, membership, or lineage value; all of those are
Streamsy runtime state inside the Durable Objects.

`deploy:demo` and `smoke:deployment` have **not** been run against a live account
in this batch: no Cloudflare credentials were available. `deploy:check` passes.
