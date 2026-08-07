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

## What this batch contains

This is the **backend vertical slice**: domain, commands, projections, coverage,
seeded data, the local host, the Cloudflare Worker, and the Alchemy program.
`public/` is a static shell only. The board UI, detail drawer, and projection
inspector arrive in the next batch.

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
curl http://localhost:8787/api/workspaces/main/projects/launch/board
```

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
