# Durable response: A1.0 and A2

A Bun host generates one Effect AI response into a SQLite-backed Streamsy log. Readers can drop, reconnect, or reload from an offset while the original model call continues. Any reader can stop the generation; every reader sees the same `finish` record with `metadata.stopped` before protocol closure.

From the repository root:

```sh
bun install --frozen-lockfile
bun run build
PORT=1340 bun run --cwd examples/durable-response dev
```

In another terminal, choose a page (both can run together):

```sh
bun run --cwd examples/durable-response-atom dev     # http://localhost:1341
bun run --cwd examples/durable-response-foldkit dev  # http://localhost:1342
```

The pages use Vite proxies for `/chat`, `/responses`, and `/debug`. Set `DURABLE_RESPONSE_API` on a page to select another API origin. `preview` uses the same proxies after `build`. No page is served by this API host.

The default model is a native `LanguageModel.make` provider that emits a fixed answer, with one part every 250 ms. It needs no key and never contacts a provider. `DURABLE_RESPONSE_PACE_MS=700` makes manual clicks easier. Each response has request metadata, a text start, ten text deltas, a text end, and a finish (14 entries total). `/debug/model-calls` counts calls and observed provider interruptions across this server process; reconnecting never increments calls.

SQLite defaults to `.data/responses.sqlite` in this example; override `DURABLE_RESPONSE_DB` with a filename. Data is retained until the database is removed. `build` bundles the Bun entry to `dist/`; `bun dist/index.js` runs it from this directory after dependencies and core packages have been built.

| Route                       | Behavior                                                                            |
| --------------------------- | ----------------------------------------------------------------------------------- |
| `POST /chat`                | `{ prompt, chatId }` → 201 `{ id, prompt, chatId }`, `Location: /responses/<id>`    |
| `GET /responses/<id>`       | Read-only Durable Streams protocol (`offset`, `live`)                               |
| `GET /chat/<chatId>/stream` | 200 with `Location` for the latest running generation, 204 if idle                  |
| `POST /responses/<id>/stop` | `{ by, reason }` → 200 `{ alreadyClosed }`; invalid input 400, unknown response 404 |
| `GET /debug/model-calls`    | Scripted model `{ calls, interruptions }`                                           |

A service-scoped `FiberMap` owns model calls independently of HTTP requests. An append semaphore serializes parts and terminal decisions per generation. Stop appends its native finish part with `close: true`, then interrupts and joins the provider outside the semaphore. Finish and stop have one winner. Later stops succeed without changing stored bytes, including after the original service has forgotten the closed generation.

The contract package explicitly encodes Effect AI parts before Streamsy's JSON boundary. This preserves omission of native optional fields: `Schema.toCodecJson` would otherwise encode some `undefined` fields as `null`, which the native Effect AI schema rejects. Every browser batch is decoded with `Response.StreamPart()` before its checkpoint advances.

The browser contract owns the fold and atomic `{ offset, view, closed }` session checkpoint. Atom owns reads through atom dependencies; Foldkit owns reads through subscription dependencies. The shared transport cancels on Effect interruption. It waits for a decoded closed batch because the published client's `closed` promise can resolve before consumption of the final response body.

Run the four packages' checks:

```sh
bun run --cwd examples/durable-response test
bun run --cwd examples/durable-response-contract test
bun run --cwd examples/durable-response-atom test
bun run --cwd examples/durable-response-foldkit test
# Each also has build and typecheck scripts.
```

The HTTP proofs run the SQLite `Serve.layer` Bun host on ephemeral ports. They cover three-record disconnect/resume with identical encoded bytes and one model call, two readers plus a late reader seeing the same stopped record and closure, observed provider interruption, repeated stop, closed resume, finish/stop races, and invalid/missing ids. Shared fold/storage and page lifecycle tests cover checkpoint restoration, interruption, subscription gating, terminal state, and malformed parts. Real browser smoke flows cover drop, reload, two readers, stop propagation, and repeated stop in both stacks.

This is a single-process demo without authentication. SQLite preserves recorded output across host restarts; an interrupted upstream model call cannot resume. Active chat probes and provider fibers are process-local. A surviving open log can still be replayed and explicitly stopped after restart. There is no recovery worker or multi-host writer coordination. Durable Object hosting, batching/access flags, real-provider configuration, CLI, AI SDK, and TanStack variants are outside this batch.
