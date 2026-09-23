# Memory server HTTP walkthrough

The smallest Streamsy server: a Bun HTTP edge backed by one process-local memory
store. Use it to try the Durable Streams protocol with `curl`.

## Run it

From the repository root:

```bash
bun install --frozen-lockfile
bun run build
bun run --cwd examples/memory-server start
```

`PORT` defaults to `1337`. The edge mounts streams directly below `/`, so the
walkthrough uses `http://localhost:1337/walkthrough/demo`.

## Walk through the protocol

Each command below is one line. Header names are case-insensitive, and offset
values are opaque: copy the value returned by the server.

1. Create an empty JSON stream.

   ```bash
   curl -i -X PUT http://localhost:1337/walkthrough/demo -H 'Content-Type: application/json'
   ```

   The tested response is `201 Created` with `Content-Type: application/json`, a
   `Location` header, and `Stream-Next-Offset: 0000000000000000_0000000000000000`.

2. Append one JSON message.

   ```bash
   curl -i -X POST http://localhost:1337/walkthrough/demo -H 'Content-Type: application/json' --data '{"type":"hello","n":1}'
   ```

   The response is `204 No Content` with
   `Stream-Next-Offset: 0000000000000001_0000000000000000`.

3. Catch up from the beginning.

   ```bash
   curl -i 'http://localhost:1337/walkthrough/demo?offset=-1'
   ```

   The response is `200 OK`, `Stream-Up-To-Date: true`, the current next offset,
   an `ETag`, and this body:

   ```text
   [{"type":"hello","n":1}]
   ```

4. Reuse that validator for a conditional catch-up read.

   ```bash
   curl -i 'http://localhost:1337/walkthrough/demo?offset=-1' -H 'If-None-Match: "L3dhbGt0aHJvdWdoL2RlbW8=:-1:0000000000000001_0000000000000000"'
   ```

   With no later append, the response is `304 Not Modified` with no body. Copy
   the exact `ETag` from step 3 if you use a different stream id.

5. Wait for a later append with a long poll.

   ```bash
   curl -i 'http://localhost:1337/walkthrough/demo?offset=0000000000000001_0000000000000000&live=long-poll'
   ```

   While that request waits, append `{"type":"hello","n":2}` with the step 2
   command. The waiting response is `200 OK`, includes `Stream-Cursor` and
   `Stream-Next-Offset: 0000000000000002_0000000000000000`, and returns:

   ```text
   [{"type":"hello","n":2}]
   ```

6. Follow the stream with server-sent events.

   ```bash
   curl -N 'http://localhost:1337/walkthrough/demo?offset=-1&live=sse'
   ```

   The response is `200 OK` with `Content-Type: text/event-stream`. It starts
   with a `data` event containing both messages, then a `control` event carrying
   `streamNextOffset`, `streamCursor`, and `upToDate: true`.

## How it works

`HttpRouter.serve(Http.routes())` runs with one `Streams.layerMemory()` and a
scoped Bun listener. The 60-second idle timeout allows a 30-second protocol long
poll to complete. Shutdown disposes the runtime, closing the listener and store.

## Verify

```bash
bun run --cwd examples/memory-server test:unit
bun run --cwd examples/memory-server typecheck
bun run --cwd examples/memory-server smoke:http
```

The offline smoke drives create, append, catch-up read, conditional `ETag`,
long-poll, and SSE flows against a child process. It then stops and restarts the
server and confirms the earlier stream returns `404 Not Found`.

## Limits

The store belongs to one server process. A restart starts with a fresh store, so
this demo is for local protocol exploration rather than durable data.

Guide: [streamsy.dev/docs/demos/memory-server](https://streamsy.dev/docs/demos/memory-server).
