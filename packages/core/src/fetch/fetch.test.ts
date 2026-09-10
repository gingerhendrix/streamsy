// oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch, effecttsgo/global-timers -- Bun tests own the real local HTTP boundary and its teardown.
import { expect, it } from "bun:test";
import { Clock, Effect, Exit, Fiber, Layer, ManagedRuntime } from "effect";
import { FetchHttpClient, HttpClient, HttpClientResponse } from "effect/unstable/http";
import { Streams, StreamsReader, StreamsWriter, StreamId, ZERO_OFFSET } from "../index.ts";
import { makeEdge } from "../http/edge.ts";
import * as Fetch from "./index.ts";
import * as Wire from "./wire.ts";

const id = StreamId.make("orders/nested");
const bytes = new TextEncoder().encode('[{"id":1},{"id":2}]');
const clock = Layer.effect(
  Clock.Clock,
  Effect.map(Clock.Clock, (live) => ({
    currentTimeMillis: Effect.succeed(1780000000000),
    currentTimeMillisUnsafe: () => 1780000000000,
    currentTimeNanos: live.currentTimeNanos,
    currentTimeNanosUnsafe: () => live.currentTimeNanosUnsafe(),
    monotonicTimeNanos: live.monotonicTimeNanos,
    monotonicTimeNanosUnsafe: () => live.monotonicTimeNanosUnsafe(),
    sleep: (duration) => live.sleep(duration),
  })),
);
const memory = () => Streams.layerMemory({ longPollTimeoutMs: 5 }).pipe(Layer.provideMerge(clock));
const fixture = Effect.gen(function* () {
  const reader = yield* StreamsReader;
  const writer = yield* StreamsWriter;
  const results = [];
  results.push(yield* reader.head(id));
  results.push(yield* writer.create(id, { contentType: "application/json", ttlSeconds: 60 }));
  results.push(yield* writer.create(id, { contentType: "application/json", ttlSeconds: 60 }));
  results.push(yield* writer.create(id, { contentType: "text/plain" }));
  results.push(yield* reader.head(id));
  results.push(
    yield* writer.append(id, {
      data: bytes,
      contentType: "application/json",
      expectedOffset: ZERO_OFFSET,
    }),
  );
  results.push(
    yield* writer.append(id, {
      data: bytes,
      contentType: "application/json",
      expectedOffset: ZERO_OFFSET,
    }),
  );
  results.push(yield* reader.read(id, { limit: 1 }));
  results.push(yield* reader.read(id));
  results.push(yield* writer.fork(StreamId.make("fork"), id));
  results.push(yield* reader.read(StreamId.make("fork")));
  const producer = { producerId: "p", producerEpoch: 0, producerSeq: 0 };
  results.push(
    yield* writer.append(id, { data: bytes, contentType: "application/json", producer }),
  );
  results.push(
    yield* writer.append(id, { data: bytes, contentType: "application/json", producer }),
  );
  results.push(
    yield* writer.append(id, {
      data: bytes,
      contentType: "application/json",
      producer: { ...producer, producerSeq: 2 },
    }),
  );
  results.push(
    yield* writer.append(id, {
      data: bytes,
      contentType: "application/json",
      producer: { ...producer, producerEpoch: 1, producerSeq: 1 },
    }),
  );
  results.push(
    yield* writer.append(id, {
      data: bytes,
      contentType: "application/json",
      producer: { ...producer, producerEpoch: 1 },
    }),
  );
  results.push(
    yield* writer.append(id, { data: bytes, contentType: "application/json", producer }),
  );
  results.push(yield* writer.append(id, { data: bytes, contentType: "text/plain" }));
  results.push(yield* writer.fork(StreamId.make("missing-fork"), StreamId.make("missing")));
  results.push(
    yield* writer.fork(StreamId.make("bounded-fork"), id, {
      forkOffset: "0000000000000001_0000000000000000",
      forkSubOffset: 0,
    }),
  );
  results.push(yield* reader.read(StreamId.make("bounded-fork")));
  results.push(
    yield* writer.create(StreamId.make("expiry"), { expiresAt: "2030-01-01T00:00:00.000Z" }),
  );
  results.push(yield* reader.head(StreamId.make("expiry")));
  const live = yield* reader.readNext(id, { offset: ZERO_OFFSET });
  if (live.status !== "not-supported") results.push({ ...live, cursor: "cursor" });
  const tail = yield* reader.head(id);
  if (tail.status !== "ok") return yield* Effect.die("missing fixture stream");
  const waited = yield* reader.readNext(id, { offset: tail.nextOffset });
  // Cursor jitter is deliberately nondeterministic; assert its shape separately.
  expect(waited.status).toBe("timeout");
  if (waited.status !== "not-supported") {
    expect(waited.cursor).toMatch(/^\d+$/);
    results.push({ ...waited, cursor: "cursor" });
  }
  results.push(
    yield* writer.append(id, {
      data: new Uint8Array(),
      contentType: "application/json",
      close: true,
    }),
  );
  results.push(yield* reader.head(id));
  results.push(yield* writer.append(id, { data: bytes, contentType: "application/json" }));
  results.push(yield* writer.remove(id));
  results.push(yield* reader.read(id));
  results.push(yield* reader.readNext(id, { offset: ZERO_OFFSET }));
  results.push(yield* writer.remove(id));
  return results;
});

it("all operation families preserve direct outcomes, metadata and exact message bytes over local HTTP", async () => {
  const edge = makeEdge({ pathPrefix: "/streams" }, memory());
  const server = Bun.serve({ port: 0, fetch: (request) => edge.handler(request) });
  try {
    const remote = Fetch.layer({
      baseUrl: `${server.url.href}streams`,
      capabilities: { expectedOffset: true, producer: true },
    }).pipe(Layer.provide(FetchHttpClient.layer));
    expect(
      JSON.parse(JSON.stringify(await Effect.runPromise(fixture.pipe(Effect.provide(remote))))),
    ).toEqual(
      JSON.parse(JSON.stringify(await Effect.runPromise(fixture.pipe(Effect.provide(memory()))))),
    );
  } finally {
    await server.stop(true);
    await edge.dispose();
  }
});

function mock(response: () => Response) {
  return Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) =>
      Effect.sync(() => HttpClientResponse.fromWeb(request, response())),
    ),
  );
}
// oxlint-disable-next-line anti-slop/no-object-parameters -- Fixtures deliberately include malformed wire objects for decoder rejection.
const outcome = (value: object, status = 200) =>
  new Response(null, {
    status,
    headers: { [Wire.resultHeader]: encodeURIComponent(JSON.stringify(value)) },
  });
for (const [label, response] of [
  ["missing representation", () => new Response(null)],
  ["missing offset", () => outcome({ status: "ok", contentType: "text/plain" })],
  ["bad offset", () => outcome({ status: "ok", contentType: "text/plain", nextOffset: "bad" })],
  [
    "invalid integer",
    () =>
      outcome({
        status: "ok",
        contentType: "text/plain",
        nextOffset: ZERO_OFFSET,
        ttlSeconds: 0.5,
      }),
  ],
  ["invalid JSON", () => new Response("{", { headers: { "content-type": Wire.format } })],
] as const) {
  it(`classifies ${label} as TransportFault`, async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* (yield* StreamsReader).head(id).pipe(Effect.flip);
      }).pipe(
        Effect.provide(
          Fetch.layer({ baseUrl: "http://localhost/streams" }).pipe(Layer.provide(mock(response))),
        ),
      ),
    );
    expect(result._tag).toBe("TransportFault");
  });
}
it("unknown CAS and producer support return values before any HTTP request", async () => {
  let requests = 0;
  const transport = Fetch.layer({ baseUrl: "http://localhost/streams" }).pipe(
    Layer.provide(
      mock(() => {
        requests++;
        return new Response(null);
      }),
    ),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      const writer = yield* StreamsWriter;
      expect(
        yield* writer.append(id, {
          data: bytes,
          contentType: "application/json",
          expectedOffset: ZERO_OFFSET,
        }),
      ).toEqual({ status: "not-supported", feature: "expected-offset" });
      expect(
        yield* writer.append(id, {
          data: bytes,
          contentType: "application/json",
          producer: { producerId: "p", producerEpoch: 0, producerSeq: 0 },
        }),
      ).toEqual({ status: "not-supported", feature: "producer" });
    }).pipe(Effect.provide(transport)),
  );
  expect(requests).toBe(0);
});
it("connection failure stays in the typed fault channel", async () => {
  const server = Bun.serve({ port: 0, fetch: () => new Response(null) });
  const baseUrl = `${server.url.href}streams`;
  await server.stop(true);
  const failure = await Effect.runPromise(
    Effect.gen(function* () {
      return yield* (yield* StreamsReader).head(id).pipe(Effect.flip);
    }).pipe(Effect.provide(Fetch.layer({ baseUrl }).pipe(Layer.provide(FetchHttpClient.layer)))),
  );
  expect(failure).toMatchObject({ _tag: "TransportFault", reason: "request" });
});
for (const phase of ["request", "body"] as const) {
  it(`interrupting ${phase} closes the owned request scope`, async () => {
    let aborted = false;
    let cancelled = false;
    const entered = Promise.withResolvers<void>();
    const client = HttpClient.make((request, _url, signal) => {
      signal.addEventListener(
        "abort",
        () => {
          aborted = true;
        },
        { once: true },
      );
      if (phase === "request")
        return Effect.sync(() => entered.resolve()).pipe(Effect.andThen(Effect.never));
      return Effect.sync(() =>
        HttpClientResponse.fromWeb(
          request,
          new Response(
            new ReadableStream({
              start() {
                entered.resolve();
              },
              cancel() {
                cancelled = true;
              },
            }),
            { headers: { "content-type": Wire.format } },
          ),
        ),
      );
    });
    const runtime = ManagedRuntime.make(
      Fetch.layer({ baseUrl: "http://localhost/streams" }).pipe(
        Layer.provide(Layer.succeed(HttpClient.HttpClient, client)),
      ),
    );
    const fiber = runtime.runFork(
      Effect.gen(function* () {
        return yield* (yield* StreamsReader).readNext(id, { offset: ZERO_OFFSET });
      }),
    );
    await entered.promise;
    await Effect.runPromise(Fiber.interrupt(fiber));
    const exit = await Effect.runPromise(Fiber.await(fiber));
    expect(Exit.isFailure(exit)).toBe(true);
    expect(aborted).toBe(true);
    if (phase === "body") expect(cancelled).toBe(true);
    await runtime.dispose();
  });
}
it("disposing the Layer interrupts active requests", async () => {
  let aborted = false;
  const entered = Promise.withResolvers<void>();
  const client = HttpClient.make((_request, _url, signal) =>
    Effect.sync(() => {
      signal.addEventListener("abort", () => {
        aborted = true;
      });
      entered.resolve();
    }).pipe(Effect.andThen(Effect.never)),
  );
  const runtime = ManagedRuntime.make(
    Fetch.layer({ baseUrl: "http://localhost/streams" }).pipe(
      Layer.provide(Layer.succeed(HttpClient.HttpClient, client)),
    ),
  );
  const fiber = runtime.runFork(
    Effect.gen(function* () {
      return yield* (yield* StreamsReader).head(id);
    }),
  );
  await entered.promise;
  await runtime.dispose();
  expect(aborted).toBe(true);
  expect(Exit.isFailure(await Effect.runPromise(Fiber.await(fiber)))).toBe(true);
});

for (const [label, response] of [
  [
    "invalid byte",
    () =>
      new Response(
        JSON.stringify({
          status: "ok",
          messages: [{ offset: ZERO_OFFSET, timestamp: 1, data: [256] }],
          nextOffset: ZERO_OFFSET,
          upToDate: true,
        }),
        { headers: { "content-type": Wire.format } },
      ),
  ],
  [
    "truncated body",
    () => new Response('{"status":"ok",', { headers: { "content-type": Wire.format } }),
  ],
  [
    "unreadable body",
    () =>
      new Response(
        new ReadableStream({
          start(controller) {
            controller.error(new Error("body failed"));
          },
        }),
        { headers: { "content-type": Wire.format } },
      ),
  ],
  [
    "impossible status",
    () =>
      new Response(
        JSON.stringify({ status: "ok", messages: [], nextOffset: ZERO_OFFSET, upToDate: true }),
        { status: 404, headers: { "content-type": Wire.format } },
      ),
  ],
  ["invalid content type", () => new Response("[]", { headers: { "content-type": "text/plain" } })],
  [
    "missing message timestamp",
    () =>
      new Response(
        JSON.stringify({
          status: "ok",
          messages: [{ offset: ZERO_OFFSET, data: [1] }],
          nextOffset: ZERO_OFFSET,
          upToDate: true,
        }),
        { headers: { "content-type": Wire.format } },
      ),
  ],
] as const) {
  it(`rejects ${label} during finite read`, async () => {
    const transport = Fetch.layer({ baseUrl: "http://localhost/streams" }).pipe(
      Layer.provide(mock(response)),
    );
    const failure = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* (yield* StreamsReader).read(id).pipe(Effect.flip);
      }).pipe(Effect.provide(transport)),
    );
    expect(failure._tag).toBe("TransportFault");
  });
}

// Ported behavioral cases from rename-http-client's stream-protocol-client-contract.
// Promise sessions are replaced by the current reader/writer Effects.
for (const mode of ["direct", "fetch"] as const) {
  for (const contentType of [
    "text/plain",
    "application/octet-stream",
    "application/json; charset=utf-8",
  ]) {
    it(`${mode} client conformance: ${contentType} resumes after acknowledgement and closes atomically`, async () => {
      const edge = makeEdge({ pathPrefix: "/streams" }, memory());
      const server = Bun.serve({ port: 0, fetch: (request) => edge.handler(request) });
      const transport =
        mode === "direct"
          ? memory()
          : Fetch.layer({ baseUrl: `${server.url.href}streams` }).pipe(
              Layer.provide(FetchHttpClient.layer),
            );
      try {
        await Effect.runPromise(
          Effect.gen(function* () {
            const reader = yield* StreamsReader;
            const writer = yield* StreamsWriter;
            const data = contentType.startsWith("application/json")
              ? new TextEncoder().encode('{"n":1}')
              : new Uint8Array([0, 1, 65]);
            expect((yield* writer.create(id, { contentType })).status).toBe("created");
            const empty = yield* reader.read(id);
            expect(empty).toMatchObject({ status: "ok", messages: [], upToDate: true });
            const first = yield* writer.append(id, { data, contentType });
            if (first.status !== "appended") return yield* Effect.die("Append not accepted");
            expect(yield* reader.read(id, { offset: first.offset })).toMatchObject({
              status: "ok",
              messages: [],
            });
            const final = yield* writer.append(id, { data, contentType, close: true });
            if (final.status !== "appended") return yield* Effect.die("Close not accepted");
            const read = yield* reader.read(id, { offset: first.offset });
            expect(read).toMatchObject({ status: "ok", nextOffset: final.offset, closed: true });
            if (read.status === "ok")
              expect(read.messages.map((message) => message.data)).toEqual([data]);
          }).pipe(Effect.provide(transport)),
        );
      } finally {
        await server.stop(true);
        await edge.dispose();
      }
      expect(server.pendingRequests).toBe(0);
    });
  }
}

for (const headers of [
  { "stream-ttl": "1.5" },
  { "stream-closed": "false" },
  { "stream-next-offset": "0000000000000009_0000000000000000" },
  { "content-type": "invalid" },
]) {
  it(`rejects contradictory HEAD headers ${JSON.stringify(headers)}`, async () => {
    const transport = Fetch.layer({ baseUrl: "http://localhost/streams" }).pipe(
      Layer.provide(
        mock(
          () =>
            new Response(null, {
              headers: {
                [Wire.resultHeader]: encodeURIComponent(
                  JSON.stringify({
                    status: "ok",
                    nextOffset: ZERO_OFFSET,
                    contentType: "text/plain",
                    ttlSeconds: 60,
                    closed: true,
                  }),
                ),
                "stream-next-offset": ZERO_OFFSET,
                "content-type": "text/plain",
                "stream-ttl": "60",
                "stream-closed": "true",
                ...headers,
              },
            }),
        ),
      ),
    );
    const fault = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* (yield* StreamsReader).head(id).pipe(Effect.flip);
      }).pipe(Effect.provide(transport)),
    );
    expect(fault).toMatchObject({ _tag: "TransportFault", reason: "response" });
  });
}

it("resolves prefixes and application headers without letting IDs escape", async () => {
  const seen: string[] = [];
  const client = HttpClient.make((request, url) =>
    Effect.sync(() => {
      seen.push(url.href);
      expect(request.headers.authorization).toBe("Bearer fixture");
      expect(request.headers.accept).toBe(Wire.format);
      return HttpClientResponse.fromWeb(request, new Response(null, { status: 404 }));
    }),
  );
  const transport = Fetch.layer({
    baseUrl: "https://example.test/api/streams/",
    headers: { authorization: "Bearer fixture", accept: "overridden" },
  }).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, client)));
  await Effect.runPromise(
    Effect.gen(function* () {
      const reader = yield* StreamsReader;
      yield* reader.head(id);
      for (const invalid of [
        "../escape",
        "/absolute",
        "a?query",
        "a#fragment",
        "a%2Fb",
        "a//b",
        "a b",
      ]) {
        expect(yield* reader.head(StreamId.make(invalid)).pipe(Effect.flip)).toMatchObject({
          _tag: "TransportFault",
          reason: "configuration",
        });
      }
    }).pipe(Effect.provide(transport)),
  );
  expect(seen).toEqual(["https://example.test/api/streams/orders/nested"]);
});
