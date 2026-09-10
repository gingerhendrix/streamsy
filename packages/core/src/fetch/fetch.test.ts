// oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch, effecttsgo/global-timers -- Bun tests own the real local HTTP boundary and its teardown.
import { expect, it } from "bun:test";
import {
  Clock,
  Effect,
  Exit,
  Fiber,
  Layer,
  ManagedRuntime,
  Predicate,
  Schema,
  Stream,
} from "effect";
import { FetchHttpClient, HttpClient, HttpClientResponse } from "effect/unstable/http";
import { TransportFault } from "../fault.ts";
import {
  StreamRef,
  Streams,
  StreamsReader,
  StreamsWriter,
  StreamId,
  ZERO_OFFSET,
} from "../index.ts";
import { makeEdge } from "../http/edge.ts";
import * as Fetch from "./index.ts";

const id = StreamId.make("orders/nested");
const bytes = new TextEncoder().encode('[{"id":1},{"id":2}]');
const ref = StreamRef.json("orders", { schema: Schema.Struct({ id: Schema.Finite }) });
const notes = StreamRef.bytes("notes", { contentType: "text/plain" });
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
const decoder = new TextDecoder();
const asText = (item: Uint8Array) => decoder.decode(item);

interface Batch<A> {
  readonly items: ReadonlyArray<A>;
  readonly nextOffset: string;
  readonly upToDate: boolean;
  readonly closed: boolean;
}
const summarize = <A>(batch: Batch<A>) => ({
  items: batch.items,
  nextOffset: batch.nextOffset,
  upToDate: batch.upToDate,
  closed: batch.closed,
});
const batches = <A, E, R>(stream: Stream.Stream<Batch<A>, E, R>) =>
  Stream.runCollect(stream).pipe(Effect.map((collected) => collected.map(summarize)));

const producer = { producerId: "p", producerEpoch: 0, producerSeq: 0 };

/**
 * The same program through the direct Layer and through the fetch Layer. Every
 * result here is a value the public Durable Streams wire carries: status codes,
 * documented response headers, and the content-type-framed body. Cursors are
 * nondeterministic on both paths, so the fixture substitutes a fixed value.
 */
const fixture = Effect.gen(function* () {
  const writer = yield* StreamsWriter;
  const results: unknown[] = [];
  results.push(yield* Streams.create(ref));
  results.push(yield* Streams.create(ref));
  results.push(yield* Streams.append(ref, [{ id: 1 }, { id: 2 }]));
  results.push(yield* Streams.append(ref, [{ id: 3 }]));
  results.push(yield* batches(Streams.read(ref)));
  results.push(yield* Streams.head(ref));
  results.push(
    yield* Streams.session(ref, { offset: ZERO_OFFSET }).pipe(
      Effect.map((result) => ({ ...result, cursor: "cursor" })),
    ),
  );
  results.push(yield* Streams.append(ref, [{ id: 4 }], { producer }));
  results.push(yield* Streams.append(ref, [{ id: 4 }], { producer }));
  const tail = yield* Streams.head(ref);
  if (tail.status !== "ok") return yield* Effect.die("missing fixture stream");
  results.push(
    yield* Streams.session(ref, { offset: tail.nextOffset }).pipe(
      Effect.map((result) => ({ ...result, cursor: "cursor" })),
    ),
  );
  results.push(yield* Streams.append(ref, [{ id: 5 }], { expectedOffset: ZERO_OFFSET }));
  results.push(yield* writer.append(ref.id, { data: bytes, contentType: "text/plain" }));
  results.push(yield* Streams.append(ref, [], { close: true }));
  results.push(yield* Streams.append(ref, [{ id: 6 }]));
  results.push(yield* batches(Streams.follow(ref)));
  results.push(yield* Streams.remove(ref));
  const failure = yield* Streams.read(ref).pipe(Stream.runCollect, Effect.flip);
  if (!Predicate.isTagged(failure, "StreamUnavailable"))
    return yield* Effect.die("Expected a removed stream");
  results.push({ tag: "StreamUnavailable", ref: failure.ref, status: failure.status });
  return results;
});

it("every operation family returns the direct results over local HTTP", async () => {
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

it("merges text messages into one payload over the public wire", async () => {
  const edge = makeEdge({ pathPrefix: "/streams" }, memory());
  const server = Bun.serve({ port: 0, fetch: (request) => edge.handler(request) });
  try {
    const remote = Fetch.layer({ baseUrl: `${server.url.href}streams` }).pipe(
      Layer.provide(FetchHttpClient.layer),
    );
    const program = Effect.gen(function* () {
      yield* Streams.create(notes);
      yield* Streams.append(notes, [new TextEncoder().encode("ab")]);
      yield* Streams.append(notes, [new TextEncoder().encode("cd")]);
      return yield* batches(Streams.read(notes));
    });
    const direct = await Effect.runPromise(program.pipe(Effect.provide(memory())));
    const overHttp = await Effect.runPromise(program.pipe(Effect.provide(remote)));
    expect(direct.map((batch) => batch.items.map(asText))).toEqual([["ab", "cd"]]);
    // A text body is plain concatenation, so the batch carries one merged payload.
    expect(overHttp.map((batch) => batch.items.map(asText))).toEqual([["abcd"]]);
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
const against = (response: () => Response, capabilities: { readonly producer?: boolean } = {}) =>
  Fetch.layer({ baseUrl: "http://localhost/streams", capabilities }).pipe(
    Layer.provide(mock(response)),
  );
const headOver = (response: () => Response) =>
  Effect.runPromise(
    Effect.gen(function* () {
      return yield* (yield* StreamsReader).head(id);
    }).pipe(Effect.provide(against(response))),
  );
const headFailure = (response: () => Response) =>
  Effect.runPromise(
    Effect.gen(function* () {
      return yield* (yield* StreamsReader).head(id);
    }).pipe(Effect.flip, Effect.provide(against(response))),
  );
const readOver = (response: () => Response) =>
  Effect.runPromise(
    Effect.gen(function* () {
      return yield* (yield* StreamsReader).read(id);
    }).pipe(Effect.provide(against(response))),
  );
const readNextOver = (response: () => Response) =>
  Effect.runPromise(
    Effect.gen(function* () {
      return yield* (yield* StreamsReader).readNext(id, { offset: ZERO_OFFSET });
    }).pipe(Effect.provide(against(response))),
  );
const appendFailure = (response: () => Response) =>
  Effect.runPromise(
    Effect.gen(function* () {
      return yield* (yield* StreamsWriter).append(id, {
        data: bytes,
        contentType: "application/json",
      });
    }).pipe(Effect.flip, Effect.provide(against(response))),
  );

for (const [label, response] of [
  ["missing offset", () => new Response(null, { headers: { "content-type": "text/plain" } })],
  [
    "invalid offset",
    () =>
      new Response(null, {
        headers: { "content-type": "text/plain", "stream-next-offset": "bad" },
      }),
  ],
  [
    "invalid integer",
    () =>
      new Response(null, {
        headers: {
          "content-type": "text/plain",
          "stream-next-offset": ZERO_OFFSET,
          "stream-ttl": "1.5",
        },
      }),
  ],
  [
    "invalid closed flag",
    () =>
      new Response(null, {
        headers: {
          "content-type": "text/plain",
          "stream-next-offset": ZERO_OFFSET,
          "stream-closed": "maybe",
        },
      }),
  ],
  [
    "invalid content type",
    () =>
      new Response(null, {
        headers: { "content-type": "invalid", "stream-next-offset": ZERO_OFFSET },
      }),
  ],
  ["unexpected status", () => new Response("something else", { status: 500 })],
] as const) {
  it(`rejects ${label} on head as a response fault`, async () => {
    expect(await headFailure(response)).toMatchObject({
      _tag: "TransportFault",
      reason: "response",
    });
  });
}

it("reads the head outcome from standard headers", async () => {
  expect(
    await headOver(
      () =>
        new Response(null, {
          headers: {
            "content-type": "text/plain",
            "stream-next-offset": ZERO_OFFSET,
            "stream-ttl": "60",
            "stream-expires-at": "2030-01-01T00:00:00.000Z",
            "stream-closed": "true",
          },
        }),
    ),
  ).toEqual({
    status: "ok",
    contentType: "text/plain",
    nextOffset: ZERO_OFFSET,
    ttlSeconds: 60,
    expiresAt: "2030-01-01T00:00:00.000Z",
    closed: true,
  });
});

for (const [label, response] of [
  [
    "truncated body",
    () => new Response('{"status":', { headers: { "content-type": "application/json" } }),
  ],
  [
    "body that is not an array",
    () => new Response('{"a":1}', { headers: { "content-type": "application/json" } }),
  ],
  [
    "missing content type",
    () => new Response("data", { headers: { "stream-next-offset": ZERO_OFFSET } }),
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
        { headers: { "content-type": "text/plain" } },
      ),
  ],
  ["unexpected status", () => new Response("[]", { status: 500 })],
] as const) {
  it(`rejects ${label} during finite read`, async () => {
    expect(
      await readOver(response).then(
        () => "resolved",
        (failure) => failure,
      ),
    ).toMatchObject({
      _tag: "TransportFault",
    });
  });
}

it("splits a JSON read body into one payload per message", async () => {
  const result = await readOver(
    () =>
      new Response('[{"a":1},{"b":2}]', {
        headers: {
          "content-type": "application/json",
          "stream-next-offset": ZERO_OFFSET,
          "stream-up-to-date": "true",
        },
      }),
  );
  expect(result.status).toBe("ok");
  if (result.status !== "ok") return;
  expect(result.messages.map((message) => asText(message.data))).toEqual(['{"a":1}', '{"b":2}']);
  expect(result.upToDate).toBe(true);
});

it("keeps a non-JSON read body as one merged payload", async () => {
  const result = await readOver(
    () =>
      new Response("abcd", {
        headers: { "content-type": "text/plain", "stream-next-offset": ZERO_OFFSET },
      }),
  );
  expect(result.status).toBe("ok");
  if (result.status !== "ok") return;
  expect(result.messages.map((message) => asText(message.data))).toEqual(["abcd"]);
  expect(result.upToDate).toBe(false);
});

it("reads an empty long poll as a timeout", async () => {
  expect(
    await readNextOver(
      () =>
        new Response(null, {
          status: 204,
          headers: {
            "stream-next-offset": ZERO_OFFSET,
            "stream-up-to-date": "true",
            "stream-cursor": "7",
          },
        }),
    ),
  ).toEqual({
    status: "timeout",
    messages: [],
    nextOffset: ZERO_OFFSET,
    upToDate: true,
    cursor: "7",
    closed: false,
  });
});

for (const [label, response] of [
  ["unclassified conflict", () => new Response("Some other conflict", { status: 409 })],
  ["stale epoch without a current epoch", () => new Response(null, { status: 403 })],
  ["unrecognized rejection", () => new Response("Empty body not allowed", { status: 400 })],
] as const) {
  it(`rejects ${label} on append as a response fault`, async () => {
    expect(await appendFailure(response)).toMatchObject({
      _tag: "TransportFault",
      reason: "response",
    });
  });
}

it("classifies the new-epoch sequence rejection from its plain 400 body", async () => {
  const appended = await Effect.runPromise(
    Effect.gen(function* () {
      return yield* (yield* StreamsWriter).append(id, {
        data: bytes,
        contentType: "application/json",
        producer: { producerId: "p", producerEpoch: 2, producerSeq: 1 },
      });
    }).pipe(
      Effect.provide(
        against(() => new Response("New epoch must start at seq=0", { status: 400 }), {
          producer: true,
        }),
      ),
    ),
  );
  expect(appended).toEqual({ status: "invalid-epoch-seq" });
});

it("sends no Streamsy-specific request headers", async () => {
  let accept: string | undefined = "unset";
  let cacheControl: string | undefined;
  const capture = Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) =>
      Effect.sync(() => {
        accept = request.headers.accept;
        cacheControl = request.headers["cache-control"];
        return HttpClientResponse.fromWeb(request, new Response(null, { status: 404 }));
      }),
    ),
  );
  const probe = (transport: Layer.Layer<StreamsReader | StreamsWriter, TransportFault>) =>
    Effect.runPromise(
      Effect.gen(function* () {
        return yield* (yield* StreamsReader).head(id);
      }).pipe(Effect.provide(transport)),
    );
  await probe(Fetch.layer({ baseUrl: "http://localhost/streams" }).pipe(Layer.provide(capture)));
  expect(accept).toBeUndefined();
  expect(cacheControl).toBe("no-cache");
  await probe(
    Fetch.layer({
      baseUrl: "http://localhost/streams",
      headers: { accept: "application/json" },
    }).pipe(Layer.provide(capture)),
  );
  expect(accept).toBe("application/json");
});

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
            { headers: { "content-type": "text/plain", "stream-next-offset": ZERO_OFFSET } },
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
      const modeTransport =
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
          }).pipe(Effect.provide(modeTransport)),
        );
      } finally {
        await server.stop(true);
        await edge.dispose();
      }
      expect(server.pendingRequests).toBe(0);
    });
  }
}

it("resolves prefixes and application headers without letting IDs escape", async () => {
  const seen: string[] = [];
  const client = HttpClient.make((request, url) =>
    Effect.sync(() => {
      seen.push(url.href);
      expect(request.headers.authorization).toBe("Bearer fixture");
      return HttpClientResponse.fromWeb(request, new Response(null, { status: 404 }));
    }),
  );
  const transport = Fetch.layer({
    baseUrl: "https://example.test/api/streams/",
    headers: { authorization: "Bearer fixture" },
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
