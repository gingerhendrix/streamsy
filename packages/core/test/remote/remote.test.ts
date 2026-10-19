import { routes as protocolRoutes } from "../../src/protocol/index.ts";
// oxlint-disable effecttsgo/global-fetch, effecttsgo/global-timers -- Bun tests own the real local HTTP boundary and its teardown.
import { expect, it as baseIt } from "@effect/vitest";
import {
  Array,
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
import { FetchHttpClient, HttpClient, HttpClientResponse } from "effect/http";
import { TransportFault } from "../../src/fault.ts";
import { StreamAddress, StreamId, ZERO_OFFSET } from "@streamsy/core";
import { MemoryStorage } from "../../src/index.ts";
import { Streams, StreamsReader, StreamsWriter } from "../../src/protocol/index.ts";
import { HttpRouter } from "effect/http";
import { RemoteStreams } from "@streamsy/core/remote";

const id = StreamId.make("orders/nested");

const bytes = new TextEncoder().encode('[{"id":1},{"id":2}]');

const address = StreamAddress.json("orders", { schema: Schema.Struct({ id: Schema.Finite }) });

const notes = StreamAddress.bytes("notes", { contentType: "text/plain" });

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

const memory = () =>
  MemoryStorage.layerProtocol({ longPollTimeoutMs: 5 }).pipe(Layer.provideMerge(clock));

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
  Stream.runCollect(stream).pipe(Effect.map((collected) => Array.map(collected, summarize)));

const producer = { producerId: "p", producerEpoch: 0, producerSeq: 0 };

/**
 * The same program through the direct Layer and through the remote Layer. Every
 * result here is a value the public Durable Streams wire carries: status codes,
 * documented response headers, and the content-type-framed body. Cursors are
 * nondeterministic on both paths, so the fixture substitutes a fixed value.
 */
const fixture = Effect.gen(function* () {
  const writer = yield* StreamsWriter;
  const results: unknown[] = [];
  results.push(yield* Streams.create(address));
  results.push(yield* Streams.create(address));
  results.push(yield* Streams.append(address, [{ id: 1 }, { id: 2 }]));
  results.push(yield* Streams.append(address, [{ id: 3 }]));
  results.push(yield* batches(Streams.read(address)));
  results.push(yield* Streams.head(address));
  results.push(
    yield* Streams.session(address, { offset: ZERO_OFFSET }).pipe(
      Effect.map((result) => ({ ...result, cursor: "cursor" })),
    ),
  );
  results.push(yield* Streams.append(address, [{ id: 4 }], { producer }));
  results.push(yield* Streams.append(address, [{ id: 4 }], { producer }));
  const tail = yield* Streams.head(address);
  results.push(
    yield* Streams.session(address, { offset: tail.nextOffset }).pipe(
      Effect.map((result) => ({ ...result, cursor: "cursor" })),
    ),
  );
  results.push(
    yield* Effect.flip(Streams.append(address, [{ id: 5 }], { expectedOffset: ZERO_OFFSET })),
  );
  results.push(
    yield* Effect.flip(writer.append(address.id, { data: bytes, contentType: "text/plain" })),
  );
  results.push(yield* Streams.append(address, [], { close: true }));
  // @ts-expect-error an append needs items unless it closes; the wire still rejects one
  results.push(yield* Effect.flip(Streams.append(address, [])));
  results.push(yield* Effect.flip(Streams.append(address, [{ id: 6 }])));
  results.push(yield* batches(Streams.follow(address)));
  results.push(yield* Streams.remove(address));
  const failure = yield* Streams.read(address).pipe(Stream.runCollect, Effect.flip);

  if (!Predicate.isTagged(failure, "StreamNotFound"))
    return yield* Effect.die("Expected a removed stream");
  results.push({ tag: "StreamNotFound", id: failure.id });

  return results;
});

baseIt.live("every operation stream returns the direct results over local HTTP", () =>
  Effect.gen(function* () {
    const edge = yield* Effect.acquireRelease(
      Effect.sync(() =>
        HttpRouter.toWebHandler(
          protocolRoutes({ prefix: "/streams" }).pipe(Layer.provideMerge(memory())),
          { disableLogger: true },
        ),
      ),
      (acquired) => Effect.promise(() => acquired.dispose()),
    );

    const server = yield* Effect.acquireRelease(
      Effect.sync(() => Bun.serve({ port: 0, fetch: (request) => edge.handler(request) })),
      (acquired) => Effect.promise(() => acquired.stop(true)),
    );

    const remote = RemoteStreams.layerProtocol({
      baseUrl: `${server.url.href}streams`,
      capabilities: { expectedOffset: true, producer: true },
    }).pipe(Layer.provide(FetchHttpClient.layer));

    const json = Schema.fromJsonString(Schema.Array(Schema.Unknown));

    const normalize = (results: ReadonlyArray<unknown>) =>
      Schema.encodeEffect(json)(results).pipe(Effect.flatMap(Schema.decodeEffect(json)));

    const overHttp = yield* fixture.pipe(
      Effect.provideContext(yield* Layer.build(remote)),
      Effect.flatMap(normalize),
    );

    const direct = yield* fixture.pipe(
      Effect.provideContext(yield* Layer.build(memory())),
      Effect.flatMap(normalize),
    );

    expect(overHttp).toEqual(direct);
  }),
);

baseIt.live("merges text messages into one payload over the public wire", () =>
  Effect.gen(function* () {
    const edge = yield* Effect.acquireRelease(
      Effect.sync(() =>
        HttpRouter.toWebHandler(
          protocolRoutes({ prefix: "/streams" }).pipe(Layer.provideMerge(memory())),
          { disableLogger: true },
        ),
      ),
      (acquired) => Effect.promise(() => acquired.dispose()),
    );

    const server = yield* Effect.acquireRelease(
      Effect.sync(() => Bun.serve({ port: 0, fetch: (request) => edge.handler(request) })),
      (acquired) => Effect.promise(() => acquired.stop(true)),
    );

    const remote = RemoteStreams.layerProtocol({ baseUrl: `${server.url.href}streams` }).pipe(
      Layer.provide(FetchHttpClient.layer),
    );

    const program = Effect.gen(function* () {
      yield* Streams.create(notes);
      yield* Streams.append(notes, [new TextEncoder().encode("ab")]);
      yield* Streams.append(notes, [new TextEncoder().encode("cd")]);

      return yield* batches(Streams.read(notes));
    });

    const direct = yield* program.pipe(Effect.provideContext(yield* Layer.build(memory())));
    const overHttp = yield* program.pipe(Effect.provideContext(yield* Layer.build(remote)));
    expect(Array.map(direct, (batch) => Array.map(batch.items, asText))).toEqual([["ab", "cd"]]);
    // A text body is plain concatenation, so the batch carries one merged payload.
    expect(Array.map(overHttp, (batch) => Array.map(batch.items, asText))).toEqual([["abcd"]]);
  }),
);

function mock(response: () => Response) {
  return Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) =>
      Effect.sync(() => HttpClientResponse.fromWeb(request, response())),
    ),
  );
}

const against = (response: () => Response, capabilities: { readonly producer?: boolean } = {}) =>
  RemoteStreams.layerProtocol({ baseUrl: "http://localhost/streams", capabilities }).pipe(
    Layer.provide(mock(response)),
  );

const headOver = (response: () => Response) =>
  Effect.gen(function* () {
    return yield* Effect.gen(function* () {
      return yield* (yield* StreamsReader).head(id);
    }).pipe(Effect.provideContext(yield* Layer.build(against(response))));
  });

const headFailure = (response: () => Response) =>
  Effect.gen(function* () {
    return yield* Effect.gen(function* () {
      return yield* (yield* StreamsReader).head(id);
    }).pipe(Effect.flip, Effect.provideContext(yield* Layer.build(against(response))));
  });

const readOver = (response: () => Response) =>
  Effect.gen(function* () {
    return yield* Effect.gen(function* () {
      return yield* (yield* StreamsReader).read(id);
    }).pipe(Effect.provideContext(yield* Layer.build(against(response))));
  });

const readNextOver = (response: () => Response) =>
  Effect.gen(function* () {
    return yield* Effect.gen(function* () {
      return yield* (yield* StreamsReader).readNext(id, { offset: ZERO_OFFSET });
    }).pipe(Effect.provideContext(yield* Layer.build(against(response))));
  });

const appendFailure = (response: () => Response) =>
  Effect.gen(function* () {
    return yield* Effect.gen(function* () {
      return yield* (yield* StreamsWriter).append(id, {
        data: bytes,
        contentType: "application/json",
      });
    }).pipe(Effect.flip, Effect.provideContext(yield* Layer.build(against(response))));
  });

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
  baseIt.live(`rejects ${label} on head as a response fault`, () =>
    Effect.gen(function* () {
      expect(yield* headFailure(response)).toMatchObject({
        _tag: "TransportFault",
        reason: "response",
      });
    }),
  );
}

baseIt.live("reads the head outcome from standard headers", () =>
  Effect.gen(function* () {
    expect(
      yield* headOver(
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
      contentType: "text/plain",
      nextOffset: ZERO_OFFSET,
      ttlSeconds: 60,
      expiresAt: "2030-01-01T00:00:00.000Z",
      closed: true,
    });
  }),
);

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
  baseIt.live(`rejects ${label} during finite read`, () =>
    Effect.gen(function* () {
      expect(yield* Effect.flip(readOver(response))).toMatchObject({
        _tag: "TransportFault",
      });
    }),
  );
}

baseIt.live("splits a JSON read body into one payload per message", () =>
  Effect.gen(function* () {
    const result = yield* readOver(
      () =>
        new Response('[{"a":1},{"b":2}]', {
          headers: {
            "content-type": "application/json",
            "stream-next-offset": ZERO_OFFSET,
            "stream-up-to-date": "true",
          },
        }),
    );

    expect(result).toBeDefined();
    expect(Array.map(result.messages, (message) => asText(message.data))).toEqual([
      '{"a":1}',
      '{"b":2}',
    ]);
    expect(result.upToDate).toBe(true);
  }),
);

baseIt.live("keeps a non-JSON read body as one merged payload", () =>
  Effect.gen(function* () {
    const result = yield* readOver(
      () =>
        new Response("abcd", {
          headers: { "content-type": "text/plain", "stream-next-offset": ZERO_OFFSET },
        }),
    );

    expect(result).toBeDefined();
    expect(Array.map(result.messages, (message) => asText(message.data))).toEqual(["abcd"]);
    expect(result.upToDate).toBe(false);
  }),
);

baseIt.live("reads an empty long poll as a timeout", () =>
  Effect.gen(function* () {
    expect(
      yield* readNextOver(
        () =>
          new Response(null, {
            status: 204,
            headers: {
              "stream-next-offset": ZERO_OFFSET,
              "stream-up-to-date": "true",
              "stream-cursor": "7",
              "content-type": "text/plain",
            },
          }),
      ),
    ).toEqual({
      contentType: "text/plain",
      timedOut: true,
      messages: [],
      nextOffset: ZERO_OFFSET,
      upToDate: true,
      cursor: "7",
      closed: false,
    });
  }),
);

for (const [label, response] of [
  ["stale epoch without a current epoch", () => new Response(null, { status: 403 })],
] as const) {
  baseIt.live(`rejects ${label} on append as a response fault`, () =>
    Effect.gen(function* () {
      expect(yield* appendFailure(response)).toMatchObject({
        _tag: "TransportFault",
        reason: "response",
      });
    }),
  );
}

baseIt.live("keeps a bare append 400 generic without classifying its wording", () =>
  Effect.gen(function* () {
    const appended = yield* Effect.gen(function* () {
      return yield* (yield* StreamsWriter).append(id, {
        data: bytes,
        contentType: "application/json",
        producer: { producerId: "p", producerEpoch: 2, producerSeq: 1 },
      });
    }).pipe(
      Effect.flip,
      Effect.provideContext(
        yield* Layer.build(
          against(() => new Response("New epoch must start at seq=0", { status: 400 }), {
            producer: true,
          }),
        ),
      ),
    );

    expect(appended).toMatchObject({
      _tag: "InvalidAppendRequest",
      id,
      message: "New epoch must start at seq=0",
    });
  }),
);

const probe = (transport: Layer.Layer<StreamsReader | StreamsWriter, TransportFault>) =>
  Effect.gen(function* () {
    return yield* Effect.gen(function* () {
      return yield* (yield* StreamsReader).head(id);
    }).pipe(Effect.flip, Effect.provideContext(yield* Layer.build(transport)));
  });

baseIt.live("sends no Streamsy-specific request headers", () =>
  Effect.gen(function* () {
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

    yield* probe(
      RemoteStreams.layerProtocol({ baseUrl: "http://localhost/streams" }).pipe(
        Layer.provide(capture),
      ),
    );
    expect(accept).toBeUndefined();
    expect(cacheControl).toBe("no-cache");
    yield* probe(
      RemoteStreams.layerProtocol({
        baseUrl: "http://localhost/streams",
        headers: { accept: "application/json" },
      }).pipe(Layer.provide(capture)),
    );
    expect(accept).toBe("application/json");
  }),
);

baseIt.live("unknown CAS and producer support fail before any HTTP request", () =>
  Effect.gen(function* () {
    let requests = 0;

    const transport = RemoteStreams.layerProtocol({ baseUrl: "http://localhost/streams" }).pipe(
      Layer.provide(
        mock(() => {
          requests++;

          return new Response(null);
        }),
      ),
    );

    yield* Effect.gen(function* () {
      const writer = yield* StreamsWriter;
      expect(
        yield* Effect.flip(
          writer.append(id, {
            data: bytes,
            contentType: "application/json",
            expectedOffset: ZERO_OFFSET,
          }),
        ),
      ).toMatchObject({ _tag: "NotSupported", feature: "expected-offset" });
      expect(
        yield* Effect.flip(
          writer.append(id, {
            data: bytes,
            contentType: "application/json",
            producer: { producerId: "p", producerEpoch: 0, producerSeq: 0 },
          }),
        ),
      ).toMatchObject({ _tag: "NotSupported", feature: "producer" });
    }).pipe(Effect.provideContext(yield* Layer.build(transport)));
    expect(requests).toBe(0);
  }),
);

baseIt.live("connection failure stays in the typed fault channel", () =>
  Effect.gen(function* () {
    const server = yield* Effect.acquireRelease(
      Effect.sync(() => Bun.serve({ port: 0, fetch: () => new Response(null) })),
      (acquired) => Effect.promise(() => acquired.stop(true)),
    );

    const baseUrl = `${server.url.href}streams`;
    yield* Effect.promise(() => server.stop(true));

    const failure = yield* Effect.gen(function* () {
      return yield* (yield* StreamsReader).head(id).pipe(Effect.flip);
    }).pipe(
      Effect.provideContext(
        yield* Layer.build(
          RemoteStreams.layerProtocol({ baseUrl }).pipe(Layer.provide(FetchHttpClient.layer)),
        ),
      ),
    );

    expect(failure).toMatchObject({ _tag: "TransportFault", reason: "request" });
  }),
);

for (const phase of ["request", "body"] as const) {
  baseIt.live(`interrupting ${phase} closes the owned request scope`, () =>
    Effect.gen(function* () {
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

      const runtime = yield* Effect.acquireRelease(
        Effect.sync(() =>
          ManagedRuntime.make(
            RemoteStreams.layerProtocol({ baseUrl: "http://localhost/streams" }).pipe(
              Layer.provide(Layer.succeed(HttpClient.HttpClient, client)),
            ),
          ),
        ),
        (acquired) => Effect.promise(() => acquired.dispose()),
      );

      const fiber = runtime.runFork(
        Effect.gen(function* () {
          return yield* (yield* StreamsReader).readNext(id, { offset: ZERO_OFFSET });
        }),
      );

      yield* Effect.promise(() => entered.promise);
      yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);
      expect(Exit.isFailure(exit)).toBe(true);
      expect(aborted).toBe(true);

      if (phase === "body") expect(cancelled).toBe(true);
      yield* Effect.promise(() => runtime.dispose());
    }),
  );
}

baseIt.live("disposing the Layer interrupts active requests", () =>
  Effect.gen(function* () {
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

    const runtime = yield* Effect.acquireRelease(
      Effect.sync(() =>
        ManagedRuntime.make(
          RemoteStreams.layerProtocol({ baseUrl: "http://localhost/streams" }).pipe(
            Layer.provide(Layer.succeed(HttpClient.HttpClient, client)),
          ),
        ),
      ),
      (acquired) => Effect.promise(() => acquired.dispose()),
    );

    const fiber = runtime.runFork(
      Effect.gen(function* () {
        return yield* (yield* StreamsReader).head(id);
      }),
    );

    yield* Effect.promise(() => entered.promise);
    yield* Effect.promise(() => runtime.dispose());
    expect(aborted).toBe(true);
    expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true);
  }),
);

// Ported behavioral cases from rename-http-client's stream-protocol-client-contract.
// Promise sessions are replaced by the current reader/writer Effects.
for (const mode of ["direct", "remote"] as const) {
  for (const contentType of [
    "text/plain",
    "application/octet-stream",
    "application/json; charset=utf-8",
  ]) {
    baseIt.live(
      `${mode} client conformance: ${contentType} resumes after acknowledgement and closes atomically`,
      () =>
        Effect.gen(function* () {
          const stoppedServer = yield* Effect.scoped(
            Effect.gen(function* () {
              const edge = yield* Effect.acquireRelease(
                Effect.sync(() =>
                  HttpRouter.toWebHandler(
                    protocolRoutes({ prefix: "/streams" }).pipe(Layer.provideMerge(memory())),
                    { disableLogger: true },
                  ),
                ),
                (acquired) => Effect.promise(() => acquired.dispose()),
              );

              const server = yield* Effect.acquireRelease(
                Effect.sync(() =>
                  Bun.serve({ port: 0, fetch: (request) => edge.handler(request) }),
                ),
                (acquired) => Effect.promise(() => acquired.stop(true)),
              );

              const modeTransport =
                mode === "direct"
                  ? memory()
                  : RemoteStreams.layerProtocol({ baseUrl: `${server.url.href}streams` }).pipe(
                      Layer.provide(FetchHttpClient.layer),
                    );

              yield* Effect.gen(function* () {
                const reader = yield* StreamsReader;
                const writer = yield* StreamsWriter;

                const data = contentType.startsWith("application/json")
                  ? new TextEncoder().encode('{"n":1}')
                  : new Uint8Array([0, 1, 65]);

                expect((yield* writer.create(id, { contentType }))._tag).toBe("Created");
                const empty = yield* reader.read(id);
                expect(empty).toMatchObject({ messages: [], upToDate: true });
                const first = yield* writer.append(id, { data, contentType });
                expect(first._tag).toBe("Appended");
                expect(yield* reader.read(id, { offset: first.offset })).toMatchObject({
                  messages: [],
                });
                const final = yield* writer.append(id, { data, contentType, close: true });
                expect(final._tag).toBe("Appended");
                const read = yield* reader.read(id, { offset: first.offset });
                expect(read).toMatchObject({ nextOffset: final.offset, closed: true });
                expect(Array.map(read.messages, (message) => message.data)).toEqual([data]);
              }).pipe(Effect.provideContext(yield* Layer.build(modeTransport)));

              return server;
            }),
          );

          expect(stoppedServer.pendingRequests).toBe(0);
        }),
    );
  }
}

baseIt.live("resolves prefixes and application headers without letting IDs escape", () =>
  Effect.gen(function* () {
    const seen: string[] = [];

    const client = HttpClient.make((request, url) =>
      Effect.sync(() => {
        seen.push(url.href);
        expect(request.headers.authorization).toBe("Bearer fixture");

        return HttpClientResponse.fromWeb(request, new Response(null, { status: 404 }));
      }),
    );

    const transport = RemoteStreams.layerProtocol({
      baseUrl: "https://example.test/api/streams/",
      headers: { authorization: "Bearer fixture" },
    }).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, client)));

    yield* Effect.gen(function* () {
      const reader = yield* StreamsReader;
      expect((yield* Effect.flip(reader.head(id)))._tag).toBe("StreamNotFound");

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
    }).pipe(Effect.provideContext(yield* Layer.build(transport)));
    expect(seen).toEqual(["https://example.test/api/streams/orders/nested"]);
  }),
);

baseIt.live("keeps arbitrary append conflict bodies as typed conflicts", () =>
  Effect.gen(function* () {
    for (const message of ["Content-Type mismatch", "Sequence conflict", "Some other conflict"]) {
      expect(yield* appendFailure(() => new Response(message, { status: 409 }))).toMatchObject({
        _tag: "AppendConflict",
        id,
        message,
      });
    }
  }),
);

baseIt.live("keeps arbitrary append bad-request bodies as typed request errors", () =>
  Effect.gen(function* () {
    expect(
      yield* appendFailure(() => new Response("Empty body not allowed", { status: 400 })),
    ).toMatchObject({ _tag: "InvalidAppendRequest", id, message: "Empty body not allowed" });
  }),
);

for (const retry of [false, true]) {
  baseIt.live(
    `decodes a close-only producer append${retry ? " retried after close" : " on an open stream"}`,
    () =>
      Effect.gen(function* () {
        const edge = yield* Effect.acquireRelease(
          Effect.sync(() =>
            HttpRouter.toWebHandler(protocolRoutes({}).pipe(Layer.provideMerge(memory())), {
              disableLogger: true,
            }),
          ),
          (acquired) => Effect.promise(() => acquired.dispose()),
        );

        const server = yield* Effect.acquireRelease(
          Effect.sync(() => Bun.serve({ port: 0, fetch: (request) => edge.handler(request) })),
          (acquired) => Effect.promise(() => acquired.stop(true)),
        );

        const remote = RemoteStreams.layerProtocol({
          baseUrl: server.url.href,
          capabilities: { producer: true },
        }).pipe(Layer.provide(FetchHttpClient.layer));

        yield* Effect.gen(function* () {
          yield* Streams.create(notes);
          yield* Streams.append(notes, [new TextEncoder().encode("a")], { producer });
          const close = { producer: { ...producer, producerSeq: 1 }, close: true as const };

          if (retry) {
            yield* Streams.append(notes, [], close);
            expect((yield* Streams.append(notes, [], close))._tag).toBe("Duplicate");
            close.producer.producerSeq = 2;
            yield* Streams.append(notes, [], close);
          }

          const result = yield* Streams.append(notes, [], close);
          expect(result).toMatchObject({ _tag: retry ? "Appended" : "Duplicate", closed: true });
          expect(result.producerEpoch).toBe(retry ? undefined : 0);
          expect(result.producerSeq).toBe(retry ? undefined : 1);
        }).pipe(Effect.provideContext(yield* Layer.build(remote)));
      }),
  );
}
