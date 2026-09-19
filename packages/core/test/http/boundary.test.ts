// oxlint-disable effecttsgo/async-function -- Web boundary tests use requests and response body promises.
import { expect, it } from "bun:test";
import { Effect, Layer } from "effect";
import {
  Streams,
  Protocol,
  Memory,
  StreamsReader,
  StreamsWriter,
  StreamId,
  Storage,
  ZERO_OFFSET,
} from "@streamsy/core";
import { faultyStorage } from "../../src/testing/fault-injection.ts";
import { makeEdge } from "../../src/http/edge.ts";
import { read } from "../../src/http/read.ts";

it("rejects malformed cursor and producer tuple fields at ingress", async () => {
  const edge = makeEdge({}, Streams.layerMemory());
  try {
    await edge.handler(new Request("http://x/s", { method: "PUT" }));
    for (const cursor of [
      "",
      "NaN",
      "Infinity",
      "c1",
      "1x",
      "-1",
      "1.5",
      "01",
      "9007199254740992",
    ]) {
      const response = await edge.handler(
        new Request(`http://x/s?offset=-1&live=long-poll&cursor=${cursor}`),
      );
      expect(response.status).toBe(400);
      expect(await response.text()).toBe("Invalid cursor");
    }
    for (const field of ["producer-epoch", "producer-seq"]) {
      for (const value of ["-1", "1.5", "01", "NaN", "9007199254740992"]) {
        const response = await edge.handler(
          new Request("http://x/s", {
            method: "POST",
            headers: {
              "producer-id": "p",
              "producer-epoch": "0",
              "producer-seq": "0",
              [field]: value,
            },
            body: "x",
          }),
        );
        expect(response.status).toBe(400);
        expect(await response.text()).toBe("Invalid producer headers");
      }
    }
  } finally {
    await edge.dispose();
  }
});

for (const when of ["before", "after"] as const) {
  it(`maps ${when}-commit StorageFault to 500 without an opaque append retry`, async () => {
    const layer = Protocol.layer().pipe(
      Layer.provide(faultyStorage(Memory.layer(), { failOn: 2, when })),
    );
    const edge = makeEdge({}, layer);
    try {
      await edge.handler(
        new Request("http://x/s", { method: "PUT", headers: { "content-type": "text/plain" } }),
      );
      const failed = await edge.handler(
        new Request("http://x/s", {
          method: "POST",
          headers: { "content-type": "text/plain" },
          body: "once",
        }),
      );
      expect(failed.status).toBe(500);
      expect(await failed.text()).toBe("Internal server error");
      expect(failed.headers.get("x-content-type-options")).toBe("nosniff");
      const result = await edge.handler(new Request("http://x/s"));
      expect(await result.text()).toBe(when === "after" ? "once" : "");
    } finally {
      await edge.dispose();
    }
  });
}

it("keeps long-poll transport-neutral and forwards a valid cursor", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      let observed: import("../../src/protocol/options.ts").ReadNextOptions | undefined;
      const reader = StreamsReader.of({
        head: () =>
          Effect.succeed({ closed: false, contentType: "text/plain", nextOffset: ZERO_OFFSET }),
        read: () => Effect.die("unexpected catch-up"),
        readNext: (_id, options) =>
          Effect.sync(() => {
            observed = options;
            return {
              contentType: "text/plain",
              timedOut: true,
              closed: false,
              messages: [],
              nextOffset: ZERO_OFFSET,
              upToDate: true,
              cursor: "42",
            };
          }),
      });
      const response = yield* read(
        reader,
        StreamId.make("s"),
        new URL(`http://x/s?offset=${ZERO_OFFSET}&live=long-poll&cursor=12`),
        new Headers(),
        "private",
      );
      expect(observed).toEqual({ offset: ZERO_OFFSET, cursor: "12" });
      expect(response.status).toBe(204);
      if (!(response instanceof Response)) throw new Error("Expected empty Web response");
      expect(response.headers.get("stream-cursor")).toBe("42");
      expect(response.headers.get("cache-control")).toBe("no-store");
    }),
  ));

const inspectedLayer = Protocol.layer().pipe(Layer.provideMerge(Memory.layer()));

it("preserves close-only sequence lowering and unpersisted fresh tuple on an already closed stream", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const storage = yield* Storage;
      const writer = yield* StreamsWriter;
      const id = StreamId.make("s");
      yield* writer.create(id, { contentType: "text/plain" });
      yield* writer.append(id, {
        data: new TextEncoder().encode("x"),
        contentType: "text/plain",
        seq: "z",
      });
      expect(
        yield* writer.append(id, {
          data: new Uint8Array(),
          contentType: "other",
          seq: "a",
          close: true,
        }),
      ).toMatchObject({ _tag: "Appended", closed: true });
      const record = yield* storage.record(id);
      expect(record).toMatchObject({ _tag: "Some", value: { lifecycle: { lastSeq: "a" } } });
      const producer = { producerId: "fresh", producerEpoch: 0, producerSeq: 0 };
      expect(
        yield* writer.append(id, {
          data: new Uint8Array(),
          contentType: "other",
          close: true,
          producer,
        }),
      ).toMatchObject({ _tag: "Appended", closed: true });
      expect(
        yield* Effect.flip(
          writer.append(id, {
            data: new Uint8Array(),
            contentType: "other",
            close: true,
            producer: { ...producer, producerSeq: 1 },
          }),
        ),
      ).toMatchObject({ _tag: "ProducerGap", expectedSeq: 0, receivedSeq: 1 });
    }).pipe(
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This test is the runtime owner and provides its complete layer here.
      Effect.provide(inspectedLayer),
    ),
  ));

it("caps catch-up pages while live reads return the entire available burst", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const writer = yield* StreamsWriter;
      const reader = yield* StreamsReader;
      const id = StreamId.make("burst");
      yield* writer.create(id, {
        contentType: "application/json",
        initialData: new TextEncoder().encode("[1,2,3]"),
      });
      const first = yield* reader.read(id);
      expect(first.messages).toHaveLength(1);
      expect(first.upToDate).toBe(false);
      const second = yield* reader.read(id, { offset: first.nextOffset });
      expect(second.messages).toHaveLength(1);
      expect(second.nextOffset > first.nextOffset).toBe(true);
      const live = yield* reader.readNext(id, { offset: ZERO_OFFSET });

      expect(live.messages).toHaveLength(3);
      return undefined;
    }).pipe(
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This test owns the memory runtime.
      Effect.provide(Streams.layerMemory({ readLimit: 1 })),
    ),
  ));

it("HTTP catch-up omits up-to-date until the server page reaches the tail", async () => {
  const edge = makeEdge({}, Streams.layerMemory({ readLimit: 1 }));
  try {
    await edge.handler(
      new Request("http://example.test/pages", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: "[1,2]",
      }),
    );
    const first = await edge.handler(new Request("http://example.test/pages?offset=-1"));
    expect(await first.json()).toEqual([1]);
    expect(first.headers.has("stream-up-to-date")).toBe(false);
    const second = await edge.handler(
      new Request(`http://example.test/pages?offset=${first.headers.get("stream-next-offset")}`),
    );
    expect(await second.json()).toEqual([2]);
    expect(second.headers.get("stream-up-to-date")).toBe("true");
  } finally {
    await edge.dispose();
  }
});

it("the default catch-up page holds 1000 messages", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const writer = yield* StreamsWriter;
      const reader = yield* StreamsReader;
      const id = StreamId.make("default-page");
      yield* writer.create(id, {
        contentType: "application/json",
        initialData: new TextEncoder().encode(
          JSON.stringify(Array.from({ length: 1001 }, (_, n) => n)),
        ),
      });
      const first = yield* reader.read(id);
      expect(first.messages).toHaveLength(1000);
      expect(first.upToDate).toBe(false);
      const last = yield* reader.read(id, { offset: first.nextOffset });
      expect(last.messages).toHaveLength(1);
      expect(last.upToDate).toBe(true);
    }).pipe(
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This test owns the memory runtime.
      Effect.provide(Streams.layerMemory()),
    ),
  ));

it("SSE closes its body normally at the configured deadline", async () => {
  const edge = makeEdge({ sseDeadlineMs: 20 }, Streams.layerMemory());
  try {
    await edge.handler(new Request("http://example.test/deadline", { method: "PUT" }));
    const response = await edge.handler(
      new Request("http://example.test/deadline?offset=-1&live=sse"),
    );
    expect(response.status).toBe(200);
    const reader = response.body!.getReader();
    let chunks = 0;
    while (!(await reader.read()).done) chunks++;
    expect(await reader.closed).toBeUndefined();
    expect(chunks).toBeGreaterThan(0);
  } finally {
    await edge.dispose();
  }
});

it("GET, POST and DELETE use their protocol call without extra HEAD requests", async () => {
  let heads = 0;
  const readers = Layer.effect(
    StreamsReader,
    Effect.gen(function* () {
      const reader = yield* StreamsReader;
      return StreamsReader.of({
        ...reader,
        head: (id) => {
          heads++;
          return reader.head(id);
        },
      });
    }),
  ).pipe(Layer.provideMerge(Streams.layerMemory()));
  const edge = makeEdge({}, readers);
  try {
    await edge.handler(
      new Request("http://example.test/round-trips", {
        method: "PUT",
        headers: { "content-type": "text/plain" },
        body: "a",
      }),
    );
    for (const suffix of ["?offset=-1", "?offset=now", "?offset=-1&live=long-poll"]) {
      const response = await edge.handler(new Request(`http://example.test/round-trips${suffix}`));
      expect(response.status).toBe(200);
      expect(response.headers.get("content-type")).toBe("text/plain");
      await response.text();
    }
    expect(
      (
        await edge.handler(
          new Request("http://example.test/round-trips", {
            method: "POST",
            headers: { "content-type": "text/plain" },
            body: "b",
          }),
        )
      ).status,
    ).toBe(204);
    expect(
      (await edge.handler(new Request("http://example.test/round-trips", { method: "DELETE" })))
        .status,
    ).toBe(204);
    expect(heads).toBe(0);
  } finally {
    await edge.dispose();
  }
});
