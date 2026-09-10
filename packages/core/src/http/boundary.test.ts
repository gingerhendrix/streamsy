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
} from "../index.ts";
import { faultyStorage } from "../testing/fault-injection.ts";
import { makeEdge } from "./edge.ts";
import { read } from "./read.ts";

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
      let observed: import("../protocol/options.ts").ReadNextOptions | undefined;
      const reader = StreamsReader.of({
        head: () =>
          Effect.succeed({ closed: false, contentType: "text/plain", nextOffset: ZERO_OFFSET }),
        read: () => Effect.die("unexpected catch-up"),
        readNext: (_id, options) =>
          Effect.sync(() => {
            observed = options;
            return {
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

it("keeps direct zero-limit semantics while live reads return the entire available burst", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const writer = yield* StreamsWriter;
      const reader = yield* StreamsReader;
      const id = StreamId.make("burst");
      yield* writer.create(id, {
        contentType: "application/json",
        initialData: new TextEncoder().encode("[1,2,3]"),
      });
      const head = yield* reader.head(id);

      expect(yield* reader.read(id, { limit: 0 })).toMatchObject({
        messages: [],
        nextOffset: head.nextOffset,
        upToDate: true,
      });
      const live = yield* reader.readNext(id, { offset: ZERO_OFFSET });

      expect(live.messages).toHaveLength(3);
      return undefined;
    }).pipe(
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This test owns the memory runtime.
      Effect.provide(Streams.layerMemory()),
    ),
  ));
