import { expect, it } from "vitest";
import { Effect, Exit, Fiber, Layer, Random, Stream } from "effect";
import { TestClock } from "effect/testing";
import { jitterDeadline, sse } from "../../src/http/sse.ts";
import { StreamsReader, StreamsWriter } from "../../src/protocol/tags.ts";
import { StreamsTest, layerTest } from "../../src/testing/streams-test.ts";
import { StreamId } from "../../src/schema/index.ts";

const id = StreamId.make("sse");

const json = "application/json";

const encoder = new TextEncoder();

const decoder = new TextDecoder();

function check<E>(program: Effect.Effect<void, E, StreamsReader | StreamsWriter | StreamsTest>) {
  return expect(
    Effect.runPromiseExit(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(
            Layer.mergeAll(layerTest({ longPollTimeoutMs: 100_000 }), TestClock.layer()),
          );

          return yield* program.pipe(Effect.provide(context));
        }),
      ),
    ),
  ).resolves.toEqual(Exit.succeed(undefined));
}

/** A Random service that always draws `r`, so the deadline is known. */
const fixedRandom = (r: number) =>
  Effect.provideService(Random.Random, { nextIntUnsafe: () => 0, nextDoubleUnsafe: () => r });

const body = (reader: typeof StreamsReader.Service, deadlineMs: number) => {
  const response = sse(reader, id, json, "-1", undefined, deadlineMs);

  if (response.body._tag !== "Stream") throw new Error("SSE body must be a stream");

  // The web body stream has an unknown error channel; a failure here is a test defect.
  return response.body.stream.pipe(Stream.orDie);
};

it("jitterDeadline spreads the deadline by 10 percent either way", () => {
  expect(jitterDeadline(60_000, 0)).toBe(54_000);
  expect(jitterDeadline(60_000, 0.5)).toBe(60_000);
  expect(jitterDeadline(60_000, 0.999_999)).toBe(66_000);
  expect(jitterDeadline(20, 0)).toBe(18);
  expect(jitterDeadline(20, 0.999_999)).toBe(22);
});

for (const [r, deadline] of [
  [0, 900],
  [0.999_999, 1100],
] as const) {
  it(`an SSE body with draw ${r} ends at ${deadline} ms, not before`, () =>
    check(
      Effect.gen(function* () {
        yield* (yield* StreamsWriter).create(id, { contentType: json });
        const control = yield* StreamsTest;

        const fiber = yield* body(yield* StreamsReader, 1000).pipe(
          Stream.runDrain,
          fixedRandom(r),
          Effect.forkScoped,
        );

        yield* control.snapshot;
        yield* TestClock.adjust(deadline - 1);
        expect(fiber.pollUnsafe()).toBeUndefined();
        yield* TestClock.adjust(1);
        expect(Exit.isSuccess(yield* Fiber.await(fiber))).toBe(true);
      }).pipe(Effect.scoped),
    ));
}

it("each SSE chunk is one whole batch that ends with its control event", () =>
  check(
    Effect.gen(function* () {
      const writer = yield* StreamsWriter;
      yield* writer.create(id, { contentType: json });
      yield* writer.append(id, { contentType: json, data: encoder.encode('[{"n":1},{"n":2}]') });
      const control = yield* StreamsTest;

      const fiber = yield* body(yield* StreamsReader, 60_000).pipe(
        Stream.take(2),
        Stream.runCollect,
        fixedRandom(0.5),
        Effect.forkScoped,
      );

      yield* control.snapshot;
      yield* writer.append(id, { contentType: json, data: encoder.encode('[{"n":3}]') });
      const chunks = (yield* Fiber.join(fiber)).map((chunk) => decoder.decode(chunk));
      expect(chunks).toHaveLength(2);

      for (const chunk of chunks) {
        expect(chunk.startsWith("event: data\n")).toBe(true);
        expect(chunk.split("event: control\n")).toHaveLength(2);
        expect(chunk).toMatch(/event: control\ndata:\{[^\n]*"streamNextOffset"[^\n]*\}\n\n$/);
      }

      expect(chunks[0]).toContain('data:{"n":1},\ndata:{"n":2}\n');
      expect(chunks[1]).toContain('data:{"n":3}\n');
    }).pipe(Effect.scoped),
  ));
