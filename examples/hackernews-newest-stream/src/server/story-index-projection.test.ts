import { Streams, StreamsReader, StreamsWriter } from "@streamsy/core-next";
/* oxlint-disable effecttsgo/async-function -- This Bun integration suite drives Promise protocol adapters and executes Effect descriptions at the test boundary. */
import * as StateProjection from "./bridge/state-projection.ts";
import type { Instance as StateProjectionInstance } from "./bridge/state-projection.ts";
import { Context, Effect, Schema } from "effect";
import { afterEach, describe, expect, test } from "bun:test";
import { makeStoryProjectionInstance } from "./projection.ts";
import { hackerNewsSource, hackerNewsTarget } from "./stream-resources.ts";
import { sourceDelete, sourceUpsert } from "./story-index-projection.ts";
import { demoHarness, story } from "./test-support.ts";
import { HackerNewsStateChange, type HnStory } from "../state-schema.ts";

const clients = new Set<Awaited<ReturnType<typeof demoHarness>>>();
const limits = { pages: 10, batches: 10, items: 50, bytes: 100_000 };

afterEach(async () => {
  await Promise.all(Array.from(clients, (client) => client.close()));
  clients.clear();
});

async function harness() {
  const h = await demoHarness();
  clients.add(h);
  return h;
}

function catchUp<Input>(
  projection: StateProjectionInstance<Input>,
  clientLayer: Awaited<ReturnType<typeof demoHarness>>["clientLayer"],
  limitOverrides: Partial<typeof limits> = {},
) {
  const program = StateProjection.catchUp(projection, {
    limits: { ...limits, ...limitOverrides },
  });
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This helper is the Bun execution boundary for the fixed-client projection layer.
  return program.pipe(Effect.provide(clientLayer));
}

describe("Hacker News StateProjection story index", () => {
  test("emits client-readable upserts, updates, and deletes", async () => {
    const h = await harness();
    const first = story(101, 1_700_000_030, "First title");
    const second = story(102, 1_700_000_020, "Second title");
    await h.append(hackerNewsSource.streamId, [sourceUpsert(first), sourceUpsert(second)]);

    expect(await Effect.runPromise(catchUp(h.projection, h.clientLayer))).toMatchObject({
      status: "caught-up",
      progress: { batches: 1, items: 2 },
    });

    const updated = story(101, 1_700_000_030, "Updated title");
    await h.append(hackerNewsSource.streamId, [sourceUpsert(updated), sourceDelete(second)]);
    await Effect.runPromise(catchUp(h.projection, h.clientLayer));

    const facts = (await h.read(hackerNewsTarget.streamId)).filter(isStoryFact);
    expect(facts.map((fact) => [fact.key, fact.headers.operation])).toEqual([
      ["101", "upsert"],
      ["102", "upsert"],
      ["101", "upsert"],
      ["102", "delete"],
    ]);
    expect(storyTitle(facts[2] && "value" in facts[2] ? facts[2].value : undefined)).toBe(
      "Updated title",
    );
    expect(storyTitle(facts[3] && "old_value" in facts[3] ? facts[3].old_value : undefined)).toBe(
      "Second title",
    );
  });

  test("resumes bounded catch-up at the next durable source boundary", async () => {
    const h = await harness();
    await h.append(hackerNewsSource.streamId, [sourceUpsert(story(101, 1_700_000_030, "First"))]);
    await Effect.runPromise(catchUp(h.projection, h.clientLayer, { batches: 1, items: 1 }));

    await h.append(hackerNewsSource.streamId, [sourceUpsert(story(102, 1_700_000_020, "Second"))]);

    const resumed = await Effect.runPromise(
      catchUp(h.projection, h.clientLayer, { batches: 1, items: 1 }),
    );
    expect(resumed).toMatchObject({
      status: "caught-up",
      progress: { batches: 1, items: 1 },
    });
    const facts = (await h.read(hackerNewsTarget.streamId)).filter(isStoryFact);
    expect(facts.map((fact) => fact.key)).toEqual(["101", "102"]);
  });

  test("does not duplicate output when projection orchestration restarts", async () => {
    const h = await harness();
    await h.append(hackerNewsSource.streamId, [sourceUpsert(story(101, 1_700_000_030, "First"))]);
    await Effect.runPromise(catchUp(h.projection, h.clientLayer));
    const before = await h.read(hackerNewsTarget.streamId);

    const restarted = makeStoryProjectionInstance();
    const result = await Effect.runPromise(catchUp(restarted, h.clientLayer));
    const after = await h.read(hackerNewsTarget.streamId);

    expect(result).toMatchObject({
      status: "caught-up",
      progress: { batches: 0, items: 0 },
    });
    expect(after).toEqual(before);
  });

  test("refuses a source boundary larger than the configured item bound", async () => {
    const h = await harness();
    await h.append(hackerNewsSource.streamId, [
      sourceUpsert(story(101, 1_700_000_030, "First")),
      sourceUpsert(story(102, 1_700_000_020, "Second")),
      sourceUpsert(story(103, 1_700_000_010, "Third")),
    ]);

    const outcome = await Effect.runPromise(catchUp(h.projection, h.clientLayer, { items: 2 }));
    expect(outcome).toMatchObject({
      status: "boundary-too-large",
      limit: "items",
      actual: 3,
      maximum: 2,
    });
    expect(await h.read(hackerNewsTarget.streamId)).toHaveLength(0);
  });
});

const isStoryFact = Schema.is(HackerNewsStateChange);

function storyTitle(value: HnStory | undefined): string | undefined {
  return value?.title;
}

describe("private bridge recovery and bounds", () => {
  for (const limit of ["pages", "batches", "items", "bytes"] as const) {
    test(`preserves ${limit} limit-reached progress across read pages`, async () => {
      const h = await harness();
      const values = [sourceUpsert(story(101, 1, "First")), sourceUpsert(story(102, 2, "Second"))];
      await h.append(hackerNewsSource.streamId, values);
      const context = await h.runtime.runPromise(Effect.context<StreamsReader | StreamsWriter>());
      const reader = Context.get(context, StreamsReader);
      const paged = Context.add(
        context,
        StreamsReader,
        StreamsReader.of({
          ...reader,
          read: (id, options) =>
            reader.read(id, id === hackerNewsSource.ref.id ? { ...options, limit: 1 } : options),
        }),
      );
      const bytes = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.Unknown)))([
        values[0],
      ]).length;
      const outcome = await Effect.runPromise(
        StateProjection.catchUp(h.projection, {
          limits: { ...limits, [limit]: limit === "bytes" ? bytes + 10 : 1 },
        }).pipe(Effect.provide(paged)),
      );
      expect(outcome).toMatchObject({
        status: "limit-reached",
        limit,
        progress: { batches: 1, items: 1, pages: 1 },
      });
      expect(await h.read(hackerNewsTarget.streamId)).toHaveLength(1);
      const resumed = await Effect.runPromise(catchUp(h.projection, h.clientLayer));
      expect(resumed).toMatchObject({ status: "caught-up", progress: { batches: 1, items: 1 } });
      expect(
        (await h.read(hackerNewsTarget.streamId)).filter(isStoryFact).map((fact) => fact.key),
      ).toEqual(["101", "102"]);
    });
  }
  test("rejects a byte-oversized boundary without writing output", async () => {
    const h = await harness();
    await h.append(hackerNewsSource.streamId, [sourceUpsert(story(101, 1, "First"))]);
    expect(
      await Effect.runPromise(catchUp(h.projection, h.clientLayer, { bytes: 1 })),
    ).toMatchObject({ status: "boundary-too-large", limit: "bytes", maximum: 1 });
    expect(await h.read(hackerNewsTarget.streamId)).toHaveLength(0);
  });
  test("a concurrent target write returns output-conflict instead of advancing recovery", async () => {
    const h = await harness();
    await h.append(hackerNewsSource.streamId, [sourceUpsert(story(101, 1, "First"))]);
    const context = await h.runtime.runPromise(Effect.context<StreamsReader | StreamsWriter>());
    const writer = Context.get(context, StreamsWriter);
    let raced = false;
    const competing = Context.add(
      context,
      StreamsWriter,
      StreamsWriter.of({
        ...writer,
        append: (id, options) =>
          Effect.gen(function* () {
            if (id === hackerNewsTarget.ref.id && !raced) {
              raced = true;
              const winner = yield* writer.append(id, options);
              expect(winner.status).toBe("appended");
            }
            return yield* writer.append(id, options);
          }),
      }),
    );
    expect(
      await Effect.runPromise(
        StateProjection.catchUp(h.projection, { limits }).pipe(Effect.provide(competing)),
      ),
    ).toMatchObject({
      status: "output-conflict",
      reason: "expected-offset",
      progress: { batches: 0, items: 0 },
    });
    expect(await h.read(hackerNewsTarget.streamId)).toHaveLength(1);
    expect(await Effect.runPromise(catchUp(h.projection, h.clientLayer))).toMatchObject({
      status: "caught-up",
      progress: { batches: 0 },
    });
  });
  test("missing source or target stays an explicit outcome", async () => {
    for (const stream of ["source", "target"] as const) {
      const h = await harness();
      const ref = stream === "source" ? hackerNewsSource.ref : hackerNewsTarget.ref;
      await h.runtime.runPromise(Streams.remove(ref));
      expect(await Effect.runPromise(catchUp(h.projection, h.clientLayer))).toMatchObject({
        status: "missing",
        stream,
      });
    }
  });
});
