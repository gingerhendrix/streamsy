import { Streams, StreamsReader, type StreamsWriter } from "@streamsy/core";
import { Checkpoints, Projection, ProjectionFault, type State } from "@streamsy/projection";
import * as ProjectionMemory from "@streamsy/projection/memory";
import { Context, Effect, Layer, ManagedRuntime, Schedule, Schema, Stream } from "effect";
import { afterEach, describe, expect, test } from "bun:test";
import { sourceDelete, sourceUpsert } from "../../src/server/source-change.ts";
import { hackerNewsSource } from "../../src/server/stream-resources.ts";
import { hackerNewsStoryIndex, hackerNewsTarget } from "../../src/server/story-index-projection.ts";
import { storyProjectionLayer } from "../../src/server/projection.ts";
import { demoHarness, story } from "../../src/server/test-support.ts";
import { DemoStreams, demoStreamsLayer } from "../../src/server/streams.ts";
import { HackerNewsStateChange, type HnStory } from "../../src/state-schema.ts";

type Harness = Awaited<ReturnType<typeof demoHarness>>;
const clients = new Set<Harness>();
const limits = { limit: 10 };

afterEach(async () => {
  await Promise.all(Array.from(clients, (client) => client.close()));
  clients.clear();
});

async function harness(readLimit = 1000) {
  const h = await demoHarness(readLimit);
  clients.add(h);
  return h;
}

function run(h: Harness, options: { limit?: number } = {}) {
  return Effect.runPromise(
    Projection.run(hackerNewsStoryIndex, { ...limits, ...options }).pipe(
      Effect.provide(h.clientLayer),
    ),
  );
}

function loadRecord(h: Harness) {
  return h.runtime.runPromise(
    Effect.flatMap(Checkpoints, (owner) => owner.load(Projection.key(hackerNewsStoryIndex))),
  );
}

describe("Hacker News story index projection", () => {
  test("emits client-readable upserts, updates, and deletes", async () => {
    const h = await harness();
    const first = story(101, 1_700_000_030, "First title");
    const second = story(102, 1_700_000_020, "Second title");
    await h.append(hackerNewsSource.id, [sourceUpsert(first), sourceUpsert(second)]);

    expect(await run(h)).toMatchObject({ status: "caught-up", units: 1, items: 2 });

    const updated = story(101, 1_700_000_030, "Updated title");
    await h.append(hackerNewsSource.id, [sourceUpsert(updated), sourceDelete(second)]);
    await run(h);

    const facts = (await h.read(hackerNewsTarget.id)).filter(isStoryFact);
    expect(facts.map((fact) => [fact.key, fact.headers.operation])).toEqual([
      ["101", "upsert"],
      ["102", "upsert"],
      ["101", "upsert"],
      ["102", "delete"],
    ]);
    expect(storyTitle(facts[2] && "value" in facts[2] ? facts[2].value : undefined)).toBe(
      "Updated title",
    );
    expect(facts[3]).toEqual({ type: "hn-story", key: "102", headers: { operation: "delete" } });
  });

  test("declares one rows output keyed by story id", () => {
    expect(hackerNewsStoryIndex.outputs["hn-story"]).toMatchObject({ _tag: "Rows", key: "id" });
    expect(Object.keys(hackerNewsStoryIndex.streams)).toEqual(["hn-story"]);
  });

  test("resumes a bounded run at the next durable source boundary", async () => {
    const h = await harness(1);
    await h.append(hackerNewsSource.id, [sourceUpsert(story(101, 1_700_000_030, "First"))]);
    expect(await run(h, { limit: 1 })).toMatchObject({
      status: "limit-reached",
      units: 1,
      items: 1,
    });

    await h.append(hackerNewsSource.id, [sourceUpsert(story(102, 1_700_000_020, "Second"))]);

    expect(await run(h, { limit: 1 })).toMatchObject({
      status: "limit-reached",
      units: 1,
      items: 1,
    });
    expect(await run(h)).toMatchObject({ status: "caught-up", units: 0, items: 0 });
    const facts = (await h.read(hackerNewsTarget.id)).filter(isStoryFact);
    expect(facts.map((fact) => fact.key)).toEqual(["101", "102"]);
  });

  test("does not duplicate output when projection orchestration restarts", async () => {
    const h = await harness();
    await h.append(hackerNewsSource.id, [sourceUpsert(story(101, 1_700_000_030, "First"))]);
    await run(h);
    const before = await h.read(hackerNewsTarget.id);

    // A fresh declaration with the same identity restores the stored checkpoint.
    const restarted = { ...hackerNewsStoryIndex };
    const result = await Effect.runPromise(
      Projection.run(restarted, limits).pipe(Effect.provide(h.clientLayer)),
    );
    const after = await h.read(hackerNewsTarget.id);

    expect(result).toMatchObject({ status: "caught-up", units: 0, items: 0 });
    expect(after).toEqual(before);
  });

  test("the Layer-scoped change watcher indexes an append after catching up", async () => {
    const host = demoStreamsLayer.pipe(Layer.provideMerge(ProjectionMemory.layerMemory()));
    const runtime = ManagedRuntime.make(
      storyProjectionLayer(limits).pipe(Layer.provideMerge(host)),
    );
    try {
      const streams = await runtime.runPromise(DemoStreams);
      await runtime.runPromise(
        streams.appendSourceBatch([sourceUpsert(story(101, 1_700_000_030, "Initial"))]),
      );
      await runtime.runPromise(
        Streams.read(hackerNewsTarget).pipe(
          Streams.items,
          Stream.runCollect,
          Effect.flatMap((items) =>
            items.length === 0
              ? Effect.fail("projection has not caught up yet")
              : Effect.succeed(items),
          ),
          Effect.retry({ schedule: Schedule.spaced(10), times: 25 }),
        ),
      );

      await runtime.runPromise(
        streams.appendSourceBatch([sourceUpsert(story(102, 1_700_000_040, "Woken"))]),
      );
      const facts = await runtime.runPromise(
        Streams.read(hackerNewsTarget).pipe(
          Streams.items,
          Stream.runCollect,
          Effect.flatMap((items) =>
            items.length < 2
              ? Effect.fail("projection has not appended yet")
              : Effect.succeed(items),
          ),
          Effect.retry({ schedule: Schedule.spaced(10), times: 25 }),
        ),
      );
      expect(facts).toHaveLength(2);
      expect(facts[1]?.key).toBe("102");
    } finally {
      await runtime.dispose();
    }
  });

  test("limit-reached at one item per pass, then resumes to caught-up", async () => {
    const h = await harness(1);
    await h.append(hackerNewsSource.id, [
      sourceUpsert(story(101, 1_700_000_030, "First")),
      sourceUpsert(story(102, 1_700_000_020, "Second")),
    ]);

    expect(await run(h, { limit: 1 })).toMatchObject({
      status: "limit-reached",
      units: 1,
      items: 1,
    });
    expect((await h.read(hackerNewsTarget.id)).filter(isStoryFact).map((f) => f.key)).toEqual([
      "101",
    ]);

    expect(await run(h)).toMatchObject({ status: "caught-up", units: 1, items: 1 });
    expect((await h.read(hackerNewsTarget.id)).filter(isStoryFact).map((f) => f.key)).toEqual([
      "101",
      "102",
    ]);
  });

  test("a competing checkpoint save fails token-conflict without advancing", async () => {
    const h = await harness();
    await h.append(hackerNewsSource.id, [sourceUpsert(story(101, 1_700_000_030, "First"))]);
    const context = await h.runtime.runPromise(
      Effect.context<StreamsReader | StreamsWriter | Checkpoints | State>(),
    );
    const reader = Context.get(context, StreamsReader);
    // A competing runner completes inside this runner's read window.
    const raced = Context.add(
      context,
      StreamsReader,
      StreamsReader.of({
        ...reader,
        read: (id, options) =>
          Projection.run(hackerNewsStoryIndex, limits).pipe(
            Effect.provide(h.clientLayer),
            Effect.orDie,
            Effect.andThen(reader.read(id, options)),
          ),
      }),
    );

    const failed = await Effect.runPromise(
      Projection.run(hackerNewsStoryIndex, limits).pipe(Effect.flip, Effect.provide(raced)),
    );
    expect(failed).toBeInstanceOf(ProjectionFault);
    expect(failed).toMatchObject({ phase: "pin", reason: "token-conflict" });

    expect(await h.read(hackerNewsTarget.id)).toHaveLength(1);
    expect((await loadRecord(h)).token).toBe("2");
    expect(await run(h)).toMatchObject({ status: "caught-up", units: 0, items: 0 });
    expect(await h.read(hackerNewsTarget.id)).toHaveLength(1);
  });

  test("a missing source fails the read naming the input", async () => {
    const h = await harness();
    await h.runtime.runPromise(Streams.remove(hackerNewsSource));
    const failed = await Effect.runPromise(
      Projection.run(hackerNewsStoryIndex, limits).pipe(Effect.flip, Effect.provide(h.clientLayer)),
    );
    expect(failed).toMatchObject({
      phase: "read",
      reason: "history-unavailable",
      input: "input",
    });
  });
});

const isStoryFact = Schema.is(HackerNewsStateChange);

function storyTitle(value: HnStory | undefined): string | undefined {
  return value?.title;
}
