/* oxlint-disable effecttsgo/async-function -- This Vitest integration suite drives Promise protocol adapters and executes Effect descriptions at the test boundary. */
import { type StreamProtocolClient } from "@streamsy/core";
import * as StateProjection from "@streamsy/experimental/state-projection";
import type { Instance as StateProjectionInstance } from "@streamsy/experimental/state-projection";
import { Effect } from "effect";
import { afterEach, describe, expect, test } from "vitest";
import { makeStoryProjectionInstance } from "./projection.ts";
import { hackerNewsSource, hackerNewsTarget } from "./stream-resources.ts";
import { sourceDelete, sourceUpsert } from "./story-index-projection.ts";
import { demoHarness, story } from "./test-support.ts";

const clients = new Set<StreamProtocolClient>();
const limits = { pages: 10, batches: 10, items: 50, bytes: 100_000 };

afterEach(async () => {
  await Promise.all(Array.from(clients, (client) => client.close()));
  clients.clear();
});

async function harness() {
  const h = await demoHarness();
  clients.add(h.client);
  return h;
}

function catchUp<Input>(
  projection: StateProjectionInstance<Input>,
  clientLayer: ReturnType<typeof StateProjection.layerClient>,
  limitOverrides: Partial<typeof limits> = {},
) {
  const program = StateProjection.catchUp(projection, {
    limits: { ...limits, ...limitOverrides },
  });
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This helper is the Vitest execution boundary for the fixed-client projection layer.
  return program.pipe(Effect.provide(clientLayer));
}

describe("Hacker News StateProjection story index", () => {
  test("emits client-readable upserts, updates, and deletes", async () => {
    const h = await harness();
    const first = story(101, 1_700_000_030, "First title");
    const second = story(102, 1_700_000_020, "Second title");
    await h.client
      .stream(hackerNewsSource.streamId)
      .appendJsonBatch([sourceUpsert(first), sourceUpsert(second)]);

    expect(await Effect.runPromise(catchUp(h.projection, h.clientLayer))).toMatchObject({
      status: "caught-up",
      progress: { batches: 1, items: 2 },
    });

    const updated = story(101, 1_700_000_030, "Updated title");
    await h.client
      .stream(hackerNewsSource.streamId)
      .appendJsonBatch([sourceUpsert(updated), sourceDelete(second)]);
    await Effect.runPromise(catchUp(h.projection, h.clientLayer));

    const facts = (await readAllJson(h.client, hackerNewsTarget.streamId)).filter(isStoryFact);
    expect(facts.map((fact) => [fact.key, fact.headers.operation])).toEqual([
      ["101", "upsert"],
      ["102", "upsert"],
      ["101", "upsert"],
      ["102", "delete"],
    ]);
    expect(storyTitle(facts[2]?.value)).toBe("Updated title");
    expect(storyTitle(facts[3]?.old_value)).toBe("Second title");
  });

  test("resumes bounded catch-up at the next durable source boundary", async () => {
    const h = await harness();
    await h.client
      .stream(hackerNewsSource.streamId)
      .appendJsonBatch([sourceUpsert(story(101, 1_700_000_030, "First"))]);
    await Effect.runPromise(catchUp(h.projection, h.clientLayer, { batches: 1, items: 1 }));

    await h.client
      .stream(hackerNewsSource.streamId)
      .appendJsonBatch([sourceUpsert(story(102, 1_700_000_020, "Second"))]);

    const resumed = await Effect.runPromise(
      catchUp(h.projection, h.clientLayer, { batches: 1, items: 1 }),
    );
    expect(resumed).toMatchObject({
      status: "caught-up",
      progress: { batches: 1, items: 1 },
    });
    const facts = (await readAllJson(h.client, hackerNewsTarget.streamId)).filter(isStoryFact);
    expect(facts.map((fact) => fact.key)).toEqual(["101", "102"]);
  });

  test("does not duplicate output when projection orchestration restarts", async () => {
    const h = await harness();
    await h.client
      .stream(hackerNewsSource.streamId)
      .appendJsonBatch([sourceUpsert(story(101, 1_700_000_030, "First"))]);
    await Effect.runPromise(catchUp(h.projection, h.clientLayer));
    const before = await h.adapter.listMessages(hackerNewsTarget.streamId);

    const restarted = makeStoryProjectionInstance();
    const result = await Effect.runPromise(catchUp(restarted, h.clientLayer));
    const after = await h.adapter.listMessages(hackerNewsTarget.streamId);

    expect(result).toMatchObject({
      status: "caught-up",
      progress: { batches: 0, items: 0 },
    });
    expect(after).toEqual(before);
  });

  test("refuses a source boundary larger than the configured item bound", async () => {
    const h = await harness();
    await h.client
      .stream(hackerNewsSource.streamId)
      .appendJsonBatch([
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
    expect(await h.adapter.listMessages(hackerNewsTarget.streamId)).toHaveLength(0);
  });
});

async function readAllJson(client: StreamProtocolClient, streamId: string): Promise<unknown[]> {
  const opened = await client.stream(streamId).read({ offset: "-1" });
  if (opened.status !== "ok") throw new Error(`expected target read, got ${opened.status}`);
  const items: unknown[] = [];
  for await (const batch of opened.session) {
    if (batch.kind !== "json") throw new Error("expected JSON target");
    items.push(...batch.items);
  }
  return items;
}

function isStoryFact(value: unknown): value is StoryFact {
  if (!isJsonObject(value) || value.type !== "hn-story" || typeof value.key !== "string") {
    return false;
  }
  if (!isJsonObject(value.headers) || typeof value.headers.operation !== "string") return false;
  return value.value === undefined || isStoryValue(value.value);
}

function isStoryValue(value: unknown): value is StoryValue {
  return isJsonObject(value) && typeof value.title === "string";
}

function storyTitle(value: unknown): string | undefined {
  return value !== undefined && isStoryValue(value) ? value.title : undefined;
}

function isJsonObject(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

interface StoryFact {
  readonly type: "hn-story";
  readonly key: string;
  readonly headers: Readonly<Record<string, unknown>> & { readonly operation: string };
  readonly value?: unknown;
  readonly old_value?: unknown;
}

interface StoryValue {
  readonly title: string;
}
