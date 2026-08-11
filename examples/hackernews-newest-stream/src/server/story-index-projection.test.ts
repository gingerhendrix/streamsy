import {
  StreamProtocol,
  createMemoryStorageAdapter,
  directProtocolClient,
  type JsonValue,
  type StreamProtocolClient,
} from "@streamsy/core";
import { bindStream } from "@streamsy/experimental/binding";
import { streamIdentity } from "@streamsy/experimental/causal";
import { AppendStreamsLive, ReadStreamsLive } from "@streamsy/experimental/effect";
import {
  StateProjection,
  type StateProjectionInstance,
} from "@streamsy/experimental/effect/state-projection";
import { Effect } from "effect";
import { afterEach, describe, expect, test } from "vitest";
import { hackerNewsStoryIndex } from "./story-index-projection.ts";

const clients = new Set<StreamProtocolClient>();
const limits = { pages: 10, batches: 10, items: 50, bytes: 100_000 };

afterEach(async () => {
  await Promise.all(Array.from(clients, (client) => client.close()));
  clients.clear();
});

async function harness() {
  const adapter = createMemoryStorageAdapter();
  const client = directProtocolClient(new StreamProtocol({ storage: { adapter } }));
  clients.add(client);
  const source = bindStream({
    identity: streamIdentity("hn-newest-source"),
    client,
    streamId: "hn-newest-source",
  });
  const target = bindStream({
    identity: streamIdentity("hn-story-index"),
    client,
    streamId: "hn-story-index/v1",
  });
  await client.stream(source.streamId).create({ contentType: "application/json" });
  await client.stream(target.streamId).create({ contentType: "application/json" });

  return {
    adapter,
    client,
    source,
    target,
    projection: StateProjection.instance(hackerNewsStoryIndex, {
      source,
      target,
      generation: "v1",
      producerEpoch: 0,
    }),
  };
}

function catchUp<Input>(projection: StateProjectionInstance<Input>, itemLimit = 50) {
  return StateProjection.catchUp(projection, {
    limits: { ...limits, items: itemLimit },
  }).pipe(Effect.provide(ReadStreamsLive), Effect.provide(AppendStreamsLive));
}

describe("Hacker News StateProjection story index", () => {
  test("writes stable story rows and resumes from durable output", async () => {
    const h = await harness();
    await h.client
      .stream(h.source.streamId)
      .appendJsonBatch([
        story(101, 1_700_000_030, "First title"),
        story(102, 1_700_000_020, "Second title"),
      ]);

    const first = await Effect.runPromise(catchUp(h.projection));
    expect(first).toMatchObject({
      status: "caught-up",
      progress: { batches: 1, items: 2 },
    });
    if ("progress" in first && first.progress) {
      expect("checkpoint" in first.progress).toBe(false);
    }

    await h.client
      .stream(h.source.streamId)
      .appendJsonBatch([story(101, 1_700_000_030, "Updated title")]);
    const resumed = await Effect.runPromise(catchUp(h.projection));
    expect(resumed).toMatchObject({
      status: "caught-up",
      progress: { batches: 1, items: 1 },
    });

    const facts = (await readAllJson(h.client, h.target.streamId)).filter(isStoryFact);
    expect(facts).toHaveLength(3);
    expect(facts.map((fact) => fact.key)).toEqual(["101", "102", "101"]);
    expect(facts.map((fact) => fact.value.time)).toEqual([
      1_700_000_030, 1_700_000_020, 1_700_000_030,
    ]);
    expect(facts.at(-1)?.value.title).toBe("Updated title");
  });

  test("refuses a newest-story boundary larger than the configured item bound", async () => {
    const h = await harness();
    await h.client
      .stream(h.source.streamId)
      .appendJsonBatch([
        story(101, 1_700_000_030, "First"),
        story(102, 1_700_000_020, "Second"),
        story(103, 1_700_000_010, "Third"),
      ]);

    const outcome = await Effect.runPromise(catchUp(h.projection, 2));
    expect(outcome).toMatchObject({
      status: "boundary-too-large",
      limit: "items",
      actual: 3,
      maximum: 2,
    });
    expect(await h.adapter.listMessages(h.target.streamId)).toHaveLength(0);
  });
});

function story(id: number, time: number, title: string): JsonValue {
  return { id, time, title, type: "story" };
}

async function readAllJson(client: StreamProtocolClient, streamId: string): Promise<JsonValue[]> {
  const opened = await client.stream(streamId).read({ offset: "-1" });
  if (opened.status !== "ok") throw new Error(`expected target read, got ${opened.status}`);
  const items: JsonValue[] = [];
  for await (const batch of opened.session) {
    if (batch.kind !== "json") throw new Error("expected JSON target");
    items.push(...batch.items);
  }
  return items;
}

function isStoryFact(value: JsonValue): value is StoryFact {
  return (
    isJsonObject(value) &&
    value.type === "hn-story" &&
    typeof value.key === "string" &&
    isJsonObject(value.value) &&
    typeof value.value.time === "number" &&
    typeof value.value.title === "string"
  );
}

function isJsonObject(value: JsonValue): value is Readonly<Record<string, JsonValue>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

interface StoryFact extends Readonly<Record<string, JsonValue>> {
  readonly type: "hn-story";
  readonly key: string;
  readonly value: StoryValue;
}

interface StoryValue extends Readonly<Record<string, JsonValue>> {
  readonly time: number;
  readonly title: string;
}
