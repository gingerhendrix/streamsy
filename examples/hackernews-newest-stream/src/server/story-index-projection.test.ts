import {
  StreamProtocol,
  createMemoryStorageAdapter,
  directProtocolClient,
  type JsonValue,
  type StreamProtocolClient,
} from "@streamsy/core";
import { streamIdentity } from "@streamsy/experimental/causal";
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
  const source = StateProjection.resource({
    identity: streamIdentity("hn-newest-source"),
    streamId: "hn-newest-source",
  });
  const target = StateProjection.resource({
    identity: streamIdentity("hn-story-index"),
    streamId: "hn-story-index/v1",
  });
  await client.stream(source.streamId).create({ contentType: "application/json" });
  await client.stream(target.streamId).create({ contentType: "application/json" });

  return {
    adapter,
    client,
    source,
    target,
    clientLayer: StateProjection.layerClient(client),
    projection: StateProjection.instance(hackerNewsStoryIndex, {
      source,
      target,
      generation: "v1",
      producerEpoch: 0,
    }),
  };
}

function catchUp<Input>(
  projection: StateProjectionInstance<Input>,
  clientLayer: ReturnType<typeof StateProjection.layerClient>,
  itemLimit = 50,
) {
  return StateProjection.catchUp(projection, {
    limits: { ...limits, items: itemLimit },
  }).pipe(Effect.provide(clientLayer));
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

    const first = await Effect.runPromise(catchUp(h.projection, h.clientLayer));
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
    const resumed = await Effect.runPromise(catchUp(h.projection, h.clientLayer));
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

    const outcome = await Effect.runPromise(catchUp(h.projection, h.clientLayer, 2));
    expect(outcome).toMatchObject({
      status: "boundary-too-large",
      limit: "items",
      actual: 3,
      maximum: 2,
    });
    expect(await h.adapter.listMessages(h.target.streamId)).toHaveLength(0);
  });

  test("runs the same declaration under isolated client layers", async () => {
    const first = await harness();
    const second = await harness();
    expect("client" in first.projection).toBe(false);
    expect("client" in first.projection.source).toBe(false);
    expect("client" in first.projection.target).toBe(false);

    await first.client
      .stream(first.source.streamId)
      .appendJsonBatch([story(201, 1_700_000_020, "First client")]);
    await second.client
      .stream(second.source.streamId)
      .appendJsonBatch([story(202, 1_700_000_010, "Second client")]);

    await Effect.runPromise(catchUp(first.projection, first.clientLayer));
    await Effect.runPromise(catchUp(second.projection, second.clientLayer));

    const firstFacts = (await readAllJson(first.client, first.target.streamId)).filter(isStoryFact);
    const secondFacts = (await readAllJson(second.client, second.target.streamId)).filter(
      isStoryFact,
    );
    expect(firstFacts.map((fact) => fact.key)).toEqual(["201"]);
    expect(secondFacts.map((fact) => fact.key)).toEqual(["202"]);
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
