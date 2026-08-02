import {
  StreamProtocol,
  createHttpHandler,
  createMemoryStorageAdapter,
  directProtocolClient,
  type ClientAppendResult,
  type JsonValue,
  type StorageAdapter,
  type StreamProtocolClient,
  type StreamProtocolHandle,
} from "@streamsy/core";
import { officialProtocolClient, protocolPathUrl } from "@streamsy/client";
import { bindStream, type StreamBinding } from "@streamsy/experimental/binding";
import { sourceAck, streamIdentity, type SourceAck } from "@streamsy/experimental/causal";
import {
  MESH_LINEAGE_TYPE,
  appendDerivedStateBatch,
  deriveProducerLane,
  recoverDerivedState,
  type ProducerLane,
} from "@streamsy/experimental/ivm-mesh";
import { afterEach, describe, expect, test } from "vitest";
import {
  COUNTER_COLLECTION,
  EagerCounterConsumer,
  appendCounterIncrement,
  projectCounterIncrements,
} from "./causal-counter.ts";

interface Harness {
  readonly adapter: StorageAdapter;
  readonly client: StreamProtocolClient;
  readonly source: StreamBinding;
  readonly target: StreamBinding;
  readonly lane: ProducerLane;
}

const clients = new Set<StreamProtocolClient>();

afterEach(async () => {
  await Promise.all(Array.from(clients, (client) => client.close()));
  clients.clear();
});

describe("causal counter — memory", () => {
  test("proves an exact source acknowledgement only with atomically visible counter and lineage", async () => {
    const h = await memoryHarness("direct");
    const consumer = new EagerCounterConsumer();
    const appended = await appendCounterIncrement(h.source, { counterId: "visits", delta: 3 });
    if (appended.status !== "appended") throw new Error("expected source append");
    const ack = appended.ack;

    expect(consumer.counterValue("visits")).toBeUndefined();
    expect(consumer.syncedThrough(ack)).toEqual({ status: "not-yet" });
    expect(
      consumer.syncedThrough(sourceAck(streamIdentity("another-source"), ack.position)),
    ).toEqual({ status: "not-yet" });

    expect(await projectCounterIncrements(h)).toMatchObject({
      status: "caught-up",
      batches: 1,
      checkpoint: { sourceThrough: ack.position },
    });

    const observations: unknown[] = [];
    await consumer.catchUp(h.target, (view) => {
      observations.push(view);
      if (view.lineage?.value.sourceThrough === ack.position) {
        expect(view.counters.visits).toBe(3);
      }
    });
    expect(observations).toHaveLength(1);
    expect(consumer.counterValue("visits")).toBe(3);
    expect(consumer.syncedThrough(ack)).toEqual({ status: "proven" });
    expect(
      consumer.syncedThrough(sourceAck(streamIdentity("another-source"), ack.position)),
    ).toEqual({ status: "incomparable" });
    expect(consumer.syncedThrough(sourceAck(h.source.identity, `${ack.position}z`))).toEqual({
      status: "not-yet",
    });

    const events = await storedValues(h.adapter);
    expect(events.map(eventType)).toEqual([COUNTER_COLLECTION, MESH_LINEAGE_TYPE]);
  });

  test("projector and consumer restart from durable checkpoints without duplicating rows", async () => {
    const h = await memoryHarness("direct");
    const appended = await appendCounterIncrement(h.source, { counterId: "visits", delta: 2 });
    if (appended.status !== "appended") throw new Error("expected source append");
    expect(await projectCounterIncrements(h)).toMatchObject({ status: "caught-up", batches: 1 });

    const firstConsumer = new EagerCounterConsumer();
    await firstConsumer.catchUp(h.target);
    const snapshot = firstConsumer.snapshot();
    const before = await storedValues(h.adapter);

    expect(await projectCounterIncrements(h)).toMatchObject({ status: "caught-up", batches: 0 });
    const restartedConsumer = new EagerCounterConsumer(snapshot);
    await restartedConsumer.catchUp(h.target);
    expect(restartedConsumer.counterValue("visits")).toBe(2);
    expect(restartedConsumer.syncedThrough(appended.ack)).toEqual({ status: "proven" });
    expect(restartedConsumer.snapshot()).toEqual(snapshot);
    expect(await storedValues(h.adapter)).toEqual(before);
  });

  test("lost derived response commits one transaction and restart reaches proof", async () => {
    const h = await memoryHarness("direct");
    const appended = await appendCounterIncrement(h.source, { counterId: "visits", delta: 4 });
    if (appended.status !== "appended") throw new Error("expected source append");
    const lossyTarget = bindStream({ ...h.target, client: loseFirstAppendResponse(h.client) });

    expect(await projectCounterIncrements({ ...h, target: lossyTarget })).toMatchObject({
      status: "retryable",
      phase: "append",
    });
    expect(await storedValues(h.adapter)).toHaveLength(2);
    expect(await projectCounterIncrements(h)).toMatchObject({ status: "caught-up", batches: 0 });
    expect(await storedValues(h.adapter)).toHaveLength(2);

    const consumer = new EagerCounterConsumer();
    await consumer.catchUp(h.target);
    expect(consumer.counterValue("visits")).toBe(4);
    expect(consumer.syncedThrough(appended.ack)).toEqual({ status: "proven" });
  });

  test("changed retry bytes under one producer tuple do not write twice or verify payload", async () => {
    const h = await memoryHarness("direct");
    const appended = await appendCounterIncrement(h.source, { counterId: "visits", delta: 5 });
    if (appended.status !== "appended") throw new Error("expected source append");
    const previous = await recoverDerivedState(h.target, h.lane);
    if (previous.status !== "ready") throw new Error("expected ready output");
    const accepted = await appendDerivedStateBatch({
      target: h.target,
      lane: h.lane,
      previous,
      sourceThrough: appended.ack.position,
      facts: [counterEvent(appended.ack, 5)],
    });
    expect(accepted.status).toBe("appended");
    const changedRetry = await appendDerivedStateBatch({
      target: h.target,
      lane: h.lane,
      previous,
      sourceThrough: appended.ack.position,
      facts: [counterEvent(appended.ack, 999)],
    });
    expect(changedRetry.status).toBe("sequence-already-accepted");
    expect(changedRetry.status).not.toBe("verified-duplicate");
    expect(await storedValues(h.adapter)).toHaveLength(2);

    const consumer = new EagerCounterConsumer();
    await consumer.catchUp(h.target);
    expect(consumer.counterValue("visits")).toBe(5);
    expect(consumer.syncedThrough(appended.ack)).toEqual({ status: "proven" });
  });

  test("a racing target writer returns typed conflict without false lineage proof", async () => {
    const h = await memoryHarness("direct");
    const appended = await appendCounterIncrement(h.source, { counterId: "visits", delta: 1 });
    if (appended.status !== "appended") throw new Error("expected source append");
    const racingTarget = bindStream({
      ...h.target,
      client: raceFirstAppend(h.client, appended.ack),
    });
    expect(await projectCounterIncrements({ ...h, target: racingTarget })).toMatchObject({
      status: "output-conflict",
      reason: "expected-offset",
      batches: 0,
    });

    const consumer = new EagerCounterConsumer();
    await consumer.catchUp(h.target);
    expect(consumer.counterValue("foreign")).toBe(10);
    expect(consumer.syncedThrough(appended.ack)).toEqual({ status: "not-yet" });
    expect(
      (await storedValues(h.adapter)).filter((value) => eventType(value) === MESH_LINEAGE_TYPE),
    ).toHaveLength(0);
  });
});

test("direct and fetch variants produce identical counter rows and lineage", async () => {
  const direct = await completedScenario("direct");
  const fetch = await completedScenario("fetch");
  expect(stripResume(fetch.snapshot)).toEqual(stripResume(direct.snapshot));
  expect(fetch.events).toEqual(direct.events);
  expect(fetch.proof).toEqual({ status: "proven" });
});

async function completedScenario(transport: "direct" | "fetch") {
  const h = await memoryHarness(transport);
  const appended = await appendCounterIncrement(h.source, { counterId: "visits", delta: 7 });
  if (appended.status !== "appended") throw new Error("expected source append");
  expect(await projectCounterIncrements(h)).toMatchObject({ status: "caught-up", batches: 1 });
  const consumer = new EagerCounterConsumer();
  await consumer.catchUp(h.target);
  return {
    snapshot: consumer.snapshot(),
    events: await storedValues(h.adapter),
    proof: consumer.syncedThrough(appended.ack),
  };
}

async function memoryHarness(transport: "direct" | "fetch"): Promise<Harness> {
  const adapter = createMemoryStorageAdapter();
  const protocol = new StreamProtocol({ storage: { adapter } });
  const client =
    transport === "direct"
      ? directProtocolClient(protocol)
      : officialProtocolClient({
          urlFor: (id) => protocolPathUrl("https://counter.test/streams", id),
          fetch: ((input, init) =>
            createHttpHandler({ protocol, pathPrefix: "/streams" }).fetch(
              new Request(input, init),
            )) as typeof globalThis.fetch,
          backoffOptions: { initialDelay: 1, maxDelay: 1, multiplier: 1, maxRetries: 0 },
          warnOnHttp: false,
        });
  clients.add(client);
  return initializeHarness(adapter, client);
}

async function initializeHarness(
  adapter: StorageAdapter,
  client: StreamProtocolClient,
): Promise<Harness> {
  const sourceIdentity = streamIdentity("counter-facts");
  const targetIdentity = streamIdentity("derived-counter-state");
  const source = bindStream({ identity: sourceIdentity, client, streamId: "raw-counter-facts" });
  const target = bindStream({ identity: targetIdentity, client, streamId: "counter-state" });
  const lane = await deriveProducerLane({
    processorId: "causal-counter",
    processorVersion: "1.0.0",
    outputGeneration: "generation-1",
    source: sourceIdentity,
    target: targetIdentity,
    producerEpoch: 1,
  });
  expect(
    await client.stream(source.streamId).create({ contentType: "application/json" }),
  ).toMatchObject({
    status: "created",
  });
  expect(
    await client.stream(target.streamId).create({ contentType: "application/json" }),
  ).toMatchObject({
    status: "created",
  });
  return { adapter, client, source, target, lane };
}

function counterEvent(ack: SourceAck, delta: number): JsonValue {
  return {
    type: COUNTER_COLLECTION,
    key: `visits:${ack.position}:0`,
    value: { counterId: "visits", delta, sourcePosition: ack.position },
    headers: { operation: "upsert" },
  };
}

async function storedValues(adapter: StorageAdapter): Promise<unknown[]> {
  const decoder = new TextDecoder();
  return (await adapter.listMessages("counter-state")).map((message) =>
    JSON.parse(decoder.decode(message.data)),
  );
}

function eventType(value: unknown): unknown {
  return typeof value === "object" && value !== null && "type" in value ? value.type : undefined;
}

function stripResume(snapshot: ReturnType<EagerCounterConsumer["snapshot"]>) {
  const { targetResume: _, ...portable } = snapshot;
  return portable;
}

function loseFirstAppendResponse(client: StreamProtocolClient): StreamProtocolClient {
  let lose = true;
  return mapAppend(client, async (delegate, items, options) => {
    const result = await delegate.appendJsonBatch(items, options);
    if (!lose) return result;
    lose = false;
    return {
      status: "error",
      code: "transport",
      message: "response lost after commit",
      retryable: true,
    };
  });
}

function raceFirstAppend(client: StreamProtocolClient, ack: SourceAck): StreamProtocolClient {
  let race = true;
  return mapAppend(client, async (delegate, items, options) => {
    if (race) {
      race = false;
      await delegate.appendJsonBatch([
        {
          type: COUNTER_COLLECTION,
          key: "foreign:0",
          value: { counterId: "foreign", delta: 10, sourcePosition: ack.position },
          headers: { operation: "upsert" },
        },
      ]);
    }
    return delegate.appendJsonBatch(items, options);
  });
}

function mapAppend(
  client: StreamProtocolClient,
  operation: (
    delegate: StreamProtocolHandle,
    items: readonly JsonValue[],
    options: Parameters<StreamProtocolHandle["appendJsonBatch"]>[1],
  ) => Promise<ClientAppendResult>,
): StreamProtocolClient {
  return {
    stream(streamId): StreamProtocolHandle {
      const delegate = client.stream(streamId);
      return {
        id: delegate.id,
        head: (options) => delegate.head(options),
        create: (options) => delegate.create(options),
        append: (data, options) => delegate.append(data, options),
        appendJsonBatch: (items, options) => operation(delegate, items, options),
        close: (options) => delegate.close(options),
        read: (options) => delegate.read(options),
      };
    },
    close: (reason) => client.close(reason),
  };
}
