import {
  StreamProtocol,
  createHttpHandler,
  createMemoryStorageAdapter,
  directProtocolClient,
  type AppendJsonBatchOptions,
  type ClientAppendResult,
  type JsonValue,
  type StorageAdapter,
  type StreamProtocolClient,
  type StreamProtocolHandle,
} from "@streamsy/core";
import { officialProtocolClient, protocolPathUrl } from "@streamsy/client";
import { describe, expect, test } from "vitest";
import { bindStream, type StreamBinding } from "../binding.ts";
import { streamIdentity } from "../causal.ts";
import { appendDerivedStateBatch, recoverDerivedState } from "./derived-append.ts";
import { deriveProducerLane, type ProducerLane } from "./lane.ts";
import { MESH_LINEAGE_KEY, MESH_LINEAGE_TYPE, createLineageEvent } from "./state-meta.ts";

const decoder = new TextDecoder();
const noRetry = { initialDelay: 1, maxDelay: 1, multiplier: 1, maxRetries: 0 };

type Harness = {
  adapter: StorageAdapter;
  protocol: StreamProtocol;
  client: StreamProtocolClient;
  target: StreamBinding;
  lane: ProducerLane;
  close(): Promise<void>;
};

async function harness(): Promise<Harness> {
  const adapter = createMemoryStorageAdapter();
  const protocol = new StreamProtocol({ storage: { adapter } });
  const client = directProtocolClient(protocol);
  const source = streamIdentity("orders");
  const targetIdentity = streamIdentity("orders-by-status");
  const target = bindStream({ identity: targetIdentity, client, streamId: "derived" });
  const lane = await deriveProducerLane({
    processorId: "orders-by-status",
    processorVersion: "1.0.0",
    outputGeneration: "generation-1",
    source,
    target: targetIdentity,
    producerEpoch: 41,
  });
  const created = await client.stream("derived").create({ contentType: "application/json" });
  expect(created.status === "created" || created.status === "conflict").toBe(true);
  return {
    adapter,
    protocol,
    client,
    target,
    lane,
    async close() {
      await client.close();
    },
  };
}

function fact(value: string): JsonValue {
  return {
    type: "order",
    key: "o-1",
    value: { id: "o-1", status: value },
    headers: { operation: "upsert" },
  };
}

async function ready(h: Harness) {
  const recovered = await recoverDerivedState(h.target, h.lane);
  if (recovered.status !== "ready") throw new Error(`expected ready, got ${recovered.status}`);
  return recovered;
}

async function rawValues(adapter: StorageAdapter): Promise<unknown[]> {
  const messages = await adapter.listMessages("derived");
  return messages.map((message) => JSON.parse(decoder.decode(message.data)) as unknown);
}

describe("derived State append — memory", () => {
  test("commits facts and final lineage in one boundary and restarts with the fixed lane", async () => {
    const h = await harness();
    const commits: string[] = [];
    h.protocol.onAfterCommit((event) => commits.push(event.offset));
    const initial = await ready(h);
    const first = await appendDerivedStateBatch({
      target: h.target,
      lane: h.lane,
      previous: initial,
      sourceThrough: "00000001",
      facts: [fact("open")],
    });
    expect(first.status).toBe("appended");
    if (first.status !== "appended") throw new Error("expected appended");
    expect(commits).toEqual([first.offset]);
    const values = await rawValues(h.adapter);
    expect(values).toHaveLength(2);
    expect(values[0]).toEqual(fact("open"));
    expect(values[1]).toMatchObject({
      type: MESH_LINEAGE_TYPE,
      key: MESH_LINEAGE_KEY,
      value: { sourceThrough: "00000001", nextProducerSeq: 1 },
    });

    const restarted = await ready(h);
    expect(restarted).toEqual(first.checkpoint);
    expect(restarted.producerEpoch).toBe(41);
    const second = await appendDerivedStateBatch({
      target: h.target,
      lane: h.lane,
      previous: restarted,
      sourceThrough: "00000002",
      facts: [],
    });
    expect(second).toMatchObject({ status: "appended", checkpoint: { nextProducerSeq: 2 } });
    expect(await h.adapter.getProducerState("derived", h.lane.producerId)).toEqual({
      epoch: 41,
      lastSeq: 1,
    });
    await h.close();
  });

  test("reconciles a lost response without a second batch or payload-verification claim", async () => {
    const h = await harness();
    const initial = await ready(h);
    const lossyTarget = bindStream({
      identity: h.target.identity,
      streamId: h.target.streamId,
      client: loseFirstAppendResponse(h.client),
    });
    const first = await appendDerivedStateBatch({
      target: lossyTarget,
      lane: h.lane,
      previous: initial,
      sourceThrough: "00000001",
      facts: [fact("open")],
    });
    expect(first).toMatchObject({ status: "error", code: "transport" });
    const retried = await appendDerivedStateBatch({
      target: lossyTarget,
      lane: h.lane,
      previous: initial,
      sourceThrough: "00000001",
      facts: [fact("changed-retry-bytes")],
    });
    expect(retried).toMatchObject({ status: "sequence-already-accepted" });
    expect(JSON.stringify(retried)).not.toContain("verified");
    const values = await rawValues(h.adapter);
    expect(values).toHaveLength(2);
    expect(values[0]).toEqual(fact("open"));
    await h.close();
  });

  test("contention conflicts without mutating the lane or appending lineage", async () => {
    const h = await harness();
    const initial = await ready(h);
    await h.client.stream("derived").appendJsonBatch([fact("foreign")]);
    const result = await appendDerivedStateBatch({
      target: h.target,
      lane: h.lane,
      previous: initial,
      sourceThrough: "00000001",
      facts: [fact("open")],
    });
    expect(result).toMatchObject({ status: "output-conflict", reason: "expected-offset" });
    expect(await h.adapter.getProducerState("derived", h.lane.producerId)).toBeUndefined();
    expect(await rawValues(h.adapter)).toEqual([fact("foreign")]);
    await h.close();
  });

  test("surfaces stale epoch and sequence gap without automatic epoch changes", async () => {
    const h = await harness();
    const initial = await ready(h);
    const bumpedLane = { ...h.lane, producerEpoch: 42 };
    const bumpedMeta = createLineageEvent(bumpedLane, {
      sourceThrough: "00000000",
      nextProducerSeq: 1,
    });
    await h.client.stream("derived").appendJsonBatch([bumpedMeta as unknown as JsonValue], {
      expectedOffset: initial.targetOffset,
      producer: { producerId: h.lane.producerId, producerEpoch: 42, producerSeq: 0 },
    });
    const stale = await appendDerivedStateBatch({
      target: h.target,
      lane: h.lane,
      previous: initial,
      sourceThrough: "00000001",
      facts: [fact("open")],
    });
    expect(stale).toEqual({ status: "stale-epoch", currentEpoch: 42 });
    expect(await h.adapter.getProducerState("derived", h.lane.producerId)).toEqual({
      epoch: 42,
      lastSeq: 0,
    });
    await h.close();

    const gapHarness = await harness();
    const gapInitial = await ready(gapHarness);
    const accepted = await appendDerivedStateBatch({
      target: gapHarness.target,
      lane: gapHarness.lane,
      previous: gapInitial,
      sourceThrough: "00000001",
      facts: [fact("open")],
    });
    if (accepted.status !== "appended") throw new Error("expected appended");
    const gap = await appendDerivedStateBatch({
      target: gapHarness.target,
      lane: gapHarness.lane,
      previous: { ...accepted.checkpoint, nextProducerSeq: 2 },
      sourceThrough: "00000002",
      facts: [fact("closed")],
    });
    expect(gap).toEqual({ status: "producer-gap", expectedSeq: 1, receivedSeq: 2 });
    expect(await rawValues(gapHarness.adapter)).toHaveLength(2);
    await gapHarness.close();
  });

  test("programmer misuse stops before request and malformed/incompatible metadata halts recovery", async () => {
    const h = await harness();
    const initial = await ready(h);
    await expect(
      appendDerivedStateBatch({
        target: h.target,
        lane: h.lane,
        previous: initial,
        sourceThrough: "00000001",
        facts: [
          {
            type: "__streamsy.application",
            key: "bad",
            value: {},
            headers: { operation: "upsert" },
          },
        ],
      }),
    ).rejects.toThrow(/reserved/);
    expect(await rawValues(h.adapter)).toEqual([]);
    expect(await h.adapter.getProducerState("derived", h.lane.producerId)).toBeUndefined();

    await h.client.stream("derived").appendJsonBatch([
      {
        type: MESH_LINEAGE_TYPE,
        key: MESH_LINEAGE_KEY,
        value: { format: "broken" },
        headers: { operation: "upsert" },
      },
    ]);
    const before = await h.client.stream("derived").head();
    expect(await recoverDerivedState(h.target, h.lane)).toMatchObject({
      status: "malformed-output",
    });
    expect(await h.client.stream("derived").head()).toEqual(before);
    await h.close();

    const incompatible = await harness();
    const otherLane = await deriveProducerLane({
      ...incompatible.lane,
      outputGeneration: "generation-2",
    });
    await incompatible.client.stream("derived").appendJsonBatch([
      createLineageEvent(otherLane, {
        sourceThrough: "00000001",
        nextProducerSeq: 1,
      }) as unknown as JsonValue,
    ]);
    expect(await recoverDerivedState(incompatible.target, incompatible.lane)).toMatchObject({
      status: "incompatible-output",
    });
    await incompatible.close();
  });
});

test("direct and fetch store byte-identical ordered State event framing with one POST", async () => {
  const directAdapter = createMemoryStorageAdapter();
  const directProtocol = new StreamProtocol({ storage: { adapter: directAdapter } });
  const direct = directProtocolClient(directProtocol);
  await direct.stream("derived").create({ contentType: "application/json" });

  const fetchAdapter = createMemoryStorageAdapter();
  const fetchProtocol = new StreamProtocol({ storage: { adapter: fetchAdapter } });
  const handler = createHttpHandler({ protocol: fetchProtocol, pathPrefix: "/streams" });
  const requests: Request[] = [];
  const fetch = (async (input: Parameters<typeof globalThis.fetch>[0], init?: RequestInit) => {
    const request = new Request(input, init);
    requests.push(request.clone());
    return handler.fetch(request);
  }) as typeof globalThis.fetch;
  const remote = officialProtocolClient({
    urlFor: (id) => protocolPathUrl("https://stream.test/streams", id),
    fetch,
    backoffOptions: noRetry,
    warnOnHttp: false,
  });
  await remote.stream("derived").create({ contentType: "application/json" });
  const events = [fact("open"), fact("closed")];
  await direct.stream("derived").appendJsonBatch(events);
  const before = requests.length;
  await remote.stream("derived").appendJsonBatch(events);
  expect(requests.slice(before).map((request) => request.method)).toEqual(["POST"]);
  const directBytes = (await directAdapter.listMessages("derived")).map((message) => message.data);
  const fetchBytes = (await fetchAdapter.listMessages("derived")).map((message) => message.data);
  expect(fetchBytes).toEqual(directBytes);
  await direct.close();
  await remote.close();
});

function loseFirstAppendResponse(client: StreamProtocolClient): StreamProtocolClient {
  let lose = true;
  return {
    stream(streamId: string): StreamProtocolHandle {
      const delegate = client.stream(streamId);
      return {
        id: delegate.id,
        head: (options) => delegate.head(options),
        create: (options) => delegate.create(options),
        append: (data, options) => delegate.append(data, options),
        close: (options) => delegate.close(options),
        read: (options) => delegate.read(options),
        async appendJsonBatch(
          items: readonly JsonValue[],
          options?: AppendJsonBatchOptions,
        ): Promise<ClientAppendResult> {
          const result = await delegate.appendJsonBatch(items, options);
          if (lose) {
            lose = false;
            return {
              status: "error",
              code: "transport",
              message: "response lost after commit",
              retryable: true,
            };
          }
          return result;
        },
      };
    },
    close: (reason) => client.close(reason),
  };
}
