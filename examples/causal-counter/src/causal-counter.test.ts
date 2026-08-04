import { officialProtocolClient, protocolPathUrl } from "@streamsy/client";
import {
  StreamProtocol,
  createHttpHandler,
  createMemoryStorageAdapter,
  directProtocolClient,
  type StorageAdapter,
  type StreamProtocolClient,
} from "@streamsy/core";
import { bindStream, type BoundAppendResult, type StreamBinding } from "@streamsy/experimental/binding";
import { sourceAck, streamIdentity } from "@streamsy/experimental/causal";
import { AppendStreamsLive, ReadStreamsLive } from "@streamsy/experimental/effect";
import { DerivedRecoveryLive, deriveProducerLane, type ProducerLane } from "@streamsy/experimental/ivm-mesh";
import { Layer, ManagedRuntime } from "effect";
import { afterEach, describe, expect, test } from "vitest";
import { EagerCounterConsumer, appendCounterIncrement, projectCounterIncrements } from "./causal-counter.ts";

interface Harness {
  readonly adapter: StorageAdapter;
  readonly client: StreamProtocolClient;
  readonly source: StreamBinding;
  readonly target: StreamBinding;
  readonly lane: ProducerLane;
  append(delta: number): Promise<BoundAppendResult>;
  project(): Promise<unknown>;
  consume(consumer: EagerCounterConsumer): Promise<void>;
  close(): Promise<void>;
}

const active: Harness[] = [];
afterEach(async () => {
  await Promise.all(active.splice(0).map((h) => h.close()));
});

describe("causal counter — Effect runtime edge", () => {
  test.each(["direct", "fetch"] as const)("proves an acknowledgement with visible state and lineage over %s", async (transport) => {
    const h = await harness(transport);
    const consumer = new EagerCounterConsumer();
    const appended = await h.append(3);
    if (appended.status !== "appended") throw new Error("expected append");
    expect(consumer.syncedThrough(appended.ack)).toEqual({ status: "not-yet" });
    expect(await h.project()).toMatchObject({ status: "caught-up", batches: 1 });
    await h.consume(consumer);
    expect(consumer.counterValue("visits")).toBe(3);
    expect(consumer.syncedThrough(appended.ack)).toEqual({ status: "proven" });
    expect(consumer.syncedThrough(sourceAck(streamIdentity("other"), appended.ack.position))).toEqual({ status: "incomparable" });
  });

  test("runtime reuse, projector restart, and consumer restart are byte-stable", async () => {
    const h = await harness("direct");
    const appended = await h.append(2);
    if (appended.status !== "appended") throw new Error("expected append");
    expect(await h.project()).toMatchObject({ status: "caught-up", batches: 1 });
    const consumer = new EagerCounterConsumer();
    await h.consume(consumer);
    const snapshot = consumer.snapshot();
    const before = await h.adapter.listMessages(h.target.streamId);
    expect(await h.project()).toMatchObject({ status: "caught-up", batches: 0 });
    const restarted = new EagerCounterConsumer(snapshot);
    await h.consume(restarted);
    expect(restarted.snapshot()).toEqual(snapshot);
    expect(restarted.syncedThrough(appended.ack)).toEqual({ status: "proven" });
    expect(await h.adapter.listMessages(h.target.streamId)).toEqual(before);
  });
});

async function harness(transport: "direct" | "fetch"): Promise<Harness> {
  const adapter = createMemoryStorageAdapter();
  const protocol = new StreamProtocol({ storage: { adapter } });
  const routedFetch = Object.assign(
    (input: Parameters<typeof globalThis.fetch>[0], init?: RequestInit) =>
      createHttpHandler({ protocol, pathPrefix: "/streams" }).fetch(new Request(input, init)),
    { preconnect: globalThis.fetch.preconnect },
  );
  const client = transport === "direct" ? directProtocolClient(protocol) : officialProtocolClient({
    urlFor: (id) => protocolPathUrl("https://counter.test/streams", id),
    fetch: routedFetch,
    backoffOptions: { initialDelay: 1, maxDelay: 1, multiplier: 1, maxRetries: 0 },
    warnOnHttp: false,
  });
  const sourceIdentity = streamIdentity("counter-facts");
  const targetIdentity = streamIdentity("counter-state");
  const source = bindStream({ identity: sourceIdentity, client, streamId: "counter-facts" });
  const target = bindStream({ identity: targetIdentity, client, streamId: "counter-state" });
  const lane = await deriveProducerLane({ processorId: "counter", processorVersion: "1", outputGeneration: "1", source: sourceIdentity, target: targetIdentity, producerEpoch: 1 });
  await client.stream(source.streamId).create({ contentType: "application/json" });
  await client.stream(target.streamId).create({ contentType: "application/json" });
  const recoveryLayer = DerivedRecoveryLive.pipe(Layer.provide(ReadStreamsLive));
  const runtime = ManagedRuntime.make(Layer.merge(Layer.merge(ReadStreamsLive, AppendStreamsLive), recoveryLayer));
  const result = {
    adapter,
    client,
    source,
    target,
    lane,
    append: (delta: number) => runtime.runPromise(appendCounterIncrement(source, { counterId: "visits", delta })),
    project: () => runtime.runPromise(projectCounterIncrements({ source, target, lane })),
    consume: (consumer: EagerCounterConsumer) => runtime.runPromise(consumer.catchUp(target)),
    async close() { await runtime.dispose(); await client.close(); },
  };
  active.push(result);
  return result;
}
