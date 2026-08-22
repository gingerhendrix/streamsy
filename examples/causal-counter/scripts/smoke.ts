import { StreamProtocol, createMemoryStorageAdapter, directProtocolClient } from "@streamsy/core";
import { bindStream } from "@streamsy/experimental/binding";
import { streamIdentity } from "@streamsy/experimental/causal";
import { AppendStreamsLive, ReadStreamsLive } from "@streamsy/experimental/effect";
import { DerivedRecoveryLive, deriveProducerLane } from "@streamsy/experimental/ivm-mesh";
import { Layer, ManagedRuntime } from "effect";
import {
  EagerCounterConsumer,
  appendCounterIncrement,
  projectCounterIncrements,
} from "../src/causal-counter.ts";

const adapter = createMemoryStorageAdapter();
const client = directProtocolClient(new StreamProtocol({ storage: { adapter } }));
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

const recoveryLayer = DerivedRecoveryLive.pipe(Layer.provide(ReadStreamsLive));
const streamLayer = Layer.merge(ReadStreamsLive, AppendStreamsLive);
const runtime = ManagedRuntime.make(Layer.merge(streamLayer, recoveryLayer));

let sourceToken = "";
try {
  await client.stream(source.streamId).create({ contentType: "application/json" });
  await client.stream(target.streamId).create({ contentType: "application/json" });
  const appended = await runtime.runPromise(
    appendCounterIncrement(source, { counterId: "visits", delta: 1 }),
  );
  assert(appended.status === "appended", "source increment must append");
  sourceToken = appended.ack.position;
  const projected = await runtime.runPromise(projectCounterIncrements({ source, target, lane }));
  assert(projected.status === "caught-up", "projection must catch up");
  const consumer = new EagerCounterConsumer();
  await runtime.runPromise(consumer.catchUp(target));
  assert(consumer.counterValue("visits") === 1, "counter row must be visible");
  assert(consumer.syncedThrough(appended.ack).status === "proven", "lineage must prove the ack");
} finally {
  await runtime.dispose();
  await client.close();
}

// oxlint-disable-next-line effecttsgo/global-console -- This standalone Bun smoke executable reports its single result line directly to the invoking terminal.
console.log(`causal-counter smoke passed at source token ${sourceToken}`);

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
