import { StreamProtocol, createMemoryStorageAdapter, directProtocolClient } from "@streamsy/core";
import { bindStream } from "@streamsy/experimental/binding";
import { streamIdentity } from "@streamsy/experimental/causal";
import { deriveProducerLane } from "@streamsy/experimental/ivm-mesh";
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

await client.stream(source.streamId).create({ contentType: "application/json" });
await client.stream(target.streamId).create({ contentType: "application/json" });
const appended = await appendCounterIncrement(source, { counterId: "visits", delta: 1 });
assert(appended.status === "appended", "source increment must append");
const projected = await projectCounterIncrements({ source, target, lane });
assert(projected.status === "caught-up", "projection must catch up");
const consumer = new EagerCounterConsumer();
await consumer.catchUp(target);
assert(consumer.counterValue("visits") === 1, "counter row must be visible");
assert(consumer.syncedThrough(appended.ack).status === "proven", "lineage must prove the ack");
await client.close();

console.log(`causal-counter smoke passed at source token ${appended.ack.position}`);

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
