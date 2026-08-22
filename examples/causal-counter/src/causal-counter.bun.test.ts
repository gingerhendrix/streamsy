/* oxlint-disable effecttsgo/async-function -- Bun's test runner owns these Promise-native callbacks; the example's workflows are executed through one ManagedRuntime per host. */
// oxlint-disable-next-line effecttsgo/node-builtin-import -- The SQLite host needs a real on-disk database file, created through the Node-compatible filesystem API.
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
// oxlint-disable-next-line effecttsgo/node-builtin-import -- The SQLite host builds that database path with the Node-compatible path API.
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import { StreamProtocol, directProtocolClient } from "@streamsy/core";
import { bindStream } from "@streamsy/experimental/binding";
import { streamIdentity } from "@streamsy/experimental/causal";
import { AppendStreamsLive, ReadStreamsLive } from "@streamsy/experimental/effect";
import { DerivedRecoveryLive, deriveProducerLane } from "@streamsy/experimental/ivm-mesh";
import { createSqliteStorageAdapter } from "@streamsy/storage-sqlite";
import { Layer, ManagedRuntime } from "effect";
import {
  EagerCounterConsumer,
  appendCounterIncrement,
  projectCounterIncrements,
} from "./causal-counter.ts";

describe("causal counter — SQLite", () => {
  test("one runtime per host reopens durable lineage and resumes without duplicate rows", async () => {
    const filename = join(
      mkdtempSync(join(tmpdir(), "streamsy-causal-counter-")),
      "counter.sqlite",
    );
    const first = await sqliteHarness(filename);
    await first.create();
    const appended = await first.append(2);
    if (appended.status !== "appended") throw new Error("expected append");
    expect(await first.project()).toMatchObject({ status: "caught-up", batches: 1 });
    const consumer = new EagerCounterConsumer();
    await first.consume(consumer);
    const snapshot = consumer.snapshot();
    const stored = await first.adapter.listMessages("target");
    await first.close();

    const reopened = await sqliteHarness(filename);
    expect(await reopened.project()).toMatchObject({ status: "caught-up", batches: 0 });
    const restarted = new EagerCounterConsumer(snapshot);
    await reopened.consume(restarted);
    expect(restarted.counterValue("visits")).toBe(2);
    expect(restarted.syncedThrough(appended.ack)).toEqual({ status: "proven" });
    expect(await reopened.adapter.listMessages("target")).toEqual(stored);

    const later = await reopened.append(3);
    if (later.status !== "appended") throw new Error("expected later append");
    expect(await reopened.project()).toMatchObject({ status: "caught-up", batches: 1 });
    await reopened.consume(restarted);
    expect(restarted.counterValue("visits")).toBe(5);
    expect(restarted.syncedThrough(later.ack)).toEqual({ status: "proven" });
    await reopened.close();
  });
});

async function sqliteHarness(filename: string) {
  const adapter = createSqliteStorageAdapter({ filename });
  const client = directProtocolClient(new StreamProtocol({ storage: { adapter } }));
  const sourceIdentity = streamIdentity("source");
  const targetIdentity = streamIdentity("target");
  const source = bindStream({ identity: sourceIdentity, client, streamId: "source" });
  const target = bindStream({ identity: targetIdentity, client, streamId: "target" });
  const lane = await deriveProducerLane({
    processorId: "counter",
    processorVersion: "1",
    outputGeneration: "1",
    source: sourceIdentity,
    target: targetIdentity,
    producerEpoch: 1,
  });
  const recoveryLayer = DerivedRecoveryLive.pipe(Layer.provide(ReadStreamsLive));
  const runtime = ManagedRuntime.make(
    Layer.merge(Layer.merge(ReadStreamsLive, AppendStreamsLive), recoveryLayer),
  );
  return {
    adapter,
    create: async () => {
      await client.stream("source").create({ contentType: "application/json" });
      await client.stream("target").create({ contentType: "application/json" });
    },
    append: (delta: number) =>
      runtime.runPromise(appendCounterIncrement(source, { counterId: "visits", delta })),
    project: () => runtime.runPromise(projectCounterIncrements({ source, target, lane })),
    consume: (consumer: EagerCounterConsumer) => runtime.runPromise(consumer.catchUp(target)),
    async close() {
      await runtime.dispose();
      await client.close();
      adapter.close();
    },
  };
}
