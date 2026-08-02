import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import { StreamProtocol, directProtocolClient } from "@streamsy/core";
import { bindStream } from "@streamsy/experimental/binding";
import { streamIdentity } from "@streamsy/experimental/causal";
import { deriveProducerLane } from "@streamsy/experimental/ivm-mesh";
import { createSqliteStorageAdapter } from "@streamsy/storage-sqlite";
import {
  EagerCounterConsumer,
  appendCounterIncrement,
  projectCounterIncrements,
} from "./causal-counter.ts";

describe("causal counter — SQLite", () => {
  test("reopens projector and eager consumer with stable rows, lineage, and resume position", async () => {
    const directory = mkdtempSync(join(tmpdir(), "streamsy-causal-counter-"));
    const filename = join(directory, "counter.sqlite");
    const first = await sqliteHarness(filename);
    await first.client.stream(first.source.streamId).create({ contentType: "application/json" });
    await first.client.stream(first.target.streamId).create({ contentType: "application/json" });
    const initial = await appendCounterIncrement(first.source, { counterId: "visits", delta: 2 });
    if (initial.status !== "appended") throw new Error("expected source append");
    expect(await projectCounterIncrements(first)).toMatchObject({
      status: "caught-up",
      batches: 1,
      checkpoint: { sourceThrough: initial.ack.position },
    });
    const firstConsumer = new EagerCounterConsumer();
    await firstConsumer.catchUp(first.target);
    const snapshot = firstConsumer.snapshot();
    const storedBeforeReopen = await first.adapter.listMessages(first.target.streamId);
    await first.close();

    const reopened = await sqliteHarness(filename);
    expect(await projectCounterIncrements(reopened)).toMatchObject({
      status: "caught-up",
      batches: 0,
      checkpoint: { sourceThrough: initial.ack.position },
    });
    const reopenedConsumer = new EagerCounterConsumer(snapshot);
    await reopenedConsumer.catchUp(reopened.target);
    expect(reopenedConsumer.snapshot()).toEqual(snapshot);
    expect(reopenedConsumer.counterValue("visits")).toBe(2);
    expect(reopenedConsumer.syncedThrough(initial.ack)).toEqual({ status: "proven" });
    expect(await reopened.adapter.listMessages(reopened.target.streamId)).toEqual(
      storedBeforeReopen,
    );

    const later = await appendCounterIncrement(reopened.source, { counterId: "visits", delta: 3 });
    if (later.status !== "appended") throw new Error("expected later source append");
    expect(await projectCounterIncrements(reopened)).toMatchObject({
      status: "caught-up",
      batches: 1,
      checkpoint: { sourceThrough: later.ack.position },
    });
    await reopenedConsumer.catchUp(reopened.target);
    expect(reopenedConsumer.counterValue("visits")).toBe(5);
    expect(reopenedConsumer.syncedThrough(later.ack)).toEqual({ status: "proven" });
    await reopened.close();
  });
});

async function sqliteHarness(filename: string) {
  const adapter = createSqliteStorageAdapter({ filename });
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
  return {
    adapter,
    client,
    source,
    target,
    lane,
    async close() {
      await client.close();
      adapter.close();
    },
  };
}
