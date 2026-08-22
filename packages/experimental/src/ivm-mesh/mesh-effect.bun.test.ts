/* oxlint-disable effecttsgo/async-function -- This Bun SQLite integration suite has one Promise-returning runner and adapter execution model. */
// oxlint-disable-next-line effecttsgo/node-builtin-import -- The Bun SQLite adapter creates its isolated temporary database directory through Node fs.
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
// oxlint-disable-next-line effecttsgo/node-builtin-import -- The Bun SQLite adapter constructs its temporary database filename through Node path.
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import { StreamProtocol, directProtocolClient } from "@streamsy/core";
import { createSqliteStorageAdapter } from "@streamsy/storage-sqlite";
import { Effect, Layer } from "effect";
import { bindStream } from "../binding.ts";
import { streamIdentity } from "../causal.ts";
import { AppendStreamsLive, ReadStreamsLive } from "../effect/streams.ts";
import { provideTestLayers } from "../effect/test-layers.ts";
import { DerivedRecoveryLive } from "./derived-append.ts";
import { deriveProducerLane } from "./lane.ts";
import { catchUp } from "./projection.ts";

const MeshTestLive = DerivedRecoveryLive.pipe(
  Layer.provide(ReadStreamsLive),
  Layer.merge(ReadStreamsLive),
  Layer.merge(AppendStreamsLive),
);

describe("Effect-first mesh — SQLite", () => {
  test("reopens and resumes from durable lineage without duplicate output", async () => {
    const filename = join(mkdtempSync(join(tmpdir(), "streamsy-effect-mesh-")), "state.sqlite");
    const first = await makeHarness(filename);
    await first.client.stream("source").create({ contentType: "application/json" });
    await first.client.stream("target").create({ contentType: "application/json" });
    const appended = await first.client.stream("source").appendJsonBatch([1, 2]);
    if (appended.status !== "appended") throw new Error("expected append");
    expect(await first.run()).toMatchObject({
      status: "caught-up",
      batches: 1,
      checkpoint: { sourceThrough: appended.offset },
    });
    const stored = await first.adapter.listMessages("target");
    await first.close();

    const reopened = await makeHarness(filename);
    expect(await reopened.run()).toMatchObject({
      status: "caught-up",
      batches: 0,
      checkpoint: { sourceThrough: appended.offset },
    });
    expect(await reopened.adapter.listMessages("target")).toEqual(stored);
    await reopened.close();
  });
});

async function makeHarness(filename: string) {
  const adapter = createSqliteStorageAdapter({ filename });
  const client = directProtocolClient(new StreamProtocol({ storage: { adapter } }));
  const sourceIdentity = streamIdentity("source");
  const targetIdentity = streamIdentity("target");
  const source = bindStream({ identity: sourceIdentity, client, streamId: "source" });
  const target = bindStream({ identity: targetIdentity, client, streamId: "target" });
  const lane = await deriveProducerLane({
    processorId: "sqlite",
    processorVersion: "1",
    outputGeneration: "1",
    source: sourceIdentity,
    target: targetIdentity,
    producerEpoch: 1,
  });
  return {
    adapter,
    client,
    run: () =>
      Effect.runPromise(
        catchUp({
          source,
          target,
          lane,
          limits: { maxItems: 100, maxPages: 100, maxBatches: 100, maxBytes: 100_000 },
          decode(batch) {
            if (batch.kind !== "json") throw new Error("expected JSON");
            return batch.items;
          },
          reduce(items) {
            return items.map((item) => ({
              type: "value",
              key: typeof item === "object" ? JSON.stringify(item) : String(item),
              value: item,
              headers: { operation: "upsert" },
            }));
          },
        }).pipe((effect) => provideTestLayers(effect, MeshTestLive)),
      ),
    async close() {
      await client.close();
      adapter.close();
    },
  };
}
