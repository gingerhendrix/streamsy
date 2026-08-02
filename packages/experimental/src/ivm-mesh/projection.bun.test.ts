import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import { StreamProtocol, directProtocolClient, type JsonValue } from "@streamsy/core";
import { createSqliteStorageAdapter } from "@streamsy/storage-sqlite";
import { bindStream } from "../binding.ts";
import { streamIdentity } from "../causal.ts";
import { recoverDerivedState } from "./derived-append.ts";
import { deriveProducerLane } from "./lane.ts";
import { catchUp } from "./projection.ts";

const decoder = new TextDecoder();
const limits = { maxItems: 100, maxPages: 100, maxBatches: 100, maxBytes: 100_000 };

describe("bounded one-source projection — SQLite", () => {
  test("reopens, resumes after durable lineage, and makes a no-op restart byte-stable", async () => {
    const directory = mkdtempSync(join(tmpdir(), "streamsy-projection-"));
    const filename = join(directory, "state.sqlite");
    const first = await sqliteHarness(filename);
    await first.client.stream("source").create({ contentType: "application/json" });
    await first.client.stream("derived").create({ contentType: "application/json" });
    const initialSource = await first.client.stream("source").appendJsonBatch([1, 2]);
    if (initialSource.status !== "appended") throw new Error("expected source append");
    expect(await catchUp(first.options())).toMatchObject({
      status: "caught-up",
      batches: 1,
      checkpoint: { sourceThrough: initialSource.offset, nextProducerSeq: 1 },
    });
    await first.close();

    const reopened = await sqliteHarness(filename);
    const laterSource = await reopened.client.stream("source").appendJsonBatch([3]);
    if (laterSource.status !== "appended") throw new Error("expected source append");
    expect(await catchUp(reopened.options())).toMatchObject({
      status: "caught-up",
      batches: 1,
      checkpoint: { sourceThrough: laterSource.offset, nextProducerSeq: 2 },
    });
    const beforeRestart = await reopened.adapter.listMessages("derived");
    expect(await catchUp(reopened.options())).toMatchObject({
      status: "caught-up",
      batches: 0,
      checkpoint: { sourceThrough: laterSource.offset, nextProducerSeq: 2 },
    });
    expect(await reopened.adapter.listMessages("derived")).toEqual(beforeRestart);
    expect(await recoverDerivedState(reopened.target, reopened.lane)).toMatchObject({
      status: "ready",
      sourceThrough: laterSource.offset,
      nextProducerSeq: 2,
    });
    const values = beforeRestart.map((message) => JSON.parse(decoder.decode(message.data)));
    expect(values.filter((value) => value.type === "order").map((value) => value.key)).toEqual([
      "o-1",
      "o-2",
      "o-3",
    ]);
    await reopened.close();
  });

  test("full replay over fixed source boundaries produces identical rows and lineage", async () => {
    const directory = mkdtempSync(join(tmpdir(), "streamsy-projection-replay-"));
    const h = await sqliteHarness(join(directory, "state.sqlite"));
    await h.client.stream("source").create({ contentType: "application/json" });
    await h.client.stream("derived").create({ contentType: "application/json" });
    await h.client.stream("derived-replay").create({ contentType: "application/json" });
    await h.client.stream("source").appendJsonBatch([1, 2, 3]);
    expect(await catchUp(h.options())).toMatchObject({ status: "caught-up", batches: 1 });

    const replayTarget = bindStream({
      identity: h.target.identity,
      client: h.client,
      streamId: "derived-replay",
    });
    expect(await catchUp({ ...h.options(), target: replayTarget })).toMatchObject({
      status: "caught-up",
      batches: 1,
    });
    const decodeValues = async (streamId: string) =>
      (await h.adapter.listMessages(streamId)).map((message) =>
        JSON.parse(decoder.decode(message.data)),
      );
    expect(await decodeValues("derived-replay")).toEqual(await decodeValues("derived"));
    await h.close();
  });
});

async function sqliteHarness(filename: string) {
  const adapter = createSqliteStorageAdapter({ filename });
  const client = directProtocolClient(new StreamProtocol({ storage: { adapter } }));
  const sourceIdentity = streamIdentity("orders");
  const targetIdentity = streamIdentity("orders-by-status");
  const source = bindStream({ identity: sourceIdentity, client, streamId: "source" });
  const target = bindStream({ identity: targetIdentity, client, streamId: "derived" });
  const lane = await deriveProducerLane({
    processorId: "orders-by-status",
    processorVersion: "1.0.0",
    outputGeneration: "generation-1",
    source: sourceIdentity,
    target: targetIdentity,
    producerEpoch: 7,
  });
  return {
    adapter,
    client,
    target,
    lane,
    options: () => ({
      source,
      target,
      lane,
      limits,
      decode(batch: { kind: string; items?: readonly JsonValue[] }) {
        if (batch.kind !== "json" || !batch.items) throw new Error("expected JSON source");
        return batch.items;
      },
      reduce(items: readonly JsonValue[]) {
        return items.map((item) => ({
          type: "order",
          key: `o-${String(item)}`,
          value: { id: `o-${String(item)}`, value: item },
          headers: { operation: "upsert" },
        })) as JsonValue[];
      },
    }),
    async close() {
      await client.close();
      adapter.close();
    },
  };
}
