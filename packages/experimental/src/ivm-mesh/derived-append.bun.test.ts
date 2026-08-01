import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import {
  StreamProtocol,
  directProtocolClient,
  type AppendJsonBatchOptions,
  type ClientAppendResult,
  type JsonValue,
  type StreamProtocolClient,
  type StreamProtocolHandle,
} from "@streamsy/core";
import { createSqliteStorageAdapter, type SqliteStorageAdapter } from "@streamsy/storage-sqlite";
import { bindStream, type StreamBinding } from "../binding.ts";
import { streamIdentity } from "../causal.ts";
import { appendDerivedStateBatch, recoverDerivedState } from "./derived-append.ts";
import { deriveProducerLane, type ProducerLane } from "./lane.ts";
import { MESH_LINEAGE_KEY, MESH_LINEAGE_TYPE, createLineageEvent } from "./state-meta.ts";

const decoder = new TextDecoder();

type SqliteHarness = {
  adapter: SqliteStorageAdapter;
  protocol: StreamProtocol;
  client: StreamProtocolClient;
  target: StreamBinding;
  lane: ProducerLane;
  close(): Promise<void>;
};

async function harness(filename?: string): Promise<SqliteHarness> {
  const adapter = createSqliteStorageAdapter(filename ? { filename } : {});
  const protocol = new StreamProtocol({ storage: { adapter } });
  const client = directProtocolClient(protocol);
  const targetIdentity = streamIdentity("orders-by-status");
  const target = bindStream({ identity: targetIdentity, client, streamId: "derived" });
  const lane = await deriveProducerLane({
    processorId: "orders-by-status",
    processorVersion: "1.0.0",
    outputGeneration: "generation-1",
    source: streamIdentity("orders"),
    target: targetIdentity,
    producerEpoch: 41,
  });
  await client.stream("derived").create({ contentType: "application/json" });
  return {
    adapter,
    protocol,
    client,
    target,
    lane,
    async close() {
      await client.close();
      adapter.close();
    },
  };
}

function fact(status: string): JsonValue {
  return {
    type: "order",
    key: "o-1",
    value: { id: "o-1", status },
    headers: { operation: "upsert" },
  };
}

async function ready(h: SqliteHarness) {
  const recovered = await recoverDerivedState(h.target, h.lane);
  if (recovered.status !== "ready") throw new Error(`expected ready, got ${recovered.status}`);
  return recovered;
}

async function values(h: SqliteHarness): Promise<unknown[]> {
  return (await h.adapter.listMessages("derived")).map(
    (message) => JSON.parse(decoder.decode(message.data)) as unknown,
  );
}

describe("derived State append — SQLite fault evidence", () => {
  test("one atomic boundary, one bounded lane, fixed-epoch restart, and file reopen", async () => {
    const directory = mkdtempSync(join(tmpdir(), "streamsy-derived-state-"));
    const filename = join(directory, "state.sqlite");
    const first = await harness(filename);
    const commits: string[] = [];
    first.protocol.onAfterCommit((event) => commits.push(event.offset));
    const initial = await ready(first);
    const appended = await appendDerivedStateBatch({
      target: first.target,
      lane: first.lane,
      previous: initial,
      sourceThrough: "00000001",
      facts: [fact("open")],
    });
    expect(appended.status).toBe("appended");
    if (appended.status !== "appended") throw new Error("expected appended");
    expect(commits).toEqual([appended.offset]);
    expect(await values(first)).toEqual([
      fact("open"),
      expect.objectContaining({
        type: MESH_LINEAGE_TYPE,
        key: MESH_LINEAGE_KEY,
        value: expect.objectContaining({ sourceThrough: "00000001", nextProducerSeq: 1 }),
      }),
    ]);
    expect(
      first.adapter.state.db
        .query<{ count: number }, []>("select count(*) as count from streamsy_producers")
        .get()!.count,
    ).toBe(1);
    await first.close();

    const reopened = await harness(filename);
    expect(await ready(reopened)).toEqual(appended.checkpoint);
    expect(await reopened.adapter.getProducerState("derived", reopened.lane.producerId)).toEqual({
      epoch: 41,
      lastSeq: 0,
    });
    const second = await appendDerivedStateBatch({
      target: reopened.target,
      lane: reopened.lane,
      previous: appended.checkpoint,
      sourceThrough: "00000002",
      facts: [],
    });
    expect(second).toMatchObject({ status: "appended", checkpoint: { nextProducerSeq: 2 } });
    expect(
      reopened.adapter.state.db
        .query<{ count: number }, []>("select count(*) as count from streamsy_producers")
        .get()!.count,
    ).toBe(1);
    await reopened.close();
  });

  test("lost response and changed retry bytes reconcile only as sequence already accepted", async () => {
    const h = await harness();
    const initial = await ready(h);
    const lossy = bindStream({
      identity: h.target.identity,
      streamId: h.target.streamId,
      client: loseFirstAppendResponse(h.client),
    });
    expect(
      await appendDerivedStateBatch({
        target: lossy,
        lane: h.lane,
        previous: initial,
        sourceThrough: "00000001",
        facts: [fact("open")],
      }),
    ).toMatchObject({ status: "error", code: "transport" });
    const retry = await appendDerivedStateBatch({
      target: lossy,
      lane: h.lane,
      previous: initial,
      sourceThrough: "00000001",
      facts: [fact("changed")],
    });
    expect(retry).toMatchObject({ status: "sequence-already-accepted" });
    expect(JSON.stringify(retry)).not.toContain("verified");
    expect(await values(h)).toHaveLength(2);
    expect((await values(h))[0]).toEqual(fact("open"));
    await h.close();
  });

  test("contention, stale epoch, and sequence gap are typed and write nothing", async () => {
    const contended = await harness();
    const initial = await ready(contended);
    await contended.client.stream("derived").appendJsonBatch([fact("foreign")]);
    expect(
      await appendDerivedStateBatch({
        target: contended.target,
        lane: contended.lane,
        previous: initial,
        sourceThrough: "00000001",
        facts: [fact("open")],
      }),
    ).toMatchObject({ status: "output-conflict", reason: "expected-offset" });
    expect(await contended.adapter.getProducerState("derived", contended.lane.producerId)).toBe(
      undefined,
    );
    expect(await values(contended)).toEqual([fact("foreign")]);
    await contended.close();

    const staleHarness = await harness();
    const staleInitial = await ready(staleHarness);
    const bumped = { ...staleHarness.lane, producerEpoch: 42 };
    await staleHarness.client.stream("derived").appendJsonBatch(
      [
        createLineageEvent(bumped, {
          sourceThrough: "00000000",
          nextProducerSeq: 1,
        }) as unknown as JsonValue,
      ],
      {
        expectedOffset: staleInitial.targetOffset,
        producer: { producerId: staleHarness.lane.producerId, producerEpoch: 42, producerSeq: 0 },
      },
    );
    expect(
      await appendDerivedStateBatch({
        target: staleHarness.target,
        lane: staleHarness.lane,
        previous: staleInitial,
        sourceThrough: "00000001",
        facts: [fact("open")],
      }),
    ).toEqual({ status: "stale-epoch", currentEpoch: 42 });
    await staleHarness.close();

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
    expect(
      await appendDerivedStateBatch({
        target: gapHarness.target,
        lane: gapHarness.lane,
        previous: { ...accepted.checkpoint, nextProducerSeq: 2 },
        sourceThrough: "00000002",
        facts: [fact("closed")],
      }),
    ).toEqual({ status: "producer-gap", expectedSeq: 1, receivedSeq: 2 });
    expect(await values(gapHarness)).toHaveLength(2);
    await gapHarness.close();
  });

  test("before-request misuse and malformed or incompatible metadata halt without advance", async () => {
    const h = await harness();
    const initial = await ready(h);
    expect(() =>
      appendDerivedStateBatch({
        target: h.target,
        lane: h.lane,
        previous: initial,
        sourceThrough: "00000001",
        facts: [
          {
            type: "__streamsy.bad",
            key: "bad",
            value: {},
            headers: { operation: "upsert" },
          },
        ],
      }),
    ).toThrow(/reserved/);
    expect(await values(h)).toEqual([]);
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
    const other = await deriveProducerLane({
      ...incompatible.lane,
      outputGeneration: "generation-2",
    });
    await incompatible.client.stream("derived").appendJsonBatch([
      createLineageEvent(other, {
        sourceThrough: "00000001",
        nextProducerSeq: 1,
      }) as unknown as JsonValue,
    ]);
    const incompatibleBefore = await incompatible.client.stream("derived").head();
    expect(await recoverDerivedState(incompatible.target, incompatible.lane)).toMatchObject({
      status: "incompatible-output",
    });
    expect(await incompatible.client.stream("derived").head()).toEqual(incompatibleBefore);
    await incompatible.close();
  });
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
          if (!lose) return result;
          lose = false;
          return {
            status: "error",
            code: "transport",
            message: "response lost after commit",
            retryable: true,
          };
        },
      };
    },
    close: (reason) => client.close(reason),
  };
}
