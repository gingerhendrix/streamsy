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
import { Cause, Effect, Exit, Option } from "effect";
import { bindStream, type StreamBinding } from "../binding.ts";
import { streamIdentity } from "../causal.ts";
import { AppendStreamsLive, ReadStreamsLive } from "../effect/streams.ts";
import { IncompatibleLineage, MalformedLineage, StreamAppendError } from "../effect/errors.ts";
import {
  appendDerivedStateBatch,
  DerivedRecoveryLive,
  recoverDerivedState,
  type RecoveredDerivedState,
} from "./derived-append.ts";
import { deriveProducerLane, type ProducerLane } from "./lane.ts";
import { catchUp } from "./projection.ts";
import { MESH_LINEAGE_TYPE, createLineageEvent } from "./state-meta.ts";

const decoder = new TextDecoder();
const limits = { maxItems: 100, maxPages: 100, maxBatches: 100, maxBytes: 100_000 };

interface SqliteHarness {
  readonly adapter: SqliteStorageAdapter;
  readonly client: StreamProtocolClient;
  readonly source: StreamBinding;
  readonly target: StreamBinding;
  readonly lane: ProducerLane;
  close(): Promise<void>;
}

const provideLive = <A, E, R>(program: Effect.Effect<A, E, R>) =>
  program.pipe(
    Effect.provide(DerivedRecoveryLive),
    Effect.provide(ReadStreamsLive),
    Effect.provide(AppendStreamsLive),
  );

async function harness(filename?: string): Promise<SqliteHarness> {
  const adapter = createSqliteStorageAdapter(filename ? { filename } : {});
  const client = directProtocolClient(new StreamProtocol({ storage: { adapter } }));
  const sourceIdentity = streamIdentity("orders");
  const targetIdentity = streamIdentity("orders-state");
  const source = bindStream({ identity: sourceIdentity, client, streamId: "source" });
  const target = bindStream({ identity: targetIdentity, client, streamId: "target" });
  const lane = await deriveProducerLane({
    processorId: "sqlite-orders",
    processorVersion: "1",
    outputGeneration: "1",
    source: sourceIdentity,
    target: targetIdentity,
    producerEpoch: 7,
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

async function createStreams(h: SqliteHarness) {
  await h.client.stream(h.source.streamId).create({ contentType: "application/json" });
  await h.client.stream(h.target.streamId).create({ contentType: "application/json" });
}

function fact(value: JsonValue): JsonValue {
  const key = typeof value === "object" ? JSON.stringify(value) : String(value);
  return { type: "order", key: `o-${key}`, value, headers: { operation: "upsert" } };
}

function projectionOptions(h: SqliteHarness, target = h.target, lane = h.lane) {
  return {
    source: h.source,
    target,
    lane,
    limits,
    decode(batch: { kind: string; items?: readonly JsonValue[] }) {
      if (batch.kind !== "json" || !batch.items) throw new Error("expected JSON");
      return batch.items;
    },
    reduce(items: readonly JsonValue[]) {
      return items.map(fact);
    },
  };
}

async function run(h: SqliteHarness, target = h.target, lane = h.lane) {
  return Effect.runPromise(provideLive(catchUp(projectionOptions(h, target, lane))));
}

async function ready(h: SqliteHarness, target = h.target, lane = h.lane) {
  const result = await Effect.runPromise(provideLive(recoverDerivedState(target, lane)));
  if (result.status !== "ready") throw new Error(`expected ready, got ${result.status}`);
  return result;
}

async function append(
  h: SqliteHarness,
  previous: RecoveredDerivedState,
  sourceThrough: string,
  facts: JsonValue[],
  target = h.target,
  lane = h.lane,
) {
  return Effect.runPromise(
    provideLive(appendDerivedStateBatch({ target, lane, previous, sourceThrough, facts })),
  );
}

async function values(h: SqliteHarness, streamId = h.target.streamId): Promise<unknown[]> {
  return (await h.adapter.listMessages(streamId)).map((message) =>
    JSON.parse(decoder.decode(message.data)),
  );
}

function typedError<A, E>(exit: Exit.Exit<A, E>): E {
  if (!Exit.isFailure(exit)) throw new Error("expected failure Exit");
  const error = Cause.findErrorOption(exit.cause);
  if (Option.isNone(error)) throw new Error("expected typed error");
  return error.value;
}

describe("Effect-first mesh — SQLite regression evidence", () => {
  test("reopen continues a later boundary with one producer row and stable prior bytes", async () => {
    const filename = join(mkdtempSync(join(tmpdir(), "streamsy-effect-reopen-")), "state.sqlite");
    const first = await harness(filename);
    await createStreams(first);
    const firstSource = await first.client.stream(first.source.streamId).appendJsonBatch([1, 2]);
    if (firstSource.status !== "appended") throw new Error("expected append");
    expect(await run(first)).toMatchObject({
      status: "caught-up",
      batches: 1,
      checkpoint: { sourceThrough: firstSource.offset, nextProducerSeq: 1 },
    });
    const priorBytes = (await first.adapter.listMessages(first.target.streamId)).map(
      (message) => message.data,
    );
    expect(producerRows(first)).toBe(1);
    await first.close();

    const reopened = await harness(filename);
    const later = await reopened.client.stream(reopened.source.streamId).appendJsonBatch([3]);
    if (later.status !== "appended") throw new Error("expected append");
    expect(await run(reopened)).toMatchObject({
      status: "caught-up",
      batches: 1,
      checkpoint: { sourceThrough: later.offset, nextProducerSeq: 2 },
    });
    const allBytes = (await reopened.adapter.listMessages(reopened.target.streamId)).map(
      (message) => message.data,
    );
    expect(allBytes.slice(0, priorBytes.length)).toEqual(priorBytes);
    expect(producerRows(reopened)).toBe(1);
    expect(await run(reopened)).toMatchObject({ status: "caught-up", batches: 0 });
    expect((await values(reopened)).filter(isLineage)).toHaveLength(2);
    await reopened.close();
  });

  test("full replay over fixed source boundaries produces byte-identical rows and lineage", async () => {
    const h = await harness();
    await createStreams(h);
    await h.client.stream("replay").create({ contentType: "application/json" });
    await h.client.stream(h.source.streamId).appendJsonBatch([1, 2, 3]);
    expect(await run(h)).toMatchObject({ status: "caught-up", batches: 1 });
    const replay = bindStream({ ...h.target, streamId: "replay" });
    expect(await run(h, replay)).toMatchObject({ status: "caught-up", batches: 1 });
    expect((await h.adapter.listMessages("replay")).map((message) => message.data)).toEqual(
      (await h.adapter.listMessages(h.target.streamId)).map((message) => message.data),
    );
    await h.close();
  });

  test("lost response is typed, changed retry bytes do not write twice, and reopen recovers", async () => {
    const filename = join(mkdtempSync(join(tmpdir(), "streamsy-effect-lost-")), "state.sqlite");
    const h = await harness(filename);
    await createStreams(h);
    const previous = await ready(h);
    const lossy = bindStream({ ...h.target, client: loseFirstAppendResponse(h.client) });
    const exit = await Effect.runPromiseExit(
      provideLive(
        appendDerivedStateBatch({
          target: lossy,
          lane: h.lane,
          previous,
          sourceThrough: "00000001",
          facts: [fact("accepted")],
        }),
      ),
    );
    expect(typedError(exit)).toBeInstanceOf(StreamAppendError);
    expect(typedError(exit)).toMatchObject({ code: "transport", durability: "unknown" });
    expect(
      await append(h, previous, "00000001", [fact("changed-retry-bytes")], lossy),
    ).toMatchObject({ status: "sequence-already-accepted" });
    expect(await values(h)).toEqual([
      fact("accepted"),
      expect.objectContaining({ type: MESH_LINEAGE_TYPE }),
    ]);
    expect(producerRows(h)).toBe(1);
    await h.close();

    const reopened = await harness(filename);
    expect(await ready(reopened)).toMatchObject({ sourceThrough: "00000001", nextProducerSeq: 1 });
    expect(await values(reopened)).toHaveLength(2);
    expect(producerRows(reopened)).toBe(1);
    await reopened.close();
  });

  test("contention, stale epoch, producer gap, and invalid epoch/sequence preserve prior rows", async () => {
    const contended = await harness();
    await createStreams(contended);
    const initial = await ready(contended);
    await contended.client.stream(contended.target.streamId).appendJsonBatch([fact("foreign")]);
    const contentionBefore = await values(contended);
    expect(await append(contended, initial, "00000001", [fact(1)])).toMatchObject({
      status: "output-conflict",
      reason: "expected-offset",
    });
    expect(await values(contended)).toEqual(contentionBefore);
    await contended.close();

    const stale = await harness();
    await createStreams(stale);
    const staleInitial = await ready(stale);
    const bumped = { ...stale.lane, producerEpoch: 8 };
    await stale.client
      .stream(stale.target.streamId)
      .appendJsonBatch(
        [createLineageEvent(bumped, { sourceThrough: "00000000", nextProducerSeq: 1 })],
        {
          expectedOffset: staleInitial.targetOffset,
          producer: { producerId: stale.lane.producerId, producerEpoch: 8, producerSeq: 0 },
        },
      );
    const staleBefore = await values(stale);
    expect(await append(stale, staleInitial, "00000001", [fact(1)])).toEqual({
      status: "stale-epoch",
      currentEpoch: 8,
    });
    expect(await values(stale)).toEqual(staleBefore);
    await stale.close();

    const gap = await harness();
    await createStreams(gap);
    const accepted = await append(gap, await ready(gap), "00000001", [fact(1)]);
    if (accepted.status !== "appended") throw new Error("expected append");
    const gapBefore = await values(gap);
    expect(
      await append(gap, { ...accepted.checkpoint, nextProducerSeq: 2 }, "00000002", [fact(2)]),
    ).toEqual({ status: "producer-gap", expectedSeq: 1, receivedSeq: 2 });
    expect(await values(gap)).toEqual(gapBefore);
    await gap.close();

    const invalid = await harness();
    await createStreams(invalid);
    const invalidAccepted = await append(invalid, await ready(invalid), "00000000", [fact(0)]);
    if (invalidAccepted.status !== "appended") throw new Error("expected append");
    const invalidBefore = await values(invalid);
    const invalidLane = { ...invalid.lane, producerEpoch: 8 };
    expect(
      await append(
        invalid,
        { ...invalidAccepted.checkpoint, producerEpoch: 8 },
        "00000001",
        [fact(1)],
        invalid.target,
        invalidLane,
      ),
    ).toEqual({ status: "invalid-epoch-seq" });
    expect(await values(invalid)).toEqual(invalidBefore);
    await invalid.close();
  });

  test("malformed and incompatible SQLite metadata fail typed recovery without mutation", async () => {
    const malformed = await harness();
    await createStreams(malformed);
    await malformed.client.stream(malformed.target.streamId).appendJsonBatch([
      {
        type: MESH_LINEAGE_TYPE,
        key: "checkpoint",
        value: { format: "broken" },
        headers: { operation: "upsert" },
      },
    ]);
    const malformedBefore = await malformed.adapter.listMessages(malformed.target.streamId);
    const malformedExit = await Effect.runPromiseExit(
      provideLive(recoverDerivedState(malformed.target, malformed.lane)),
    );
    expect(typedError(malformedExit)).toBeInstanceOf(MalformedLineage);
    expect(await malformed.adapter.listMessages(malformed.target.streamId)).toEqual(
      malformedBefore,
    );
    await malformed.close();

    const incompatible = await harness();
    await createStreams(incompatible);
    const other = await deriveProducerLane({
      ...incompatible.lane,
      outputGeneration: "generation-2",
    });
    await incompatible.client
      .stream(incompatible.target.streamId)
      .appendJsonBatch([
        createLineageEvent(other, { sourceThrough: "00000001", nextProducerSeq: 1 }),
      ]);
    const incompatibleBefore = await incompatible.adapter.listMessages(
      incompatible.target.streamId,
    );
    const incompatibleExit = await Effect.runPromiseExit(
      provideLive(recoverDerivedState(incompatible.target, incompatible.lane)),
    );
    expect(typedError(incompatibleExit)).toBeInstanceOf(IncompatibleLineage);
    expect(await incompatible.adapter.listMessages(incompatible.target.streamId)).toEqual(
      incompatibleBefore,
    );
    await incompatible.close();
  });
});

function producerRows(h: SqliteHarness): number {
  return h.adapter.state.db
    .query<{ count: number }, []>("select count(*) as count from streamsy_producers")
    .get()!.count;
}

function isLineage(value: unknown): boolean {
  return (
    typeof value === "object" &&
    value !== null &&
    "type" in value &&
    value.type === MESH_LINEAGE_TYPE
  );
}

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
            message: "response lost after durable commit",
            retryable: true,
          };
        },
      };
    },
    close: (reason) => client.close(reason),
  };
}
