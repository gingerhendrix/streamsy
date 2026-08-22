import {
  StreamProtocol,
  createMemoryStorageAdapter,
  directProtocolClient,
  type JsonValue,
  type StorageAdapter,
  type StreamProtocolClient,
} from "@streamsy/core";
import { Cause, Effect, Exit, Layer, Option, Schema } from "effect";
import { afterEach, describe, expect, test } from "vitest";
import { bindStream, type StreamBinding } from "../binding.ts";
import { streamIdentity } from "../causal.ts";
import { ProjectionPoison, StateRestorePoison } from "../effect/errors.ts";
import { AppendStreamsLive, ReadStreamsLive } from "../effect/streams.ts";
import { provideTestLayers } from "../effect/test-layers.ts";
import { DerivedRecoveryLive, DerivedStateHistoryLive } from "./derived-append.ts";
import { deriveProducerLane, type ProducerLane } from "./lane.ts";
import { catchUpState, type CatchUpStateResult } from "./state-projection.ts";
import { createLineageEvent } from "./state-meta.ts";

const clients = new Set<StreamProtocolClient>();
const limits = { maxItems: 100, maxPages: 100, maxBatches: 100, maxBytes: 100_000 };

afterEach(async () => {
  await Promise.all(Array.from(clients, (client) => client.close()));
  clients.clear();
});

interface Total {
  readonly total: number;
  readonly applied: number;
}

const initial: Total = { total: 0, applied: 0 };
const StateProjectionTestLive = Layer.merge(DerivedRecoveryLive, DerivedStateHistoryLive).pipe(
  Layer.provide(ReadStreamsLive),
  Layer.merge(ReadStreamsLive),
  Layer.merge(AppendStreamsLive),
);
const isProjectionPoison = Schema.is(ProjectionPoison);
const isStateRestorePoison = Schema.is(StateRestorePoison);

interface Harness {
  readonly adapter: StorageAdapter;
  readonly client: StreamProtocolClient;
  readonly source: StreamBinding;
  readonly target: StreamBinding;
  readonly lane: ProducerLane;
}

async function harness(adapter = createMemoryStorageAdapter()): Promise<Harness> {
  const client = directProtocolClient(new StreamProtocol({ storage: { adapter } }));
  clients.add(client);
  const sourceIdentity = streamIdentity("totals-source");
  const targetIdentity = streamIdentity("totals-state");
  const source = bindStream({ identity: sourceIdentity, client, streamId: "totals-source" });
  const target = bindStream({ identity: targetIdentity, client, streamId: "totals-state" });
  const lane = await deriveProducerLane({
    processorId: "totals",
    processorVersion: "1.0.0",
    outputGeneration: "generation-1",
    source: sourceIdentity,
    target: targetIdentity,
    producerEpoch: 3,
  });
  await client.stream(source.streamId).create({ contentType: "application/json" });
  await client.stream(target.streamId).create({ contentType: "application/json" });
  return { adapter, client, source, target, lane };
}

interface ProgramOptions {
  readonly poisonStep?: boolean;
  readonly target?: StreamBinding;
  readonly onDecode?: () => void;
  readonly validateRecovered?: (recovered: { readonly state: Total }) => void | Promise<void>;
  readonly rejectFactTotal?: number;
}

function program(h: Harness, options: ProgramOptions = {}) {
  return catchUpState({
    source: h.source,
    target: options.target ?? h.target,
    lane: h.lane,
    limits,
    initial,
    restore(start, events) {
      let state = start;
      for (const event of events) {
        if (!isRecord(event) || event.type !== "total") {
          throw new TypeError("unexpected target State fact");
        }
        const value = event.value;
        if (!isRecord(value) || typeof value.total !== "number") {
          throw new TypeError("malformed total row");
        }
        if (value.total === options.rejectFactTotal) throw new Error("rejected proposed total");
        state = { total: value.total, applied: Number(value.applied) };
      }
      return state;
    },
    validateRecovered: options.validateRecovered ?? (() => {}),
    decode(batch) {
      options.onDecode?.();
      if (batch.kind !== "json") throw new TypeError("expected JSON");
      return batch.items.map((item) => {
        if (!isRecord(item) || typeof item.v !== "number") throw new TypeError("bad item");
        return item.v;
      });
    },
    step(state, values) {
      if (options.poisonStep) throw new Error("poisoned step");
      const next: Total = {
        total: state.total + values.reduce((sum, value) => sum + value, 0),
        applied: state.applied + values.length,
      };
      return {
        facts: [
          {
            type: "total",
            key: "total",
            value: { total: next.total, applied: next.applied },
            headers: { operation: "upsert" },
          },
        ],
      };
    },
  }).pipe((effect) => provideTestLayers(effect, StateProjectionTestLive));
}

function run(h: Harness, options: ProgramOptions = {}): Promise<CatchUpStateResult<Total>> {
  return Effect.runPromise(program(h, options));
}

describe("catchUpState — recovered single-source State", () => {
  test("projects a fresh source and restores typed state on the next pass", async () => {
    const h = await harness();
    await h.client.stream(h.source.streamId).appendJsonBatch([{ v: 2 }, { v: 3 }]);
    const first = await run(h);
    expect(first).toMatchObject({
      status: "caught-up",
      batches: 1,
      state: { total: 5, applied: 2 },
    });

    await h.client.stream(h.source.streamId).appendJsonBatch([{ v: 4 }]);
    const second = await run(h);
    // The second pass never replays the first boundary; the total proves the
    // restored application state rather than a re-fold of the source.
    expect(second).toMatchObject({
      status: "caught-up",
      batches: 1,
      state: { total: 9, applied: 3 },
    });
  });

  test("a restarted host restores state from durable target history alone", async () => {
    const adapter = createMemoryStorageAdapter();
    const first = await harness(adapter);
    await first.client.stream(first.source.streamId).appendJsonBatch([{ v: 7 }]);
    expect(await run(first)).toMatchObject({ status: "caught-up", state: { total: 7 } });
    await first.client.close();
    clients.delete(first.client);

    const restarted = await harness(adapter);
    await restarted.client.stream(restarted.source.streamId).appendJsonBatch([{ v: 5 }]);
    expect(await run(restarted)).toMatchObject({
      status: "caught-up",
      batches: 1,
      state: { total: 12, applied: 2 },
    });
  });

  test("malformed durable target State is a typed restore poison", async () => {
    const h = await harness();
    const appended = await h.client.stream(h.source.streamId).appendJsonBatch([{ v: 1 }]);
    if (appended.status !== "appended") throw new Error("expected append");
    // A complete, lineage-terminated transaction whose application fact the
    // domain cannot restore. Lineage is intact, so the fault is restoration.
    await h.client.stream(h.target.streamId).appendJsonBatch([
      { type: "total", key: "total", value: { total: "nope" }, headers: { operation: "upsert" } },
      createLineageEvent(h.lane, {
        sourceThrough: appended.offset,
        nextProducerSeq: 1,
      }),
    ]);
    const exit = await Effect.runPromiseExit(program(h));
    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      const error = Cause.findErrorOption(exit.cause);
      expect(Option.isSome(error) && isStateRestorePoison(error.value)).toBe(true);
    }
  });

  test("step poison is typed and leaves lineage unadvanced", async () => {
    const h = await harness();
    await h.client.stream(h.source.streamId).appendJsonBatch([{ v: 1 }]);
    const exit = await Effect.runPromiseExit(program(h, { poisonStep: true }));
    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      const error = Cause.findErrorOption(exit.cause);
      expect(Option.isSome(error) && isProjectionPoison(error.value)).toBe(true);
      if (Option.isSome(error) && isProjectionPoison(error.value)) {
        expect(error.value.phase).toBe("step");
      }
    }
    expect(await h.adapter.listMessages(h.target.streamId)).toHaveLength(0);
  });

  test("a rejected proposed fact is materialized before append and writes nothing", async () => {
    const h = await harness();
    await h.client.stream(h.source.streamId).appendJsonBatch([{ v: 9 }]);
    const exit = await Effect.runPromiseExit(program(h, { rejectFactTotal: 9 }));
    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      const error = Cause.findErrorOption(exit.cause);
      expect(Option.isSome(error) && isStateRestorePoison(error.value)).toBe(true);
    }
    expect(await h.adapter.listMessages(h.target.streamId)).toHaveLength(0);
  });

  test("recovered state validation finishes before any source boundary is decoded", async () => {
    const h = await harness();
    await h.client.stream(h.source.streamId).appendJsonBatch([{ v: 2 }]);
    await run(h);
    await h.client.stream(h.source.streamId).appendJsonBatch([{ v: 3 }]);
    let decoded = 0;
    const exit = await Effect.runPromiseExit(
      program(h, {
        onDecode: () => decoded++,
        validateRecovered: () => {
          throw new Error("state and lineage disagree");
        },
      }),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    expect(decoded).toBe(0);
  });

  test("duplicate reconciliation installs and asynchronously validates durable state", async () => {
    const h = await harness();
    await h.client.stream(h.source.streamId).appendJsonBatch([{ v: 1 }]);
    const underlying = h.target.client;
    let staged = false;
    const racingClient = new Proxy(underlying, {
      get(target, property, receiver) {
        if (property !== "stream") return Reflect.get(target, property, receiver);
        return (streamId: string) => {
          const handle = target.stream(streamId);
          return new Proxy(handle, {
            get(handleTarget, handleProperty, handleReceiver) {
              if (handleProperty !== "appendJsonBatch") {
                const value = Reflect.get(handleTarget, handleProperty, handleReceiver);
                return typeof value === "function" ? value.bind(handleTarget) : value;
              }
              return async (items: readonly JsonValue[], appendOptions: object) => {
                if (!staged) {
                  staged = true;
                  const interloper = items.map((item, index) =>
                    index === 0 && isRecord(item) && isRecord(item.value)
                      ? { ...item, value: { ...item.value, total: 999 } }
                      : item,
                  );
                  await handleTarget.appendJsonBatch(interloper, appendOptions);
                }
                return handleTarget.appendJsonBatch(items, appendOptions);
              };
            },
          });
        };
      },
    });
    const target = { ...h.target, client: racingClient };
    const exit = await Effect.runPromiseExit(
      program(h, {
        target,
        validateRecovered: async ({ state }) => {
          await Promise.resolve();
          if (state.total === 999) throw new Error("reconciled durable state is invalid");
        },
      }),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      const error = Cause.findErrorOption(exit.cause);
      expect(Option.isSome(error) && isStateRestorePoison(error.value)).toBe(true);
    }
    const written = await h.adapter.listMessages(h.target.streamId);
    expect(written).toHaveLength(2);
  });

  test("a competing target writer cannot silently advance lineage", async () => {
    const h = await harness();
    await h.client.stream(h.source.streamId).appendJsonBatch([{ v: 1 }]);
    expect(await run(h)).toMatchObject({ status: "caught-up" });
    await h.client.stream(h.source.streamId).appendJsonBatch([{ v: 1 }]);
    await h.client
      .stream(h.target.streamId)
      .appendJsonBatch([
        { type: "total", key: "foreign", value: { total: 0 }, headers: { operation: "upsert" } },
      ]);
    // Recovery refuses a target whose history no longer ends at this lane's
    // lineage boundary, so the projection fails explicitly instead of advancing.
    const exit = await Effect.runPromiseExit(program(h));
    expect(Exit.isFailure(exit)).toBe(true);
  });

  test("running twice without new source data is a no-op", async () => {
    const h = await harness();
    await h.client.stream(h.source.streamId).appendJsonBatch([{ v: 1 }]);
    await run(h);
    const stored = await h.adapter.listMessages(h.target.streamId);
    expect(await run(h)).toMatchObject({ status: "caught-up", batches: 0, state: { total: 1 } });
    expect(await h.adapter.listMessages(h.target.streamId)).toEqual(stored);
  });
});

function isRecord(value: unknown): value is Record<string, JsonValue> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
