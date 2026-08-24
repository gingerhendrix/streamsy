import { officialProtocolClient, protocolPathUrl } from "@streamsy/client";
import {
  StreamProtocol,
  createHttpHandler,
  createMemoryStorageAdapter,
  directProtocolClient,
  type AppendJsonBatchOptions,
  type ClientAppendResult,
  type JsonValue,
  type StorageAdapter,
  type StreamProtocolClient,
} from "@streamsy/core";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Schema } from "effect";
import { afterEach, describe, expect, test } from "vitest";
import { bindStream, type StreamBinding } from "../binding.ts";
import { streamIdentity } from "../causal.ts";
import {
  AppendStreams,
  AppendStreamsLive,
  type AppendOutcome,
  ReadStreams,
  ReadStreamsLive,
} from "../effect/streams.ts";
import {
  IncompatibleLineage,
  MalformedLineage,
  ProjectionPoison,
  StreamAppendError,
} from "../effect/errors.ts";
import { provideTestLayers } from "../effect/test-layers.ts";
import {
  appendDerivedStateBatch,
  DerivedRecoveryLive,
  DerivedRecoveryTest,
  recoverDerivedState,
  type RecoveredDerivedState,
} from "./derived-append.ts";
import { deriveProducerLane, type ProducerLane } from "./lane.ts";
import { catchUp } from "./projection.ts";
import { jsonValueKey } from "./state-test-fixtures.ts";
import {
  MESH_LINEAGE_TYPE,
  createLineageEvent,
  decodeLineageEvent,
  ensureLineageCompatible,
} from "./state-meta.ts";

const clients = new Set<StreamProtocolClient>();
const limits = { maxItems: 100, maxPages: 100, maxBatches: 100, maxBytes: 100_000 };
const MeshTestLive = DerivedRecoveryLive.pipe(
  Layer.provide(ReadStreamsLive),
  Layer.merge(ReadStreamsLive),
  Layer.merge(AppendStreamsLive),
);
const isProjectionPoison = Schema.is(ProjectionPoison);

afterEach(() =>
  Promise.all(Array.from(clients, (client) => client.close())).then(() => clients.clear()),
);

interface Harness {
  readonly adapter: StorageAdapter;
  readonly client: StreamProtocolClient;
  readonly source: StreamBinding;
  readonly target: StreamBinding;
  readonly lane: ProducerLane;
}

// oxlint-disable-next-line effecttsgo/async-function -- This Promise helper builds the protocol-client harness used by the Vitest runner.
async function harness(transport: "direct" | "fetch" = "direct"): Promise<Harness> {
  const adapter = createMemoryStorageAdapter();
  const protocol = new StreamProtocol({ storage: { adapter } });
  const routedFetch = Object.assign(
    (input: Parameters<typeof globalThis.fetch>[0], init?: RequestInit) =>
      createHttpHandler({ protocol, pathPrefix: "/streams" }).fetch(new Request(input, init)),
    { preconnect: globalThis.fetch.preconnect },
  );
  const client =
    transport === "direct"
      ? directProtocolClient(protocol)
      : officialProtocolClient({
          urlFor: (id) => protocolPathUrl("https://mesh.test/streams", id),
          fetch: routedFetch,
          backoffOptions: { initialDelay: 1, maxDelay: 1, multiplier: 1, maxRetries: 0 },
          warnOnHttp: false,
        });
  clients.add(client);
  const sourceIdentity = streamIdentity("orders");
  const targetIdentity = streamIdentity("orders-state");
  const source = bindStream({ identity: sourceIdentity, client, streamId: "orders" });
  const target = bindStream({ identity: targetIdentity, client, streamId: "orders-state" });
  const lane = await deriveProducerLane({
    processorId: "orders-state",
    processorVersion: "1.0.0",
    outputGeneration: "generation-1",
    source: sourceIdentity,
    target: targetIdentity,
    producerEpoch: 7,
  });
  await client.stream(source.streamId).create({ contentType: "application/json" });
  await client.stream(target.streamId).create({ contentType: "application/json" });
  return { adapter, client, source, target, lane };
}

const provideLive = <A, E, R>(program: Effect.Effect<A, E, R>) =>
  provideTestLayers(program, MeshTestLive);

function fact(value: JsonValue): JsonValue {
  const key = jsonValueKey(value);
  return { type: "order", key: `o-${key}`, value, headers: { operation: "upsert" } };
}

// oxlint-disable-next-line effecttsgo/async-function -- This Promise helper reads the storage adapter fixture for Vitest assertions.
async function targetValues(h: Harness): Promise<unknown[]> {
  const decoder = new TextDecoder();
  return (await h.adapter.listMessages(h.target.streamId)).map((message) =>
    JSON.parse(decoder.decode(message.data)),
  );
}

function projection(h: Harness) {
  return catchUp({
    source: h.source,
    target: h.target,
    lane: h.lane,
    limits,
    decode(batch) {
      if (batch.kind !== "json") throw new TypeError("expected JSON");
      return batch.items;
    },
    reduce(items) {
      return items.map(fact);
    },
  });
}

function appendProgram(
  h: Harness,
  previous: RecoveredDerivedState,
  sourceThrough: string,
  facts: readonly JsonValue[],
) {
  return provideLive(
    appendDerivedStateBatch({ target: h.target, lane: h.lane, previous, sourceThrough, facts }),
  );
}

interface ScriptedPage {
  readonly offset: string;
  readonly items: readonly JsonValue[];
}

function scriptedReadLayer(pages: readonly ScriptedPage[]) {
  return Layer.succeed(
    ReadStreams,
    ReadStreams.of({
      open: (_binding, options) =>
        Effect.sync(() => {
          const eligible = pages.filter((page) => page.offset > (options?.offset ?? "-1"));
          let index = 0;
          return {
            status: "ok" as const,
            session: {
              startOffset: options?.offset ?? "-1",
              next: Effect.sync(() => {
                const page = eligible[index++];
                return page === undefined
                  ? { done: true as const, value: undefined }
                  : {
                      done: false as const,
                      value: {
                        kind: "json" as const,
                        items: page.items,
                        offset: page.offset,
                        upToDate: false,
                        streamClosed: false,
                      },
                    };
              }),
              done: Effect.succeed({ status: "done" as const }),
              cancel: () => Effect.void,
            },
          };
        }),
    }),
  );
}

// oxlint-disable-next-line effecttsgo/async-function -- This helper is the Vitest boundary that runs recovery as a Promise.
async function ready(h: Harness): Promise<RecoveredDerivedState> {
  const recovered = await Effect.runPromise(provideLive(recoverDerivedState(h.target, h.lane)));
  if (recovered.status !== "ready") throw new Error(`expected ready, got ${recovered.status}`);
  return recovered;
}

// oxlint-disable-next-line effecttsgo/async-function -- This helper is the Vitest boundary that runs a scripted Effect scenario as a Promise.
async function runScriptedProjection(
  h: Harness,
  pages: readonly ScriptedPage[],
  options: {
    readonly limits?: typeof limits;
    readonly reduce?: (items: readonly JsonValue[]) => readonly JsonValue[];
  } = {},
) {
  const recovered = await ready(h);
  return Effect.runPromise(
    catchUp({
      source: h.source,
      target: h.target,
      lane: h.lane,
      limits: options.limits ?? limits,
      decode(batch) {
        if (batch.kind !== "json") throw new TypeError("expected JSON");
        return batch.items;
      },
      reduce: options.reduce ?? ((items) => items.map(fact)),
    }).pipe((effect) =>
      provideTestLayers(
        effect,
        Layer.merge(
          DerivedRecoveryTest(() => Effect.succeed(recovered)),
          scriptedReadLayer(pages),
        ).pipe(Layer.merge(AppendStreamsLive)),
      ),
    ),
  );
}

function typedError<A, E>(exit: Exit.Exit<A, E>): E {
  if (!Exit.isFailure(exit)) throw new Error("expected failure Exit");
  const error = Cause.findErrorOption(exit.cause);
  if (Option.isNone(error)) throw new Error("expected typed error");
  return error.value;
}

function commitThenBlock(
  binding: StreamBinding,
  items: readonly JsonValue[],
  options: AppendJsonBatchOptions | undefined,
  committed: Deferred.Deferred<void>,
): Effect.Effect<never, StreamAppendError> {
  return Effect.tryPromise({
    try: (signal) =>
      binding.client.stream(binding.streamId).appendJsonBatch(items, { ...options, signal }),
    catch: (cause) => StreamAppendError.from("appendJsonBatch", cause),
  }).pipe(
    Effect.flatMap((result: ClientAppendResult) =>
      result.status === "error"
        ? Effect.fail(StreamAppendError.from("appendJsonBatch", result))
        : result.status !== "appended"
          ? Effect.die(`unexpected append outcome ${result.status}`)
          : Deferred.succeed(committed, undefined).pipe(Effect.andThen(Effect.never)),
    ),
  );
}

function commitThenBlockLayer(committed: Deferred.Deferred<void>) {
  return Layer.succeed(
    AppendStreams,
    AppendStreams.of({
      append: () => Effect.die("unused"),
      appendJsonBatch: (binding, items, options) =>
        commitThenBlock(binding, items, options, committed),
    }),
  );
}

function appendOutcomeLayer(outcome: AppendOutcome) {
  return Layer.succeed(
    AppendStreams,
    AppendStreams.of({
      append: () => Effect.succeed(outcome),
      appendJsonBatch: () => Effect.succeed(outcome),
    }),
  );
}

describe("Effect-first mesh", () => {
  // oxlint-disable-next-line effecttsgo/async-function -- Vitest executes this Promise-returning Effect scenario at the test boundary.
  test("Schema decodes lineage and typed compatibility failures", async () => {
    const h = await harness();
    const event = createLineageEvent(h.lane, { sourceThrough: "00000001", nextProducerSeq: 1 });
    expect(await Effect.runPromise(decodeLineageEvent(JSON.parse(JSON.stringify(event))))).toEqual(
      event,
    );
    const malformed = await Effect.runPromiseExit(decodeLineageEvent({ type: MESH_LINEAGE_TYPE }));
    expect(Exit.isFailure(malformed)).toBe(true);
    const other = await deriveProducerLane({ ...h.lane, outputGeneration: "generation-2" });
    const incompatible = await Effect.runPromiseExit(ensureLineageCompatible(event, other));
    expect(Exit.isFailure(incompatible)).toBe(true);
  });

  test.each(["direct", "fetch"] as const)(
    "recovers and projects explicit boundaries over %s",
    // oxlint-disable-next-line effecttsgo/async-function -- Vitest executes this Promise-returning Effect scenario at the test boundary.
    async (transport) => {
      const h = await harness(transport);
      const sourceAppend = await h.client.stream(h.source.streamId).appendJsonBatch([1, 2]);
      if (sourceAppend.status !== "appended") throw new Error("expected append");
      const result = await Effect.runPromise(provideLive(projection(h)));
      expect(result).toMatchObject({
        status: "caught-up",
        batches: 1,
        items: 2,
        checkpoint: { sourceThrough: sourceAppend.offset },
      });
      const recovered = await Effect.runPromise(provideLive(recoverDerivedState(h.target, h.lane)));
      expect(recovered).toMatchObject({
        status: "ready",
        sourceThrough: sourceAppend.offset,
        nextProducerSeq: 1,
      });
    },
  );

  // oxlint-disable-next-line effecttsgo/async-function -- Vitest executes this Promise-returning Effect scenario at the test boundary.
  test("duplicate reconciliation is an outcome and does not claim payload verification", async () => {
    const h = await harness();
    const previous = await Effect.runPromise(provideLive(recoverDerivedState(h.target, h.lane)));
    if (previous.status !== "ready") throw new Error("expected ready");
    const accepted = await Effect.runPromise(appendProgram(h, previous, "00000001", [fact(1)]));
    expect(accepted.status).toBe("appended");
    const duplicate = await Effect.runPromise(appendProgram(h, previous, "00000001", [fact(999)]));
    expect(duplicate.status).toBe("sequence-already-accepted");
    expect(JSON.stringify(duplicate)).not.toContain("verified");
    expect(await h.adapter.listMessages(h.target.streamId)).toHaveLength(2);
  });

  // oxlint-disable-next-line effecttsgo/async-function -- Vitest executes this Promise-returning Effect scenario at the test boundary.
  test("expected-offset contention remains an explicit output-conflict", async () => {
    const h = await harness();
    const previous = await Effect.runPromise(provideLive(recoverDerivedState(h.target, h.lane)));
    if (previous.status !== "ready") throw new Error("expected ready");
    await h.client.stream(h.target.streamId).appendJsonBatch([fact("foreign")]);
    const result = await Effect.runPromise(appendProgram(h, previous, "00000001", [fact(1)]));
    expect(result).toMatchObject({ status: "output-conflict", reason: "expected-offset" });
  });

  // oxlint-disable-next-line effecttsgo/async-function -- Vitest executes this Promise-returning Effect scenario at the test boundary.
  test("decode poison is a typed failure and leaves lineage unadvanced", async () => {
    const h = await harness();
    await h.client.stream(h.source.streamId).appendJsonBatch([1]);
    const exit = await Effect.runPromiseExit(
      provideLive(
        catchUp({
          source: h.source,
          target: h.target,
          lane: h.lane,
          limits,
          decode() {
            throw new Error("poison");
          },
          reduce() {
            return [];
          },
        }),
      ),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      const error = Cause.findErrorOption(exit.cause);
      expect(Option.isSome(error) && isProjectionPoison(error.value)).toBe(true);
    }
    expect(await h.adapter.listMessages(h.target.streamId)).toHaveLength(0);
  });

  // oxlint-disable-next-line effecttsgo/async-function -- Vitest executes this Promise-returning Effect scenario at the test boundary.
  test("a filtered source boundary commits exactly one lineage-only transaction", async () => {
    const h = await harness();
    const result = await runScriptedProjection(h, [{ offset: "00000001", items: [1] }], {
      reduce: () => [],
    });

    expect(result).toMatchObject({
      status: "caught-up",
      batches: 1,
      checkpoint: { sourceThrough: "00000001", nextProducerSeq: 1 },
    });
    expect(await targetValues(h)).toEqual([
      expect.objectContaining({
        type: MESH_LINEAGE_TYPE,
        value: expect.objectContaining({ sourceThrough: "00000001", nextProducerSeq: 1 }),
      }),
    ]);
  });

  // oxlint-disable-next-line effecttsgo/async-function -- Vitest executes this Promise-returning Effect scenario at the test boundary.
  test("limits preserve complete boundaries and oversized boundaries remain terminal", async () => {
    const limited = await harness();
    const pages = [
      { offset: "00000001", items: [1] },
      { offset: "00000002", items: [2] },
    ];
    expect(
      await runScriptedProjection(limited, pages, {
        limits: { ...limits, maxBatches: 1 },
      }),
    ).toMatchObject({
      status: "limit-reached",
      limit: "maxBatches",
      batches: 1,
      checkpoint: { sourceThrough: "00000001", nextProducerSeq: 1 },
    });
    expect(await targetValues(limited)).toEqual([
      fact(1),
      expect.objectContaining({
        type: MESH_LINEAGE_TYPE,
        value: expect.objectContaining({ sourceThrough: "00000001" }),
      }),
    ]);

    const oversized = await harness();
    for (let attempt = 0; attempt < 2; attempt++) {
      expect(
        await runScriptedProjection(oversized, [{ offset: "00000001", items: [1, 2] }], {
          limits: { ...limits, maxItems: 1 },
        }),
      ).toMatchObject({
        status: "boundary-too-large",
        limit: "maxItems",
        actual: 2,
        maximum: 1,
        batches: 0,
      });
    }
    expect(await targetValues(oversized)).toEqual([]);
  });

  test.each([
    ["malformed", MalformedLineage],
    ["incompatible", IncompatibleLineage],
  ] as const)(
    "%s lineage fails recovery with its tagged error without advancing",
    // oxlint-disable-next-line effecttsgo/async-function -- Vitest executes this Promise-returning Effect scenario at the test boundary.
    async (kind, ErrorClass) => {
      const h = await harness();
      const value =
        kind === "malformed"
          ? { type: MESH_LINEAGE_TYPE }
          : createLineageEvent(
              await deriveProducerLane({ ...h.lane, outputGeneration: "generation-2" }),
              { sourceThrough: "00000001", nextProducerSeq: 1 },
            );
      const appended = await h.client.stream(h.target.streamId).appendJsonBatch([value]);
      if (appended.status !== "appended") throw new Error("expected append");
      const before = await h.adapter.listMessages(h.target.streamId);

      const exit = await Effect.runPromiseExit(provideLive(recoverDerivedState(h.target, h.lane)));

      expect(typedError(exit)).toBeInstanceOf(ErrorClass);
      expect(await h.adapter.listMessages(h.target.streamId)).toEqual(before);
      expect(
        await h.adapter.getProducerState(h.target.streamId, h.lane.producerId),
      ).toBeUndefined();
    },
  );

  test.each([
    { status: "stale-epoch", currentEpoch: 8 },
    { status: "producer-gap", expectedSeq: 1, receivedSeq: 2 },
  ] as const)(
    "$status remains an explicit append outcome",
    // oxlint-disable-next-line effecttsgo/async-function -- Vitest executes this Promise-returning Effect scenario at the test boundary.
    async (outcome) => {
      const h = await harness();
      const previous = await ready(h);
      const result = await Effect.runPromise(
        appendDerivedStateBatch({
          target: h.target,
          lane: h.lane,
          previous,
          sourceThrough: "00000001",
          facts: [fact(1)],
        }).pipe((effect) =>
          provideTestLayers(
            effect,
            Layer.merge(
              DerivedRecoveryTest(() => Effect.succeed(previous)),
              appendOutcomeLayer(outcome),
            ),
          ),
        ),
      );
      expect(result).toEqual(outcome);
    },
  );

  // oxlint-disable-next-line effecttsgo/async-function -- Vitest executes this Promise-returning Effect scenario at the test boundary.
  test("reserved Streamsy facts fail before any append", async () => {
    const h = await harness();
    const previous = await ready(h);
    const exit = await Effect.runPromiseExit(
      appendProgram(h, previous, "00000001", [
        {
          type: "__streamsy.application",
          key: "forbidden",
          value: 1,
          headers: { operation: "upsert" },
        },
      ]),
    );

    expect(Exit.isFailure(exit)).toBe(true);
    expect(await targetValues(h)).toEqual([]);
    expect(await h.adapter.getProducerState(h.target.streamId, h.lane.producerId)).toBeUndefined();
  });

  // oxlint-disable-next-line effecttsgo/async-function -- Vitest executes this Promise-returning Effect scenario at the test boundary.
  test("interruption stays interruption while an in-flight append has unknown durability", async () => {
    const h = await harness();
    const previous = await ready(h);
    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        const appendCommitted = yield* Deferred.make<void>();
        const program = projection(h).pipe((effect) =>
          provideTestLayers(
            effect,
            Layer.merge(
              DerivedRecoveryTest(() => Effect.succeed(previous)),
              scriptedReadLayer([{ offset: "00000001", items: [1] }]),
            ).pipe(Layer.merge(commitThenBlockLayer(appendCommitted))),
          ),
        );
        const fiber = yield* Effect.forkChild(program);
        yield* Deferred.await(appendCommitted);
        yield* Fiber.interrupt(fiber);
        return yield* Fiber.await(fiber);
      }),
    );
    expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);

    const recovered = await Effect.runPromise(provideLive(recoverDerivedState(h.target, h.lane)));
    expect(recovered).toMatchObject({
      status: "ready",
      sourceThrough: "00000001",
      nextProducerSeq: 1,
    });
    expect(await targetValues(h)).toEqual([
      fact(1),
      expect.objectContaining({
        type: MESH_LINEAGE_TYPE,
        value: expect.objectContaining({ sourceThrough: "00000001", nextProducerSeq: 1 }),
      }),
    ]);
  });
});
