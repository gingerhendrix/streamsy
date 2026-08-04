import { officialProtocolClient, protocolPathUrl } from "@streamsy/client";
import {
  ClientReadSession,
  StreamProtocol,
  createHttpHandler,
  createMemoryStorageAdapter,
  directProtocolClient,
  type AppendJsonBatchOptions,
  type ClientAppendResult,
  type ClientReadResult,
  type JsonValue,
  type ReadStreamOptions,
  type StorageAdapter,
  type StreamBatch,
  type StreamProtocolClient,
  type StreamProtocolHandle,
} from "@streamsy/core";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option } from "effect";
import { afterEach, describe, expect, test } from "vitest";
import { bindStream, type StreamBinding } from "../binding.ts";
import { streamIdentity } from "../causal.ts";
import {
  AppendStreams,
  AppendStreamsLive,
  ReadStreams,
  ReadStreamsLive,
  type EffectReadSession,
} from "../effect/streams.ts";
import {
  IncompatibleLineage,
  MalformedLineage,
  MalformedSourceBoundary,
  ProjectionPoison,
  StreamAppendError,
} from "../effect/errors.ts";
import {
  appendDerivedStateBatch,
  DerivedRecoveryLive,
  DerivedRecoveryTest,
  recoverDerivedState,
  type RecoveredDerivedState,
} from "./derived-append.ts";
import {
  MAX_PRODUCER_ID_LENGTH,
  canonicalLaneInput,
  deriveProducerLane,
  type ProducerLane,
  type ProducerLaneConfig,
} from "./lane.ts";
import { catchUp, type CatchUpLimits } from "./projection.ts";
import { MESH_LINEAGE_TYPE, createLineageEvent } from "./state-meta.ts";

const clients = new Set<StreamProtocolClient>();
const generous: CatchUpLimits = {
  maxItems: 100,
  maxPages: 100,
  maxBatches: 100,
  maxBytes: 100_000,
};
const noRetry = { initialDelay: 1, maxDelay: 1, multiplier: 1, maxRetries: 0 };

interface Harness {
  readonly adapter: StorageAdapter;
  readonly protocol: StreamProtocol;
  readonly client: StreamProtocolClient;
  readonly source: StreamBinding;
  readonly target: StreamBinding;
  readonly lane: ProducerLane;
}

interface ScriptedPage {
  readonly offset: string;
  readonly items: readonly JsonValue[];
}

afterEach(async () => {
  await Promise.all(Array.from(clients, (client) => client.close()));
  clients.clear();
});

const provideLive = <A, E, R>(program: Effect.Effect<A, E, R>) =>
  program.pipe(
    Effect.provide(DerivedRecoveryLive),
    Effect.provide(ReadStreamsLive),
    Effect.provide(AppendStreamsLive),
  );

async function harness(): Promise<Harness> {
  const adapter = createMemoryStorageAdapter();
  const protocol = new StreamProtocol({ storage: { adapter } });
  const client = directProtocolClient(protocol);
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
  return { adapter, protocol, client, source, target, lane };
}

function fact(value: JsonValue): JsonValue {
  return { type: "order", key: `o-${String(value)}`, value, headers: { operation: "upsert" } };
}

async function values(h: Harness): Promise<unknown[]> {
  const decoder = new TextDecoder();
  return (await h.adapter.listMessages(h.target.streamId)).map((message) =>
    JSON.parse(decoder.decode(message.data)),
  );
}

async function ready(h: Harness): Promise<RecoveredDerivedState> {
  const recovered = await Effect.runPromise(provideLive(recoverDerivedState(h.target, h.lane)));
  if (recovered.status !== "ready") throw new Error(`expected ready, got ${recovered.status}`);
  return recovered;
}

function append(
  h: Harness,
  previous: RecoveredDerivedState,
  sourceThrough: string,
  facts: JsonValue[],
) {
  return Effect.runPromise(
    provideLive(
      appendDerivedStateBatch({ target: h.target, lane: h.lane, previous, sourceThrough, facts }),
    ),
  );
}

function typedError<A, E>(exit: Exit.Exit<A, E>): E {
  if (!Exit.isFailure(exit)) throw new Error("expected failure Exit");
  const error = Cause.findErrorOption(exit.cause);
  if (Option.isNone(error)) throw new Error("expected typed error");
  return error.value;
}

function scriptedReadLayer(pages: readonly ScriptedPage[], onCancel: () => void = () => {}) {
  return Layer.succeed(
    ReadStreams,
    ReadStreams.of({
      open: (_binding, options) =>
        Effect.acquireRelease(
          Effect.sync(() => {
            const eligible = pages.filter((page) => page.offset > (options?.offset ?? "-1"));
            let index = 0;
            const session: EffectReadSession = {
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
              cancel: () => Effect.sync(onCancel),
            };
            return { status: "ok" as const, session };
          }),
          (opened) => opened.session.cancel("scripted read scope closed"),
        ),
    }),
  );
}

async function runScripted(
  h: Harness,
  pages: readonly ScriptedPage[],
  options: {
    readonly limits?: CatchUpLimits;
    readonly decode?: (batch: StreamBatch) => Iterable<JsonValue>;
    readonly reduce?: (items: readonly JsonValue[]) => Iterable<JsonValue>;
  } = {},
) {
  const recovered = await ready(h);
  return Effect.runPromise(
    catchUp({
      source: h.source,
      target: h.target,
      lane: h.lane,
      limits: options.limits ?? generous,
      decode:
        options.decode ??
        ((batch) => {
          if (batch.kind !== "json") throw new TypeError("expected JSON");
          return batch.items;
        }),
      reduce: options.reduce ?? ((items) => items.map(fact)),
    }).pipe(
      Effect.provide(DerivedRecoveryTest(() => Effect.succeed(recovered))),
      Effect.provide(scriptedReadLayer(pages)),
      Effect.provide(AppendStreamsLive),
    ),
  );
}

const laneConfig: ProducerLaneConfig = {
  processorId: "counter",
  processorVersion: "1.0.0",
  outputGeneration: "blue",
  source: streamIdentity("source"),
  target: streamIdentity("target"),
  producerEpoch: 7,
};

describe("producer lane regressions", () => {
  test("canonical identity is deterministic, bounded, and sensitive to every semantic input", async () => {
    const first = await deriveProducerLane(laneConfig);
    expect(await deriveProducerLane(laneConfig)).toEqual(first);
    expect(first.producerId).toHaveLength(MAX_PRODUCER_ID_LENGTH);
    expect(canonicalLaneInput(laneConfig)).toBe(
      '["streamsy.mesh.producer-lane.v1","counter","1.0.0","blue","streamsy.identity.v1:source","streamsy.identity.v1:target"]',
    );
    for (const changed of [
      { ...laneConfig, processorId: "other" },
      { ...laneConfig, processorVersion: "2.0.0" },
      { ...laneConfig, outputGeneration: "green" },
      { ...laneConfig, source: streamIdentity("other-source") },
      { ...laneConfig, target: streamIdentity("other-target") },
    ]) {
      expect((await deriveProducerLane(changed)).producerId).not.toBe(first.producerId);
    }
  });

  test("epoch is validated configuration but invariant in producer-id derivation", async () => {
    const first = await deriveProducerLane(laneConfig);
    const bumped = await deriveProducerLane({ ...laneConfig, producerEpoch: 8 });
    expect(bumped.producerId).toBe(first.producerId);
    expect(bumped.producerEpoch).toBe(8);
  });
});

describe("recovery and producer regressions", () => {
  test("sequential fact and lineage transactions advance one durable producer row", async () => {
    const h = await harness();
    const initial = await ready(h);
    const first = await append(h, initial, "00000001", [fact(1)]);
    if (first.status !== "appended") throw new Error("expected append");
    const second = await append(h, first.checkpoint, "00000002", []);
    expect(second).toMatchObject({
      status: "appended",
      checkpoint: { sourceThrough: "00000002", nextProducerSeq: 2 },
    });
    expect(await h.adapter.getProducerState(h.target.streamId, h.lane.producerId)).toEqual({
      epoch: 7,
      lastSeq: 1,
    });
    expect((await values(h)).filter(isLineage)).toHaveLength(2);
  });

  test("real storage returns stale epoch, producer gap, and invalid epoch/sequence without attempt writes", async () => {
    const stale = await harness();
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

    const gap = await harness();
    const accepted = await append(gap, await ready(gap), "00000001", [fact(1)]);
    if (accepted.status !== "appended") throw new Error("expected append");
    const gapBefore = await values(gap);
    expect(
      await append(gap, { ...accepted.checkpoint, nextProducerSeq: 2 }, "00000002", [fact(2)]),
    ).toEqual({ status: "producer-gap", expectedSeq: 1, receivedSeq: 2 });
    expect(await values(gap)).toEqual(gapBefore);

    const invalid = await harness();
    const invalidAccepted = await append(invalid, await ready(invalid), "00000000", [fact(0)]);
    if (invalidAccepted.status !== "appended") throw new Error("expected append");
    const invalidBefore = await values(invalid);
    const bumpedInvalidLane = { ...invalid.lane, producerEpoch: invalid.lane.producerEpoch + 1 };
    expect(
      await Effect.runPromise(
        provideLive(
          appendDerivedStateBatch({
            target: invalid.target,
            lane: bumpedInvalidLane,
            previous: {
              ...invalidAccepted.checkpoint,
              producerEpoch: bumpedInvalidLane.producerEpoch,
            },
            sourceThrough: "00000001",
            facts: [fact(1)],
          }),
        ),
      ),
    ).toEqual({ status: "invalid-epoch-seq" });
    expect(await values(invalid)).toEqual(invalidBefore);
  });

  test("bare tails and fixed-epoch mismatches fail typed recovery without writes", async () => {
    const bare = await harness();
    await bare.client.stream(bare.target.streamId).appendJsonBatch([fact("foreign")]);
    const bareExit = await Effect.runPromiseExit(
      provideLive(recoverDerivedState(bare.target, bare.lane)),
    );
    expect(typedError(bareExit)).toBeInstanceOf(IncompatibleLineage);
    expect((typedError(bareExit) as IncompatibleLineage).message).toContain(
      "lineage transaction boundary",
    );

    const mismatch = await harness();
    const accepted = await append(mismatch, await ready(mismatch), "00000001", [fact(1)]);
    expect(accepted.status).toBe("appended");
    const before = await values(mismatch);
    const exit = await Effect.runPromiseExit(
      provideLive(
        recoverDerivedState(mismatch.target, {
          ...mismatch.lane,
          producerEpoch: mismatch.lane.producerEpoch + 1,
        }),
      ),
    );
    expect(typedError(exit)).toBeInstanceOf(IncompatibleLineage);
    expect(await values(mismatch)).toEqual(before);
  });

  test("the missing-startOffset recovery reproduction fails typed and releases exactly once", async () => {
    const h = await harness();
    let cancelled = 0;
    const target = bindStream({
      ...h.target,
      client: mapReads(
        h.client,
        async <T extends JsonValue>(
          delegate: StreamProtocolHandle,
          options: ReadStreamOptions | undefined,
        ): Promise<ClientReadResult<T>> => {
          const result = await delegate.read<T>(options);
          if (result.status === "ok") {
            const originalCancel = result.session.cancel.bind(result.session);
            result.session.cancel = (reason?: unknown) => {
              cancelled++;
              originalCancel(reason);
            };
            Object.defineProperty(result.session, "startOffset", { value: undefined });
          }
          return result;
        },
      ),
    });
    const exit = await Effect.runPromiseExit(provideLive(recoverDerivedState(target, h.lane)));
    expect(typedError(exit)).toBeInstanceOf(MalformedLineage);
    expect(cancelled).toBe(1);
  });
});

describe("projection semantic regressions", () => {
  test.each([
    ["maxPages", { ...generous, maxPages: 1 }],
    ["maxItems", { ...generous, maxItems: 1 }],
    ["maxBytes", { ...generous, maxBytes: 3 }],
  ] as const)(
    "enforces cumulative %s at complete boundaries and later continues",
    async (limit, limits) => {
      const h = await harness();
      const pages = [
        { offset: "00000001", items: [1] },
        { offset: "00000002", items: [2] },
      ];
      expect(await runScripted(h, pages, { limits })).toMatchObject({
        status: "limit-reached",
        limit,
        batches: 1,
        checkpoint: { sourceThrough: "00000001", nextProducerSeq: 1 },
      });
      expect(await runScripted(h, pages)).toMatchObject({
        status: "caught-up",
        batches: 1,
        checkpoint: { sourceThrough: "00000002", nextProducerSeq: 2 },
      });
      expect((await values(h)).filter(isLineage)).toHaveLength(2);
    },
  );

  test("oversized byte boundaries are terminal and leave durable progress unchanged", async () => {
    const h = await harness();
    for (let attempt = 0; attempt < 2; attempt++) {
      expect(
        await runScripted(h, [{ offset: "00000001", items: [123] }], {
          limits: { ...generous, maxBytes: 2 },
        }),
      ).toMatchObject({
        status: "boundary-too-large",
        limit: "maxBytes",
        actual: 5,
        maximum: 2,
        batches: 0,
      });
    }
    expect(await values(h)).toEqual([]);
  });

  test("malformed source boundaries and reduce poison are distinct typed failures", async () => {
    const malformed = await harness();
    const malformedExit = await Effect.runPromiseExit(
      runScriptedEffect(malformed, [{ offset: "now", items: [1] }]),
    );
    expect(typedError(malformedExit)).toBeInstanceOf(MalformedSourceBoundary);
    expect(await values(malformed)).toEqual([]);

    const poison = await harness();
    const poisonExit = await Effect.runPromiseExit(
      runScriptedEffect(poison, [{ offset: "00000001", items: [1] }], {
        reduce() {
          throw new Error("bad reduction");
        },
      }),
    );
    expect(typedError(poisonExit)).toMatchObject({
      _tag: "ProjectionPoison",
      phase: "reduce",
      sourcePosition: "00000001",
    });
    expect(typedError(poisonExit)).toBeInstanceOf(ProjectionPoison);
    expect(await values(poison)).toEqual([]);
  });

  test("wrong source identity defects before reads and wrong generation fails recovery typed", async () => {
    const h = await harness();
    const wrongSource = bindStream({ ...h.source, identity: streamIdentity("wrong") });
    await expect(
      Effect.runPromise(
        provideLive(
          catchUp({
            source: wrongSource,
            target: h.target,
            lane: h.lane,
            limits: generous,
            decode: jsonItems,
            reduce: (items) => items.map(fact),
          }),
        ),
      ),
    ).rejects.toThrow(/Source binding identity/);

    await append(h, await ready(h), "00000001", [fact(1)]);
    const wrongLane = await deriveProducerLane({ ...h.lane, outputGeneration: "generation-2" });
    const exit = await Effect.runPromiseExit(
      provideLive(
        catchUp({
          source: h.source,
          target: h.target,
          lane: wrongLane,
          limits: generous,
          decode: jsonItems,
          reduce: (items) => items.map(fact),
        }),
      ),
    );
    expect(typedError(exit)).toBeInstanceOf(IncompatibleLineage);
  });

  test("interruption during recovery and blocked source reads releases each Live session once", async () => {
    const recoveryHarness = await harness();
    let recoveryCancelled = 0;
    const recoveryRead = blockedReadClient(() => recoveryCancelled++);
    const recoveryTarget = bindStream({
      ...recoveryHarness.target,
      client: recoveryRead.client,
    });
    const recoveryExit = await interruptAfterOpen(
      provideLive(recoverDerivedState(recoveryTarget, recoveryHarness.lane)),
      recoveryRead.opened,
    );
    expect(Exit.isFailure(recoveryExit) && Cause.hasInterrupts(recoveryExit.cause)).toBe(true);
    expect(recoveryCancelled).toBe(1);

    const sourceHarness = await harness();
    let sourceCancelled = 0;
    const sourceRead = blockedReadClient(() => sourceCancelled++);
    const source = bindStream({
      ...sourceHarness.source,
      client: sourceRead.client,
    });
    const previous = await ready(sourceHarness);
    const sourceExit = await interruptAfterOpen(
      catchUp({
        source,
        target: sourceHarness.target,
        lane: sourceHarness.lane,
        limits: generous,
        decode: jsonItems,
        reduce: (items) => items.map(fact),
      }).pipe(
        Effect.provide(DerivedRecoveryTest(() => Effect.succeed(previous))),
        Effect.provide(ReadStreamsLive),
        Effect.provide(AppendStreamsLive),
      ),
      sourceRead.opened,
    );
    expect(Exit.isFailure(sourceExit) && Cause.hasInterrupts(sourceExit.cause)).toBe(true);
    expect(sourceCancelled).toBe(1);
  });

  test("interruption before storage append writes nothing; interruption after an earlier commit restarts from it", async () => {
    const before = await harness();
    const beforePrevious = await ready(before);
    const entered = await Effect.runPromise(Deferred.make<void>());
    const noCommitAppend = Layer.succeed(
      AppendStreams,
      AppendStreams.of({
        append: () => Effect.die("unused"),
        appendJsonBatch: () =>
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
      }),
    );
    const beforeExit = await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* catchUp({
          source: before.source,
          target: before.target,
          lane: before.lane,
          limits: generous,
          decode: jsonItems,
          reduce: (items) => items.map(fact),
        }).pipe(
          Effect.provide(DerivedRecoveryTest(() => Effect.succeed(beforePrevious))),
          Effect.provide(scriptedReadLayer([{ offset: "00000001", items: [1] }])),
          Effect.provide(noCommitAppend),
          Effect.forkChild,
        );
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(fiber);
        return yield* Fiber.await(fiber);
      }),
    );
    expect(Exit.isFailure(beforeExit) && Cause.hasInterrupts(beforeExit.cause)).toBe(true);
    expect(await values(before)).toEqual([]);

    const prior = await harness();
    const priorPrevious = await ready(prior);
    const blocked = await Effect.runPromise(Deferred.make<void>());
    let index = 0;
    let cancelled = 0;
    const priorRead = Layer.succeed(
      ReadStreams,
      ReadStreams.of({
        open: () =>
          Effect.acquireRelease(
            Effect.succeed({
              status: "ok" as const,
              session: {
                startOffset: "-1",
                next: Effect.suspend(() => {
                  index++;
                  if (index === 1) {
                    return Effect.succeed({
                      done: false as const,
                      value: {
                        kind: "json" as const,
                        items: [1],
                        offset: "00000001",
                        upToDate: false,
                        streamClosed: false,
                      },
                    });
                  }
                  return Deferred.succeed(blocked, undefined).pipe(Effect.andThen(Effect.never));
                }),
                done: Effect.succeed({ status: "done" as const }),
                cancel: () => Effect.sync(() => cancelled++),
              },
            }),
            (opened) => opened.session.cancel(),
          ),
      }),
    );
    const priorExit = await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* catchUp({
          source: prior.source,
          target: prior.target,
          lane: prior.lane,
          limits: generous,
          decode: jsonItems,
          reduce: (items) => items.map(fact),
        }).pipe(
          Effect.provide(DerivedRecoveryTest(() => Effect.succeed(priorPrevious))),
          Effect.provide(priorRead),
          Effect.provide(AppendStreamsLive),
          Effect.forkChild,
        );
        yield* Deferred.await(blocked);
        yield* Fiber.interrupt(fiber);
        return yield* Fiber.await(fiber);
      }),
    );
    expect(Exit.isFailure(priorExit) && Cause.hasInterrupts(priorExit.cause)).toBe(true);
    expect(cancelled).toBe(1);
    expect(await ready(prior)).toMatchObject({ sourceThrough: "00000001", nextProducerSeq: 1 });
    expect(await runScripted(prior, [{ offset: "00000001", items: [1] }])).toMatchObject({
      status: "caught-up",
      batches: 0,
    });
  });
});

function runScriptedEffect(
  h: Harness,
  pages: readonly ScriptedPage[],
  options: { readonly reduce?: (items: readonly JsonValue[]) => Iterable<JsonValue> } = {},
) {
  return Effect.promise(() => ready(h)).pipe(
    Effect.flatMap((recovered) =>
      catchUp({
        source: h.source,
        target: h.target,
        lane: h.lane,
        limits: generous,
        decode: jsonItems,
        reduce: options.reduce ?? ((items) => items.map(fact)),
      }).pipe(
        Effect.provide(DerivedRecoveryTest(() => Effect.succeed(recovered))),
        Effect.provide(scriptedReadLayer(pages)),
        Effect.provide(AppendStreamsLive),
      ),
    ),
  );
}

describe("Live adapter ambiguous append evidence", () => {
  test("direct resolved failure after commit is typed, changed retry bytes reconcile, and restart trusts one transaction", async () => {
    const h = await harness();
    let commits = 0;
    h.protocol.onAfterCommit(() => commits++);
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
    expect(typedError(exit)).toMatchObject({
      _tag: "StreamAppendError",
      code: "transport",
      durability: "unknown",
    });
    expect(typedError(exit)).toBeInstanceOf(StreamAppendError);
    expect(
      await Effect.runPromise(
        provideLive(
          appendDerivedStateBatch({
            target: lossy,
            lane: h.lane,
            previous,
            sourceThrough: "00000001",
            facts: [fact("changed-retry-bytes")],
          }),
        ),
      ),
    ).toMatchObject({ status: "sequence-already-accepted" });
    expect(await values(h)).toEqual([
      fact("accepted"),
      expect.objectContaining({ type: MESH_LINEAGE_TYPE }),
    ]);
    expect(commits).toBe(1);
    expect(await ready(h)).toMatchObject({ sourceThrough: "00000001", nextProducerSeq: 1 });
  });

  test("fetch lost response keeps original bytes durable and reconciles a changed-byte same-tuple retry", async () => {
    const adapter = createMemoryStorageAdapter();
    const protocol = new StreamProtocol({ storage: { adapter } });
    const handler = createHttpHandler({ protocol, pathPrefix: "/streams" });
    let lose = true;
    const producerTuples: Array<{
      readonly id: string | null;
      readonly epoch: string | null;
      readonly seq: string | null;
    }> = [];
    const fetch = (async (input: Parameters<typeof globalThis.fetch>[0], init?: RequestInit) => {
      const request = new Request(input, init);
      const response = await handler.fetch(request);
      if (request.method === "POST" && request.headers.has("producer-id")) {
        producerTuples.push({
          id: request.headers.get("producer-id"),
          epoch: request.headers.get("producer-epoch"),
          seq: request.headers.get("producer-seq"),
        });
        if (lose) {
          lose = false;
          throw new TypeError("response lost after durable commit");
        }
      }
      return response;
    }) as typeof globalThis.fetch;
    const client = officialProtocolClient({
      urlFor: (id) => protocolPathUrl("https://mesh.test/streams", id),
      fetch,
      backoffOptions: noRetry,
      warnOnHttp: false,
    });
    clients.add(client);
    const sourceIdentity = streamIdentity("orders");
    const targetIdentity = streamIdentity("orders-state");
    const target = bindStream({ identity: targetIdentity, client, streamId: "target" });
    const lane = await deriveProducerLane({
      processorId: "orders-state",
      processorVersion: "1",
      outputGeneration: "1",
      source: sourceIdentity,
      target: targetIdentity,
      producerEpoch: 1,
    });
    await client.stream("target").create({ contentType: "application/json" });
    let commits = 0;
    protocol.onAfterCommit(() => commits++);
    const previous = await Effect.runPromise(provideLive(recoverDerivedState(target, lane)));
    if (previous.status !== "ready") throw new Error("expected ready");
    const acceptedFact = fact("accepted");
    const changedRetryFact = fact("changed-retry-bytes");
    const exit = await Effect.runPromiseExit(
      provideLive(
        appendDerivedStateBatch({
          target,
          lane,
          previous,
          sourceThrough: "00000001",
          facts: [acceptedFact],
        }),
      ),
    );
    expect(typedError(exit)).toBeInstanceOf(StreamAppendError);
    expect(typedError(exit)).toMatchObject({ code: "transport", durability: "unknown" });

    const retry = await Effect.runPromise(
      provideLive(
        appendDerivedStateBatch({
          target,
          lane,
          previous,
          sourceThrough: "00000001",
          facts: [changedRetryFact],
        }),
      ),
    );
    expect(retry).toMatchObject({
      status: "sequence-already-accepted",
      checkpoint: {
        sourceThrough: "00000001",
        nextProducerSeq: 1,
        producerId: lane.producerId,
        producerEpoch: lane.producerEpoch,
      },
    });
    expect(JSON.stringify(retry)).not.toMatch(/payload|verif/i);
    expect(producerTuples).toEqual([
      { id: lane.producerId, epoch: String(lane.producerEpoch), seq: "0" },
      { id: lane.producerId, epoch: String(lane.producerEpoch), seq: "0" },
    ]);
    expect(commits).toBe(1);
    expect(await adapter.getProducerState("target", lane.producerId)).toEqual({
      epoch: lane.producerEpoch,
      lastSeq: 0,
    });
    const stored = await adapter.listMessages("target");
    expect(stored).toHaveLength(2);
    expect(stored[0]?.data).toEqual(new TextEncoder().encode(JSON.stringify(acceptedFact)));
    expect(stored[0]?.data).not.toEqual(new TextEncoder().encode(JSON.stringify(changedRetryFact)));
    expect(JSON.parse(new TextDecoder().decode(stored[1]?.data))).toMatchObject({
      type: MESH_LINEAGE_TYPE,
      value: {
        sourceThrough: "00000001",
        producerId: lane.producerId,
        producerEpoch: lane.producerEpoch,
        nextProducerSeq: 1,
      },
    });
    expect(await Effect.runPromise(provideLive(recoverDerivedState(target, lane)))).toMatchObject({
      sourceThrough: "00000001",
      nextProducerSeq: 1,
      producerId: lane.producerId,
      producerEpoch: lane.producerEpoch,
    });
  });

  test("interrupted Live append after commit stays interrupted and restart observes exactly one transaction", async () => {
    const h = await harness();
    let commits = 0;
    h.protocol.onAfterCommit(() => commits++);
    const previous = await ready(h);
    let committedResolve!: () => void;
    const committed = new Promise<void>((resolve) => {
      committedResolve = resolve;
    });
    const ambiguous = bindStream({
      ...h.target,
      client: blockResponseAfterCommit(h.client, committedResolve),
    });
    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* appendDerivedStateBatch({
          target: ambiguous,
          lane: h.lane,
          previous,
          sourceThrough: "00000001",
          facts: [fact(1)],
        }).pipe(
          Effect.provide(DerivedRecoveryLive),
          Effect.provide(ReadStreamsLive),
          Effect.provide(AppendStreamsLive),
          Effect.forkChild,
        );
        yield* Effect.promise(() => committed);
        yield* Fiber.interrupt(fiber);
        return yield* Fiber.await(fiber);
      }),
    );
    expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
    expect(await values(h)).toHaveLength(2);
    expect(commits).toBe(1);
    expect(await ready(h)).toMatchObject({ sourceThrough: "00000001", nextProducerSeq: 1 });
  });

  test("direct and fetch Live append framing is byte-identical and fetch uses one producer POST", async () => {
    const direct = await transportAppendHarness("direct");
    const remote = await transportAppendHarness("fetch");
    const directPrevious = await direct.ready();
    const remotePrevious = await remote.ready();
    expect(await direct.append(directPrevious)).toMatchObject({ status: "appended" });
    expect(await remote.append(remotePrevious)).toMatchObject({ status: "appended" });
    expect((await remote.adapter.listMessages("target")).map((message) => message.data)).toEqual(
      (await direct.adapter.listMessages("target")).map((message) => message.data),
    );
    expect(remote.producerPosts()).toBe(1);
  });
});

async function transportAppendHarness(transport: "direct" | "fetch") {
  const adapter = createMemoryStorageAdapter();
  const protocol = new StreamProtocol({ storage: { adapter } });
  let posts = 0;
  const handler = createHttpHandler({ protocol, pathPrefix: "/streams" });
  const fetch = (async (input: Parameters<typeof globalThis.fetch>[0], init?: RequestInit) => {
    const request = new Request(input, init);
    if (request.method === "POST" && request.headers.has("producer-id")) posts++;
    return handler.fetch(request);
  }) as typeof globalThis.fetch;
  const client =
    transport === "direct"
      ? directProtocolClient(protocol)
      : officialProtocolClient({
          urlFor: (id) => protocolPathUrl("https://mesh.test/streams", id),
          fetch,
          backoffOptions: noRetry,
          warnOnHttp: false,
        });
  clients.add(client);
  const sourceIdentity = streamIdentity("orders");
  const targetIdentity = streamIdentity("orders-state");
  const target = bindStream({ identity: targetIdentity, client, streamId: "target" });
  const lane = await deriveProducerLane({
    processorId: "orders-state",
    processorVersion: "1",
    outputGeneration: "1",
    source: sourceIdentity,
    target: targetIdentity,
    producerEpoch: 1,
  });
  await client.stream("target").create({ contentType: "application/json" });
  return {
    adapter,
    producerPosts: () => posts,
    ready: async () => {
      const result = await Effect.runPromise(provideLive(recoverDerivedState(target, lane)));
      if (result.status !== "ready") throw new Error("expected ready");
      return result;
    },
    append: (previous: RecoveredDerivedState) =>
      Effect.runPromise(
        provideLive(
          appendDerivedStateBatch({
            target,
            lane,
            previous,
            sourceThrough: "00000001",
            facts: [fact(1), fact(2)],
          }),
        ),
      ),
  };
}

function isLineage(value: unknown): boolean {
  return (
    typeof value === "object" &&
    value !== null &&
    "type" in value &&
    value.type === MESH_LINEAGE_TYPE
  );
}

function jsonItems(batch: StreamBatch): readonly JsonValue[] {
  if (batch.kind !== "json") throw new TypeError("expected JSON");
  return batch.items;
}

async function interruptAfterOpen<A, E>(program: Effect.Effect<A, E>, opened: Promise<void>) {
  return Effect.runPromise(
    Effect.gen(function* () {
      const fiber = yield* program.pipe(Effect.forkChild);
      yield* Effect.promise(() => opened);
      yield* Fiber.interrupt(fiber);
      return yield* Fiber.await(fiber);
    }),
  );
}

async function unusedClientOperation(): Promise<never> {
  throw new Error("unused client operation");
}

function blockedReadClient(onCancel: () => void): {
  readonly client: StreamProtocolClient;
  readonly opened: Promise<void>;
} {
  let openedResolve!: () => void;
  const opened = new Promise<void>((resolve) => {
    openedResolve = resolve;
  });
  const client: StreamProtocolClient = {
    stream(streamId): StreamProtocolHandle {
      return {
        id: streamId,
        head: unusedClientOperation,
        create: unusedClientOperation,
        append: unusedClientOperation,
        appendJsonBatch: unusedClientOperation,
        close: unusedClientOperation,
        read: async <T extends JsonValue>() => {
          const session = new ClientReadSession<T>({ startOffset: "-1" });
          const originalCancel = session.cancel.bind(session);
          session.cancel = (reason?: unknown) => {
            onCancel();
            originalCancel(reason);
          };
          const originalNext = session.next.bind(session);
          session.next = () => {
            openedResolve();
            return originalNext();
          };
          return { status: "ok", session };
        },
      };
    },
    async close() {},
  };
  return { client, opened };
}

function loseFirstAppendResponse(client: StreamProtocolClient): StreamProtocolClient {
  let lose = true;
  return mapAppend(client, async (delegate, items, options) => {
    const result = await delegate.appendJsonBatch(items, options);
    if (!lose) return result;
    lose = false;
    return {
      status: "error",
      code: "transport",
      message: "response lost after durable commit",
      retryable: true,
    };
  });
}

function blockResponseAfterCommit(
  client: StreamProtocolClient,
  committed: () => void,
): StreamProtocolClient {
  return mapAppend(client, async (delegate, items, options) => {
    const result = await delegate.appendJsonBatch(items, options);
    if (result.status !== "appended") return result;
    committed();
    return new Promise<ClientAppendResult>((_resolve, reject) => {
      const signal = options?.signal;
      if (signal?.aborted) {
        reject(signal.reason);
        return;
      }
      signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
    });
  });
}

function mapAppend(
  client: StreamProtocolClient,
  operation: (
    delegate: StreamProtocolHandle,
    items: readonly JsonValue[],
    options: AppendJsonBatchOptions | undefined,
  ) => Promise<ClientAppendResult>,
): StreamProtocolClient {
  return {
    stream(streamId): StreamProtocolHandle {
      const delegate = client.stream(streamId);
      return {
        id: delegate.id,
        head: (options) => delegate.head(options),
        create: (options) => delegate.create(options),
        append: (data, options) => delegate.append(data, options),
        appendJsonBatch: (items, options) => operation(delegate, items, options),
        close: (options) => delegate.close(options),
        read: (options) => delegate.read(options),
      };
    },
    close: (reason) => client.close(reason),
  };
}

function mapReads(
  client: StreamProtocolClient,
  operation: <T extends JsonValue>(
    delegate: StreamProtocolHandle,
    options: ReadStreamOptions | undefined,
  ) => Promise<ClientReadResult<T>>,
): StreamProtocolClient {
  return {
    stream(streamId): StreamProtocolHandle {
      const delegate = client.stream(streamId);
      return {
        id: delegate.id,
        head: (options) => delegate.head(options),
        create: (options) => delegate.create(options),
        append: (data, options) => delegate.append(data, options),
        appendJsonBatch: (items, options) => delegate.appendJsonBatch(items, options),
        close: (options) => delegate.close(options),
        read: <T extends JsonValue>(options?: ReadStreamOptions) => operation<T>(delegate, options),
      };
    },
    close: (reason) => client.close(reason),
  };
}
