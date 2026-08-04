import { officialProtocolClient, protocolPathUrl } from "@streamsy/client";
import {
  StreamProtocol,
  createHttpHandler,
  createMemoryStorageAdapter,
  directProtocolClient,
  type JsonValue,
  type StorageAdapter,
  type StreamProtocolClient,
} from "@streamsy/core";
import { Cause, Deferred, Effect, Exit, Fiber, Option, Ref } from "effect";
import { afterEach, describe, expect, test } from "vitest";
import { bindStream, type StreamBinding } from "../binding.ts";
import { streamIdentity } from "../causal.ts";
import { AppendStreamsLive, ReadStreamsLive } from "../effect/streams.ts";
import { ProjectionPoison } from "../effect/errors.ts";
import { TestStreamsLayer } from "../effect/testing.ts";
import {
  appendDerivedStateBatch,
  DerivedRecoveryLive,
  DerivedRecoveryTest,
  recoverDerivedState,
  type RecoveredDerivedState,
} from "./derived-append.ts";
import { deriveProducerLane, type ProducerLane } from "./lane.ts";
import { catchUp } from "./projection.ts";
import {
  MESH_LINEAGE_TYPE,
  createLineageEvent,
  decodeLineageEvent,
  ensureLineageCompatible,
} from "./state-meta.ts";

const clients = new Set<StreamProtocolClient>();
const limits = { maxItems: 100, maxPages: 100, maxBatches: 100, maxBytes: 100_000 };

afterEach(async () => {
  await Promise.all(Array.from(clients, (client) => client.close()));
  clients.clear();
});

interface Harness {
  readonly adapter: StorageAdapter;
  readonly client: StreamProtocolClient;
  readonly source: StreamBinding;
  readonly target: StreamBinding;
  readonly lane: ProducerLane;
}

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
  program.pipe(
    Effect.provide(DerivedRecoveryLive),
    Effect.provide(ReadStreamsLive),
    Effect.provide(AppendStreamsLive),
  );

function fact(value: JsonValue): JsonValue {
  return { type: "order", key: `o-${String(value)}`, value, headers: { operation: "upsert" } };
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

describe("Effect-first mesh", () => {
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

  test("duplicate reconciliation is an outcome and does not claim payload verification", async () => {
    const h = await harness();
    const previous = await Effect.runPromise(provideLive(recoverDerivedState(h.target, h.lane)));
    if (previous.status !== "ready") throw new Error("expected ready");
    const accepted = await Effect.runPromise(
      provideLive(
        appendDerivedStateBatch({
          target: h.target,
          lane: h.lane,
          previous,
          sourceThrough: "00000001",
          facts: [fact(1)],
        }),
      ),
    );
    expect(accepted.status).toBe("appended");
    const duplicate = await Effect.runPromise(
      provideLive(
        appendDerivedStateBatch({
          target: h.target,
          lane: h.lane,
          previous,
          sourceThrough: "00000001",
          facts: [fact(999)],
        }),
      ),
    );
    expect(duplicate.status).toBe("sequence-already-accepted");
    expect(JSON.stringify(duplicate)).not.toContain("verified");
    expect(await h.adapter.listMessages(h.target.streamId)).toHaveLength(2);
  });

  test("expected-offset contention remains an explicit output-conflict", async () => {
    const h = await harness();
    const previous = await Effect.runPromise(provideLive(recoverDerivedState(h.target, h.lane)));
    if (previous.status !== "ready") throw new Error("expected ready");
    await h.client.stream(h.target.streamId).appendJsonBatch([fact("foreign")]);
    const result = await Effect.runPromise(
      provideLive(
        appendDerivedStateBatch({
          target: h.target,
          lane: h.lane,
          previous,
          sourceThrough: "00000001",
          facts: [fact(1)],
        }),
      ),
    );
    expect(result).toMatchObject({ status: "output-conflict", reason: "expected-offset" });
  });

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
      expect(Option.isSome(error) && error.value instanceof ProjectionPoison).toBe(true);
    }
    expect(await h.adapter.listMessages(h.target.streamId)).toHaveLength(0);
  });

  test("interruption stays interruption while an in-flight append has unknown durability", async () => {
    const h = await harness();
    const previous: RecoveredDerivedState = {
      status: "ready",
      targetOffset: "-1",
      nextProducerSeq: 0,
      producerId: h.lane.producerId,
      producerEpoch: h.lane.producerEpoch,
    };
    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        const appendStarted = yield* Deferred.make<void>();
        const nextCount = yield* Ref.make(0);
        const layer = TestStreamsLayer({
          read: {
            open: () =>
              Effect.succeed({
                status: "ok" as const,
                session: {
                  startOffset: "-1",
                  next: Ref.getAndUpdate(nextCount, (n) => n + 1).pipe(
                    Effect.map((n) =>
                      n === 0
                        ? {
                            done: false as const,
                            value: {
                              kind: "json" as const,
                              items: [1],
                              offset: "00000001",
                              upToDate: false,
                              streamClosed: false,
                            },
                          }
                        : { done: true as const, value: undefined },
                    ),
                  ),
                  done: Effect.succeed({ status: "done" as const }),
                  cancel: () => Effect.void,
                },
              }),
          },
          append: {
            append: () => Effect.die("unused"),
            appendJsonBatch: () =>
              Deferred.succeed(appendStarted, undefined).pipe(Effect.andThen(Effect.never)),
          },
        });
        const program = projection(h).pipe(
          Effect.provide(DerivedRecoveryTest(() => Effect.succeed(previous))),
          Effect.provide(layer),
        );
        const fiber = yield* Effect.forkChild(program);
        yield* Deferred.await(appendStarted);
        yield* Fiber.interrupt(fiber);
        return yield* Fiber.await(fiber);
      }),
    );
    expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
  });
});
