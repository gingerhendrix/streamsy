import {
  ClientReadSession,
  StreamProtocol,
  createHttpHandler,
  createMemoryStorageAdapter,
  directProtocolClient,
  type JsonValue,
  type ReadStreamOptions,
  type StorageAdapter,
  type StreamBatch,
  type StreamProtocolClient,
  type StreamProtocolHandle,
} from "@streamsy/core";
import { officialProtocolClient, protocolPathUrl } from "@streamsy/client";
import { describe, expect, test } from "vitest";
import { bindStream, type StreamBinding } from "../binding.ts";
import { streamIdentity } from "../causal.ts";
import { recoverDerivedState } from "./derived-append.ts";
import { deriveProducerLane, type ProducerLane } from "./lane.ts";
import { catchUp, type CatchUpLimits } from "./projection.ts";
import { MESH_LINEAGE_TYPE } from "./state-meta.ts";

const decoder = new TextDecoder();
const generous: CatchUpLimits = {
  maxItems: 100,
  maxPages: 100,
  maxBatches: 100,
  maxBytes: 100_000,
};

interface Harness {
  adapter: StorageAdapter;
  targetClient: StreamProtocolClient;
  source: StreamBinding;
  target: StreamBinding;
  lane: ProducerLane;
}

async function harness(
  pages: readonly Page[],
  targetClient?: StreamProtocolClient,
): Promise<Harness> {
  const adapter = createMemoryStorageAdapter();
  const client = targetClient ?? directProtocolClient(new StreamProtocol({ storage: { adapter } }));
  const sourceIdentity = streamIdentity("orders");
  const targetIdentity = streamIdentity("orders-by-status");
  const source = bindStream({
    identity: sourceIdentity,
    client: scriptedSourceClient(pages),
    streamId: "source",
  });
  const target = bindStream({ identity: targetIdentity, client, streamId: "derived" });
  const lane = await deriveProducerLane({
    processorId: "orders-by-status",
    processorVersion: "1.0.0",
    outputGeneration: "generation-1",
    source: sourceIdentity,
    target: targetIdentity,
    producerEpoch: 7,
  });
  await client.stream("derived").create({ contentType: "application/json" });
  return { adapter, targetClient: client, source, target, lane };
}

type Page = { readonly offset: string; readonly items: readonly JsonValue[] };

function scriptedSourceClient(pages: readonly Page[]): StreamProtocolClient {
  return {
    stream(streamId): StreamProtocolHandle {
      if (streamId !== "source") throw new TypeError(`Unexpected stream ${streamId}`);
      return {
        id: streamId,
        head: notUsed,
        create: notUsed,
        append: notUsed,
        appendJsonBatch: notUsed,
        close: notUsed,
        async read<T extends JsonValue = JsonValue>(options?: ReadStreamOptions) {
          const startOffset = options?.offset ?? "-1";
          const session = new ClientReadSession<T>({ startOffset });
          void (async () => {
            for (const page of pages) {
              if (page.offset <= startOffset) continue;
              await session.deliver({
                kind: "json",
                items: page.items as readonly T[],
                offset: page.offset,
                upToDate: false,
                streamClosed: false,
              });
            }
            session.end({ status: "done" });
          })();
          return { status: "ok", session };
        },
      };
    },
    async close() {},
  };
}

async function notUsed(): Promise<never> {
  throw new Error("unused scripted source operation");
}

function fact(item: JsonValue): JsonValue {
  return {
    type: "order",
    key: `o-${String(item)}`,
    value: { id: `o-${String(item)}`, value: item },
    headers: { operation: "upsert" },
  };
}

function projectionOptions(h: Harness, overrides: Record<string, unknown> = {}) {
  return {
    source: h.source,
    target: h.target,
    lane: h.lane,
    limits: generous,
    decode(batch: StreamBatch) {
      if (batch.kind !== "json") throw new Error("expected JSON");
      return batch.items;
    },
    reduce(items: readonly JsonValue[]) {
      return items.map(fact);
    },
    ...overrides,
  };
}

async function values(adapter: StorageAdapter): Promise<unknown[]> {
  return (await adapter.listMessages("derived")).map(
    (message) => JSON.parse(decoder.decode(message.data)) as unknown,
  );
}

describe("bounded one-source projection — memory", () => {
  test("commits each delivery boundary once and advances a filtered boundary with lineage only", async () => {
    const h = await harness([
      { offset: "00000001", items: [1] },
      { offset: "00000002", items: [2] },
    ]);
    const result = await catchUp(
      projectionOptions(h, {
        reduce(items: readonly JsonValue[]) {
          return items[0] === 2 ? [] : items.map(fact);
        },
      }),
    );
    expect(result).toMatchObject({
      status: "caught-up",
      pages: 2,
      batches: 2,
      items: 2,
      checkpoint: { sourceThrough: "00000002", nextProducerSeq: 2 },
    });
    expect(await values(h.adapter)).toEqual([
      fact(1),
      expect.objectContaining({ type: MESH_LINEAGE_TYPE }),
      expect.objectContaining({
        type: MESH_LINEAGE_TYPE,
        value: expect.objectContaining({ sourceThrough: "00000002" }),
      }),
    ]);
    await h.targetClient.close();
  });

  test("batch limit leaves the next complete boundary for deterministic continuation", async () => {
    const pages = [
      { offset: "00000001", items: [1] },
      { offset: "00000002", items: [2] },
      { offset: "00000003", items: [3] },
    ];
    const h = await harness(pages);
    const first = await catchUp(projectionOptions(h, { limits: { ...generous, maxBatches: 1 } }));
    expect(first).toMatchObject({
      status: "limit-reached",
      limit: "maxBatches",
      batches: 1,
      checkpoint: { sourceThrough: "00000001", nextProducerSeq: 1 },
    });
    const second = await catchUp(projectionOptions(h));
    expect(second).toMatchObject({
      status: "caught-up",
      batches: 2,
      checkpoint: { sourceThrough: "00000003", nextProducerSeq: 3 },
    });
    expect((await values(h.adapter)).filter(isLineage)).toHaveLength(3);
    await h.targetClient.close();
  });

  test.each([
    ["maxPages", { ...generous, maxPages: 1 }, 1],
    ["maxItems", { ...generous, maxItems: 1 }, 1],
    ["maxBytes", { ...generous, maxBytes: 3 }, 1],
  ] as const)("enforces %s only at complete boundaries", async (limit, limits, committed) => {
    const h = await harness([
      { offset: "00000001", items: [1] },
      { offset: "00000002", items: [2] },
    ]);
    const result = await catchUp(projectionOptions(h, { limits }));
    expect(result).toMatchObject({ status: "limit-reached", limit, batches: committed });
    const recovered = await recoverDerivedState(h.target, h.lane);
    expect(recovered).toMatchObject({ status: "ready", sourceThrough: "00000001" });
    await h.targetClient.close();
  });

  test("decode and reduce poison identify the source boundary without advancing it", async () => {
    for (const phase of ["decode", "reduce"] as const) {
      const h = await harness([
        { offset: "00000001", items: [1] },
        { offset: "00000002", items: [2] },
      ]);
      const poisonOverride =
        phase === "decode"
          ? {
              decode(batch: StreamBatch) {
                if (batch.offset === "00000002") throw new Error("bad source");
                return batch.kind === "json" ? batch.items : [];
              },
            }
          : {
              reduce(items: readonly JsonValue[], boundary: { source: { position: string } }) {
                if (boundary.source.position === "00000002") throw new Error("bad reduction");
                return items.map(fact);
              },
            };
      const result = await catchUp(projectionOptions(h, poisonOverride));
      expect(result).toMatchObject({
        status: "poison",
        phase,
        source: { identity: { name: "orders" }, position: "00000002" },
        checkpoint: { sourceThrough: "00000001" },
      });
      expect(await recoverDerivedState(h.target, h.lane)).toMatchObject({
        status: "ready",
        sourceThrough: "00000001",
      });
      await h.targetClient.close();
    }
  });

  test("cancellation between decode/reduce, before append, and after commit is explicit", async () => {
    const readAbort = new AbortController();
    const h0 = await harness([{ offset: "00000001", items: [1] }]);
    const abortingTarget = bindStream({
      ...h0.target,
      client: abortAfterRecovery(h0.targetClient, readAbort),
    });
    expect(
      await catchUp(projectionOptions(h0, { target: abortingTarget, signal: readAbort.signal })),
    ).toMatchObject({ status: "cancelled", phase: "read", durableProgress: "none" });
    expect(await values(h0.adapter)).toEqual([]);
    await h0.targetClient.close();

    const decodeAbort = new AbortController();
    const h1 = await harness([{ offset: "00000001", items: [1] }]);
    const decoded = await catchUp(
      projectionOptions(h1, {
        signal: decodeAbort.signal,
        decode(batch: StreamBatch) {
          decodeAbort.abort();
          return batch.kind === "json" ? batch.items : [];
        },
      }),
    );
    expect(decoded).toMatchObject({
      status: "cancelled",
      phase: "decode",
      durableProgress: "none",
    });
    expect(await values(h1.adapter)).toEqual([]);
    await h1.targetClient.close();

    const appendAbort = new AbortController();
    const h2 = await harness([{ offset: "00000001", items: [1] }]);
    const beforeAppend = await catchUp(
      projectionOptions(h2, {
        signal: appendAbort.signal,
        reduce(items: readonly JsonValue[]) {
          appendAbort.abort();
          return items.map(fact);
        },
      }),
    );
    expect(beforeAppend).toMatchObject({
      status: "cancelled",
      phase: "append",
      durableProgress: "none",
    });
    expect(await values(h2.adapter)).toEqual([]);
    await h2.targetClient.close();

    const afterAbort = new AbortController();
    const h3 = await harness([{ offset: "00000001", items: [1] }]);
    h3.target = bindStream({
      ...h3.target,
      client: abortAfterAppend(h3.targetClient, afterAbort),
    });
    const afterCommit = await catchUp(
      projectionOptions(h3, { target: h3.target, signal: afterAbort.signal }),
    );
    expect(afterCommit).toMatchObject({
      status: "cancelled",
      phase: "after-commit",
      durableProgress: "committed",
      checkpoint: { sourceThrough: "00000001" },
    });
    expect(await recoverDerivedState(h3.target, h3.lane)).toMatchObject({
      status: "ready",
      sourceThrough: "00000001",
    });
    await h3.targetClient.close();
  });

  test("wrong source identity is programmer misuse and wrong generation halts recovery", async () => {
    const h = await harness([{ offset: "00000001", items: [1] }]);
    const wrongSource = bindStream({
      ...h.source,
      identity: streamIdentity("other-source"),
    });
    await expect(catchUp(projectionOptions(h, { source: wrongSource }))).rejects.toThrow(
      /Source binding identity/,
    );
    await catchUp(projectionOptions(h));
    const wrongLane = await deriveProducerLane({ ...h.lane, outputGeneration: "generation-2" });
    expect(await catchUp(projectionOptions(h, { lane: wrongLane }))).toMatchObject({
      status: "incompatible-output",
    });
    await h.targetClient.close();
  });

  test("lost output response is recovered on restart without a second transaction", async () => {
    const h = await harness([{ offset: "00000001", items: [1] }]);
    const lossyTarget = bindStream({
      ...h.target,
      client: loseFirstAppendResponse(h.targetClient),
    });
    expect(await catchUp(projectionOptions(h, { target: lossyTarget }))).toMatchObject({
      status: "retryable",
      phase: "append",
    });
    expect(await values(h.adapter)).toHaveLength(2);
    expect(await catchUp(projectionOptions(h))).toMatchObject({
      status: "caught-up",
      batches: 0,
      checkpoint: { sourceThrough: "00000001", nextProducerSeq: 1 },
    });
    expect(await values(h.adapter)).toHaveLength(2);
    await h.targetClient.close();
  });

  test("target contention returns an output conflict without lineage advance", async () => {
    const h = await harness([{ offset: "00000001", items: [1] }]);
    const racingTarget = bindStream({
      ...h.target,
      client: raceFirstAppend(h.targetClient),
    });
    expect(await catchUp(projectionOptions(h, { target: racingTarget }))).toMatchObject({
      status: "output-conflict",
      reason: "expected-offset",
      batches: 0,
    });
    expect(await values(h.adapter)).toEqual([fact("foreign")]);
    await h.targetClient.close();
  });
});

test("direct and fetch sources derive identical rows and lineage", async () => {
  const directSourceAdapter = createMemoryStorageAdapter();
  const directSourceProtocol = new StreamProtocol({ storage: { adapter: directSourceAdapter } });
  const directSourceClient = directProtocolClient(directSourceProtocol);
  await directSourceClient.stream("source").create({ contentType: "application/json" });
  await directSourceClient.stream("source").appendJsonBatch([1, 2]);

  const fetchSourceAdapter = createMemoryStorageAdapter();
  const fetchSourceProtocol = new StreamProtocol({ storage: { adapter: fetchSourceAdapter } });
  const handler = createHttpHandler({ protocol: fetchSourceProtocol, pathPrefix: "/streams" });
  const fetchSourceClient = officialProtocolClient({
    urlFor: (id) => protocolPathUrl("https://stream.test/streams", id),
    fetch: ((input, init) => handler.fetch(new Request(input, init))) as typeof globalThis.fetch,
    backoffOptions: { initialDelay: 1, maxDelay: 1, multiplier: 1, maxRetries: 0 },
    warnOnHttp: false,
  });
  await fetchSourceClient.stream("source").create({ contentType: "application/json" });
  await fetchSourceClient.stream("source").appendJsonBatch([1, 2]);

  const direct = await realSourceHarness(directSourceClient);
  const remote = await realSourceHarness(fetchSourceClient);
  expect(await catchUp(projectionOptions(direct))).toMatchObject({
    status: "caught-up",
    batches: 1,
  });
  expect(await catchUp(projectionOptions(remote))).toMatchObject({
    status: "caught-up",
    batches: 1,
  });
  expect(await values(remote.adapter)).toEqual(await values(direct.adapter));
  await direct.targetClient.close();
  await remote.targetClient.close();
  await directSourceClient.close();
  await fetchSourceClient.close();
});

async function realSourceHarness(sourceClient: StreamProtocolClient): Promise<Harness> {
  const h = await harness([]);
  h.source = bindStream({ ...h.source, client: sourceClient });
  return h;
}

function isLineage(value: unknown): boolean {
  return (
    typeof value === "object" &&
    value !== null &&
    "type" in value &&
    value.type === MESH_LINEAGE_TYPE
  );
}

function abortAfterAppend(
  client: StreamProtocolClient,
  controller: AbortController,
): StreamProtocolClient {
  return {
    stream(streamId): StreamProtocolHandle {
      const delegate = client.stream(streamId);
      return {
        id: delegate.id,
        head: (options) => delegate.head(options),
        create: (options) => delegate.create(options),
        append: (data, options) => delegate.append(data, options),
        close: (options) => delegate.close(options),
        read: (options) => delegate.read(options),
        async appendJsonBatch(items, options) {
          const result = await delegate.appendJsonBatch(items, options);
          controller.abort();
          return result;
        },
      };
    },
    close: (reason) => client.close(reason),
  };
}

function abortAfterRecovery(
  client: StreamProtocolClient,
  controller: AbortController,
): StreamProtocolClient {
  let recoveryRead = true;
  return {
    stream(streamId): StreamProtocolHandle {
      const delegate = client.stream(streamId);
      return {
        id: delegate.id,
        head: (options) => delegate.head(options),
        create: (options) => delegate.create(options),
        append: (data, options) => delegate.append(data, options),
        close: (options) => delegate.close(options),
        appendJsonBatch: (items, options) => delegate.appendJsonBatch(items, options),
        async read<T extends JsonValue = JsonValue>(options?: ReadStreamOptions) {
          const result = await delegate.read<T>(options);
          if (result.status === "ok" && recoveryRead) {
            recoveryRead = false;
            void result.session.done.then(() => controller.abort());
          }
          return result;
        },
      };
    },
    close: (reason) => client.close(reason),
  };
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
      message: "response lost after commit",
      retryable: true,
    };
  });
}

function raceFirstAppend(client: StreamProtocolClient): StreamProtocolClient {
  let race = true;
  return mapAppend(client, async (delegate, items, options) => {
    if (race) {
      race = false;
      await delegate.appendJsonBatch([fact("foreign")]);
    }
    return delegate.appendJsonBatch(items, options);
  });
}

function mapAppend(
  client: StreamProtocolClient,
  operation: (
    delegate: StreamProtocolHandle,
    items: readonly JsonValue[],
    options: Parameters<StreamProtocolHandle["appendJsonBatch"]>[1],
  ) => ReturnType<StreamProtocolHandle["appendJsonBatch"]>,
): StreamProtocolClient {
  return {
    stream(streamId): StreamProtocolHandle {
      const delegate = client.stream(streamId);
      return {
        id: delegate.id,
        head: (options) => delegate.head(options),
        create: (options) => delegate.create(options),
        append: (data, options) => delegate.append(data, options),
        close: (options) => delegate.close(options),
        read: (options) => delegate.read(options),
        appendJsonBatch: (items, options) => operation(delegate, items, options),
      };
    },
    close: (reason) => client.close(reason),
  };
}
