/* oxlint-disable anti-slop/no-chained-type-assertions -- Namespace doubles model the workers binding boundary. */
import { expect, test } from "bun:test";
import type { DurableObjectNamespace } from "@cloudflare/workers-types";
import { Effect, Layer, Option, Stream } from "effect";
import {
  Offset,
  Storage,
  StreamId,
  StreamRecord,
  StreamsWriter,
  type CreateOptions,
  type StoredMessage,
} from "@streamsy/core";
import { Placement } from "./placement.ts";
import { decodeFrames, encodeFrames } from "./fork-frames.ts";
import { FORK_SOURCE_CONTENT_TYPE, FORK_SOURCE_MARKER } from "./fork-source.ts";
import { fetchForkSource, makeForkWriter, sourceView, type ForkHost } from "./fork-writer.ts";
import { DEFAULT_COPY_ON_FORK_MAX_BYTES, validateCopyOnForkMaxBytes } from "./object-options.ts";

const sourceId = StreamId.make("source");
const childId = StreamId.make("child");
const first: StoredMessage = {
  offset: Offset.make("0000000000000000_0000000000000001"),
  timestamp: 123,
  data: new Uint8Array([1]),
};

const sourceHeaders = (currentOffset = first.offset): HeadersInit => ({
  "streamsy-fork-source": FORK_SOURCE_MARKER,
  "streamsy-source-content-type": "text/plain",
  "streamsy-source-next-offset": currentOffset,
  "streamsy-source-created-at": "100",
});

const responseBody = (bytes: Uint8Array): ArrayBuffer => {
  const output = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(output).set(bytes);
  return output;
};

const namespaceFor = (response: Response) => {
  let fetches = 0;
  // SAFETY: this test double implements the namespace methods exercised by the fork boundary.
  const namespace = {
    idFromName: (name: string) => ({ name }),
    get: () => ({
      fetch: () => {
        fetches += 1;
        return response;
      },
    }),
  } as unknown as DurableObjectNamespace;
  return { namespace, fetches: () => fetches };
};

const hostFor = (namespace?: DurableObjectNamespace): ForkHost => ({
  namespace,
  placement: Placement.byStream(),
  copyOnForkMaxBytes: 1024,
});

const runFetch = (response: Response, options: CreateOptions = {}) => {
  const binding = namespaceFor(response);
  return fetchForkSource(hostFor(binding.namespace), "source", sourceId, {
    forkedFrom: "source",
    ...options,
  }).pipe(Effect.map((result) => ({ result, binding })));
};

const storageWithRecord = (records: ReadonlyMap<StreamId, Option.Option<StreamRecord>>) =>
  Storage.of({
    capabilities: { fork: "chain", atomicScope: "store", wake: "push", expiryIndex: "indexed" },
    record: (id) => Effect.succeed(records.get(id) ?? Option.none<StreamRecord>()),
    messages: () => Effect.succeed([]),
    producer: () => Effect.succeed(Option.none()),
    mutate: () => Effect.die("unused"),
    changes: () => Stream.empty,
    nextExpiry: Effect.succeed(Option.none()),
  });

test("fetchForkSource decodes a marked response and preserves the request contract", async () => {
  const body = encodeFrames([first]);
  const result = await Effect.runPromise(
    runFetch(
      new Response(responseBody(body), {
        status: 200,
        headers: { ...sourceHeaders(), "content-type": FORK_SOURCE_CONTENT_TYPE },
      }),
    ),
  );
  expect(result.result.record).toMatchObject({ _tag: "Some" });
  expect(result.result.messages).toEqual(decodeFrames(body));
  expect(result.result.truncated).toBe(false);
  expect(result.binding.fetches()).toBe(1);
});

test("fetchForkSource maps 404 and 410 to source snapshots", async () => {
  const missing = await Effect.runPromise(
    runFetch(new Response("missing", { status: 404, headers: { "streamsy-fork-source": "1" } })),
  );
  expect(missing.result).toEqual({ record: Option.none(), messages: [], truncated: false });

  const deleted = await Effect.runPromise(
    runFetch(
      new Response("gone", {
        status: 410,
        headers: sourceHeaders(),
      }),
    ),
  );
  expect(deleted.result.record).toMatchObject({
    _tag: "Some",
    value: { lifecycle: { softDeleted: true } },
  });
});

test("fetchForkSource turns marker, body, and cap violations into StorageFault", async () => {
  const invalidMarker = Effect.runPromise(
    runFetch(
      new Response("", {
        status: 200,
        headers: { "content-type": FORK_SOURCE_CONTENT_TYPE },
      }),
    ).pipe(Effect.flip),
  );
  await expect(invalidMarker).resolves.toMatchObject({
    _tag: "StorageFault",
    operation: "fork.source",
  });

  const malformed = Effect.runPromise(
    runFetch(
      new Response(responseBody(new Uint8Array([1])), {
        status: 200,
        headers: { ...sourceHeaders(), "content-type": FORK_SOURCE_CONTENT_TYPE },
      }),
    ).pipe(Effect.flip),
  );
  await expect(malformed).resolves.toMatchObject({
    _tag: "StorageFault",
    operation: "fork.source",
  });

  const overCap = namespaceFor(
    new Response(null, {
      status: 200,
      headers: {
        ...sourceHeaders(),
        "content-type": FORK_SOURCE_CONTENT_TYPE,
        "content-length": "2",
      },
    }),
  );
  await expect(
    Effect.runPromise(
      fetchForkSource(
        { ...hostFor(overCap.namespace), copyOnForkMaxBytes: 1 },
        "source",
        sourceId,
        { forkedFrom: "source" },
      ).pipe(Effect.flip),
    ),
  ).resolves.toMatchObject({ _tag: "StorageFault", operation: "fork.source" });
});

test("sourceView filters source windows and changes fork capability to copy", async () => {
  const source = storageWithRecord(
    new Map([
      [
        sourceId,
        Option.some({
          id: sourceId,
          config: { contentType: "text/plain", createdAt: 1 },
          lifecycle: { closed: false, softDeleted: false },
          currentOffset: first.offset,
        }),
      ],
    ]),
  );
  const second: StoredMessage = {
    offset: Offset.make("0000000000000000_0000000000000002"),
    timestamp: 124,
    data: new Uint8Array([2]),
  };
  const view = sourceView(source, sourceId, {
    record: Option.some({
      id: sourceId,
      config: { contentType: "text/plain", createdAt: 1 },
      lifecycle: { closed: false, softDeleted: false },
      currentOffset: second.offset,
    }),
    messages: [first, second],
    truncated: false,
  });
  expect(view.capabilities.fork).toBe("copy");
  expect(
    await Effect.runPromise(
      view.messages(sourceId, { after: first.offset, until: second.offset, limit: 1 }),
    ),
  ).toEqual([second]);
});

test("makeForkWriter handles ordinary, same-object, missing-namespace, and limit paths", async () => {
  const calls: Array<string> = [];
  const real = StreamsWriter.of({
    create: (id: StreamId) =>
      Effect.sync(() => {
        calls.push(id);
        return { status: "created", nextOffset: first.offset, contentType: "text/plain" } as const;
      }),
    fork: () => Effect.die("unused"),
    append: () => Effect.die("unused"),
    remove: () => Effect.die("unused"),
  });
  const storage = storageWithRecord(new Map());
  const run = (host: ForkHost) =>
    Effect.runPromise(
      makeForkWriter(host).pipe(
        Effect.provide(
          Layer.mergeAll(Layer.succeed(Storage, storage), Layer.succeed(StreamsWriter, real)),
        ),
      ),
    );

  const writer = await run(hostFor());
  expect(await Effect.runPromise(writer.create(StreamId.make("ordinary")))).toMatchObject({
    status: "created",
  });
  expect(await Effect.runPromise(writer.create(childId, { forkedFrom: "source" }))).toEqual({
    status: "not-supported",
    feature: "fork",
  });
  expect(await Effect.runPromise(writer.create(sourceId, { forkedFrom: "source" }))).toMatchObject({
    status: "created",
  });
  expect(calls).toContain("ordinary");

  const truncated = new Response(null, {
    status: 200,
    headers: {
      ...sourceHeaders(),
      "content-type": FORK_SOURCE_CONTENT_TYPE,
      "content-length": "0",
      "streamsy-frames-truncated": "1",
    },
  });
  const boundedWriter = await run({
    ...hostFor(namespaceFor(truncated).namespace),
    copyOnForkMaxBytes: 10,
  });
  expect(
    await Effect.runPromise(boundedWriter.create(childId, { forkedFrom: "source" })),
  ).toMatchObject({
    status: "conflict",
    conflictReason: "fork-copy-limit",
  });
});

test("ObjectOptions copy bound defaults and rejects non-positive unsafe values", () => {
  expect(validateCopyOnForkMaxBytes(undefined)).toBe(DEFAULT_COPY_ON_FORK_MAX_BYTES);
  for (const value of [
    0,
    -1,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    1.5,
    Number.MAX_SAFE_INTEGER + 1,
  ])
    expect(() => validateCopyOnForkMaxBytes(value)).toThrow(RangeError);
});
