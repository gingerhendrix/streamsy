import type { DurableObjectNamespace } from "@cloudflare/workers-types";
import { Effect, Option, Schema } from "effect";
import {
  Protocol,
  Storage,
  StorageFault,
  StreamId,
  StreamRecord,
  StreamsWriter,
  type CreateOptions,
  type CreateOutcome,
  type StorageShape,
  type StoredMessage,
} from "@streamsy/core";
import type { Placement } from "./placement.ts";
import {
  FORK_SOURCE_CONTENT_TYPE,
  FORK_SOURCE_HOST,
  FORK_SOURCE_MARKER,
  FORK_SOURCE_PATH,
} from "./fork-source.ts";
import { decodeFrames } from "./fork-frames.ts";

const OFFSET_PATTERN = /^\d{16}_\d{16}$/;

export interface ForkHost {
  readonly namespace?: DurableObjectNamespace;
  readonly placement: Placement;
  readonly copyOnForkMaxBytes: number;
}

export interface ForkSnapshot {
  readonly record: Option.Option<StreamRecord>;
  readonly messages: ReadonlyArray<StoredMessage>;
  readonly truncated: boolean;
}

const forkFault = (cause: unknown): StorageFault =>
  new StorageFault({
    operation: "fork.source",
    message: cause instanceof Error ? cause.message : String(cause),
    retryable: true,
    cause,
  });

const readCapped = (response: Response, budget: number): Promise<Uint8Array> =>
  new Promise((resolve, reject) => {
    const body = response.body;
    if (body === null) {
      resolve(new Uint8Array());
      return;
    }
    const reader = body.getReader();
    const chunks: Array<Uint8Array> = [];
    const readLimit = budget === Number.MAX_SAFE_INTEGER ? budget : budget + 1;
    let total = 0;
    const fail = () => {
      void reader.cancel().catch(() => undefined);
      reject(new Error("Fork source body exceeds copyOnForkMaxBytes"));
    };
    const pump = (): void => {
      void reader.read().then(({ done, value }) => {
        if (done) {
          if (total > budget) fail();
          else {
            const output = new Uint8Array(total);
            let position = 0;
            for (const chunk of chunks) {
              output.set(chunk, position);
              position += chunk.byteLength;
            }
            resolve(output);
          }
          return;
        }
        if (value === undefined) {
          fail();
          return;
        }
        const room = Math.max(0, readLimit - total);
        const kept = value.byteLength > room ? value.subarray(0, room) : value;
        chunks.push(kept);
        total += kept.byteLength;
        if (total > budget || kept.byteLength < value.byteLength) {
          fail();
          return;
        }
        pump();
      }, reject);
    };
    pump();
  });

const parseContentLength = (response: Response, budget: number): void => {
  const raw = response.headers.get("content-length");
  if (raw === null) return;
  if (!/^(0|[1-9]\d*)$/.test(raw)) throw new Error("Invalid fork source content length");
  const length = Number(raw);
  if (!Number.isSafeInteger(length) || length > budget)
    throw new Error("Fork source body exceeds copyOnForkMaxBytes");
};

const requiredHeader = (response: Response, name: string): string => {
  const value = response.headers.get(name);
  if (value === null || value.length === 0) throw new Error(`Missing fork source header: ${name}`);
  return value;
};

const optionalFiniteHeader = (response: Response, name: string): number | undefined => {
  const value = response.headers.get(name);
  if (value === null) return undefined;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new Error(`Invalid fork source header: ${name}`);
  return parsed;
};

const decodeRecord = (response: Response, sourceId: StreamId, softDeleted: boolean) =>
  Effect.try({
    try: () => {
      const contentType = requiredHeader(response, "streamsy-source-content-type");
      const currentOffset = requiredHeader(response, "streamsy-source-next-offset");
      const createdAt = optionalFiniteHeader(response, "streamsy-source-created-at");
      if (createdAt === undefined)
        throw new Error("Missing fork source header: streamsy-source-created-at");
      const ttlSeconds = optionalFiniteHeader(response, "streamsy-source-ttl");
      const expiresAt = response.headers.get("streamsy-source-expires-at");
      return {
        id: sourceId,
        config: {
          contentType,
          createdAt,
          ...(ttlSeconds === undefined ? {} : { ttlSeconds }),
          ...(expiresAt === null ? {} : { expiresAt }),
        },
        lifecycle: { closed: false, softDeleted },
        currentOffset,
      };
    },
    catch: forkFault,
  }).pipe(
    Effect.flatMap((unknownRecord) =>
      Schema.decodeUnknownEffect(StreamRecord)(unknownRecord).pipe(
        Effect.mapError((error) => forkFault(error)),
      ),
    ),
  );

export const fetchForkSource = (
  host: ForkHost,
  sourceName: string,
  sourceId: StreamId,
  options: CreateOptions,
): Effect.Effect<ForkSnapshot, StorageFault> => {
  if (host.namespace === undefined) return Effect.fail(forkFault("Fork namespace is unavailable"));
  const namespace = host.namespace;
  const rawOffset = options.forkOffset;
  const validOffset =
    rawOffset !== undefined && OFFSET_PATTERN.test(rawOffset) ? rawOffset : undefined;
  const invalidOffset = rawOffset !== undefined && validOffset === undefined;
  const tail =
    options.forkSubOffset !== undefined && options.forkSubOffset > 0 ? options.forkSubOffset : 0;
  const budget = invalidOffset ? 0 : host.copyOnForkMaxBytes;
  const url = new URL(`https://${FORK_SOURCE_HOST}${FORK_SOURCE_PATH}`);
  url.searchParams.set("stream", sourceId);
  if (validOffset !== undefined) url.searchParams.set("until", validOffset);
  url.searchParams.set("tail", String(tail));
  url.searchParams.set("budget", String(budget));
  const request = new Request(url, { headers: { "streamsy-fork-source": FORK_SOURCE_MARKER } });

  return Effect.gen(function* () {
    const response = yield* Effect.tryPromise({
      try: () => Promise.resolve(namespace.get(namespace.idFromName(sourceName)).fetch(request)),
      catch: forkFault,
    });
    if (response.headers.get("streamsy-fork-source") !== FORK_SOURCE_MARKER)
      return yield* Effect.fail(forkFault("Missing fork source marker"));
    if (response.status === 404)
      return { record: Option.none(), messages: [], truncated: false } satisfies ForkSnapshot;
    if (response.status === 410) {
      const record = yield* decodeRecord(response, sourceId, true);
      return { record: Option.some(record), messages: [], truncated: false } satisfies ForkSnapshot;
    }
    if (response.status !== 200)
      return yield* Effect.fail(forkFault(`Fork source returned ${response.status}`));
    if (response.headers.get("content-type") !== FORK_SOURCE_CONTENT_TYPE)
      return yield* Effect.fail(forkFault("Invalid fork source content type"));
    yield* Effect.try({
      try: () => parseContentLength(response, host.copyOnForkMaxBytes),
      catch: forkFault,
    });
    const body = yield* Effect.tryPromise({
      try: () => readCapped(response, host.copyOnForkMaxBytes),
      catch: forkFault,
    });
    const messages = yield* Effect.try({ try: () => decodeFrames(body), catch: forkFault });
    const record = yield* decodeRecord(response, sourceId, false);
    return {
      record: Option.some(record),
      messages,
      truncated: response.headers.get("streamsy-frames-truncated") === FORK_SOURCE_MARKER,
    } satisfies ForkSnapshot;
  });
};

const placementName = (placement: Placement, path: string): string | undefined => {
  const candidate: unknown = placement.name(path);
  const name = String(candidate);
  return candidate === name && name.length > 0 ? name : undefined;
};

const invalidPlacement = (): CreateOutcome => ({
  status: "bad-request",
  nextOffset: "",
  contentType: "",
  errorMessage: "Invalid placement key",
});

const sourceMessages =
  (
    storage: typeof Storage.Service,
    sourceId: StreamId,
    snapshot: ForkSnapshot,
  ): StorageShape["messages"] =>
  (id, window) => {
    if (id !== sourceId) return storage.messages(id, window);
    const after = window.after;
    const until = window.until;
    const limit = window.limit === undefined ? undefined : Math.max(0, Math.trunc(window.limit));
    if (limit === 0) return Effect.succeed([]);
    const filtered = snapshot.messages.filter(
      (message) =>
        (after === undefined || message.offset > after) &&
        (until === undefined || message.offset <= until),
    );
    return Effect.succeed(limit === undefined ? filtered : filtered.slice(0, limit));
  };

export const sourceView = (
  storage: typeof Storage.Service,
  sourceId: StreamId,
  snapshot: ForkSnapshot,
): typeof Storage.Service =>
  Storage.of({
    ...storage,
    capabilities: { ...storage.capabilities, fork: "copy" },
    record: (id) => (id === sourceId ? Effect.succeed(snapshot.record) : storage.record(id)),
    messages: sourceMessages(storage, sourceId, snapshot),
  });

export const makeForkWriter = (host: ForkHost) =>
  Effect.gen(function* () {
    const storage = yield* Storage;
    const real = yield* StreamsWriter;
    const create = Effect.fn("Cloudflare.forkWriter.create")(function* (
      id: StreamId,
      options: CreateOptions = {},
    ): Effect.fn.Return<CreateOutcome, StorageFault> {
      if (options.forkedFrom === undefined) return yield* real.create(id, options);
      const sourceId = StreamId.make(options.forkedFrom);
      if (Option.isSome(yield* storage.record(sourceId))) return yield* real.create(id, options);

      const childName = placementName(host.placement, id);
      if (childName === undefined) return invalidPlacement();
      const sourceName = placementName(host.placement, sourceId);
      if (sourceName === undefined) return invalidPlacement();
      if (childName === sourceName) return yield* real.create(id, options);

      const existing = yield* Protocol.expireIfNeeded(storage, id);
      if (Option.isSome(existing)) return yield* real.create(id, options);
      if (host.namespace === undefined) return { status: "not-supported", feature: "fork" };

      const snapshot = yield* fetchForkSource(host, sourceName, sourceId, options);
      if (snapshot.truncated)
        return {
          status: "conflict",
          nextOffset: "",
          contentType: "",
          conflictReason: "fork-copy-limit",
          errorMessage: "Fork copy exceeds copyOnForkMaxBytes",
        };
      return yield* Protocol.create(sourceView(storage, sourceId, snapshot), id, options);
    });
    return StreamsWriter.of({ ...real, create });
  });
