import { Effect, Option } from "effect";
import {
  Protocol,
  Storage,
  Offset,
  StreamId,
  StreamRecord,
  type StoredMessage,
} from "@streamsy/core";
import { HttpServerRequest } from "effect/unstable/http";
import { StreamPathService } from "@streamsy/core/http";
import { encodeFrames } from "./fork-frames.ts";

export const FORK_SOURCE_HOST = "streamsy.internal";
export const FORK_SOURCE_PATH = "/fork-source";
export const FORK_SOURCE_CONTENT_TYPE = "application/vnd.streamsy.frames";
export const FORK_SOURCE_MARKER = "1";
const PAGE_SIZE = 16;
const MAX_TAIL = 10_000;
const OFFSET_PATTERN = /^\d{16}_\d{16}$/;

export interface ForkSourceOptions {
  readonly pathPrefix?: string;
  readonly copyOnForkMaxBytes: number;
}

const securityHeaders = {
  "x-content-type-options": "nosniff",
  "cross-origin-resource-policy": "cross-origin",
};

const response = (body: BodyInit | null, status: number, headers: HeadersInit = {}): Response =>
  new Response(body, {
    status,
    headers: { ...securityHeaders, "cache-control": "no-store", ...headers },
  });

const invalidPath = (options: ForkSourceOptions): Response =>
  response(
    `Stream path required: ${new StreamPathService(options.pathPrefix ?? "/").requiredPathPattern()}`,
    400,
  );

const parseUnsigned = (value: string | null, max?: number): number | undefined => {
  if (value === null || !/^(0|[1-9]\d*)$/.test(value)) return undefined;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && (max === undefined || parsed <= max) ? parsed : undefined;
};

const recordHeaders = (record: StreamRecord): HeadersInit => {
  const headers: Record<string, string> = {
    "streamsy-fork-source": FORK_SOURCE_MARKER,
    "streamsy-source-content-type": record.config.contentType,
    "streamsy-source-next-offset": record.currentOffset,
    "streamsy-source-created-at": String(record.config.createdAt),
  };
  if (record.config.ttlSeconds !== undefined)
    headers["streamsy-source-ttl"] = String(record.config.ttlSeconds);
  if (record.config.expiresAt !== undefined)
    headers["streamsy-source-expires-at"] = record.config.expiresAt;
  return headers;
};

const readPages = (
  storage: typeof Storage.Service,
  id: StreamId,
  after: Offset | undefined,
  until: Offset | undefined,
  maxMessages: number | undefined,
  budget: number,
) =>
  Effect.gen(function* () {
    const messages: Array<StoredMessage> = [];
    let cursor = after;
    let remaining = maxMessages;
    let used = 0;
    while (remaining === undefined || remaining > 0) {
      const limit = Math.min(PAGE_SIZE, remaining ?? PAGE_SIZE);
      const page = yield* storage.messages(id, {
        ...(cursor === undefined ? {} : { after: cursor }),
        ...(until === undefined ? {} : { until }),
        limit,
      });
      if (page.length === 0) break;
      for (const message of page) {
        const frameBytes = 45 + message.data.byteLength;
        if (used + frameBytes > budget) return { messages, truncated: true };
        messages.push(message);
        used += frameBytes;
        cursor = message.offset;
        if (remaining !== undefined) remaining -= 1;
      }
      if (page.length < limit) break;
    }
    return { messages, truncated: false };
  });

const readQuery = (options: ForkSourceOptions, url: URL) => {
  const stream = url.searchParams.get("stream");
  if (stream === null || stream.length === 0) return { response: invalidPath(options) } as const;
  const rawUntil = url.searchParams.get("until");
  if (rawUntil !== null && !OFFSET_PATTERN.test(rawUntil))
    return { response: response("Invalid fork-source until", 400) } as const;
  const tail = parseUnsigned(url.searchParams.get("tail"), MAX_TAIL);
  if (tail === undefined) return { response: response("Invalid fork-source tail", 400) } as const;
  const budget = parseUnsigned(url.searchParams.get("budget"));
  if (budget === undefined)
    return { response: response("Invalid fork-source budget", 400) } as const;
  return {
    stream,
    until: rawUntil === null ? undefined : rawUntil,
    tail,
    budget,
  } as const;
};

const runExport = (options: ForkSourceOptions) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const storage = yield* Storage;
    const parsed = readQuery(options, new URL(request.originalUrl));
    if ("response" in parsed) return parsed.response;

    const id = StreamId.make(parsed.stream);
    const current = yield* Protocol.expireIfNeeded(storage, id);
    if (Option.isNone(current))
      return response("Stream not found", 404, { "streamsy-fork-source": FORK_SOURCE_MARKER });
    const record = current.value;
    if (record.lifecycle.softDeleted)
      return response("Stream is soft-deleted", 410, recordHeaders(record));

    const until = parsed.until === undefined ? record.currentOffset : Offset.make(parsed.until);
    if (parsed.until !== undefined && until > record.currentOffset)
      return response(null, 200, {
        ...recordHeaders(record),
        "streamsy-frames-omitted": "until-beyond-tail",
        "content-type": FORK_SOURCE_CONTENT_TYPE,
        "content-length": "0",
      });

    const budget = Math.min(parsed.budget, options.copyOnForkMaxBytes);
    let messages: ReadonlyArray<StoredMessage> = [];
    let truncated = false;
    if (budget > 0) {
      const prefix = yield* readPages(storage, id, undefined, until, undefined, budget);
      messages = prefix.messages;
      truncated = prefix.truncated;
      if (!truncated && parsed.tail > 0) {
        const tail = yield* readPages(
          storage,
          id,
          until,
          undefined,
          parsed.tail,
          budget - messages.reduce((sum, message) => sum + 45 + message.data.byteLength, 0),
        );
        messages = [...messages, ...tail.messages];
        truncated = tail.truncated;
      }
    }
    const body = encodeFrames(messages);
    return response(body, 200, {
      ...recordHeaders(record),
      "content-type": FORK_SOURCE_CONTENT_TYPE,
      "content-length": String(body.byteLength),
      ...(truncated ? { "streamsy-frames-truncated": "1" } : {}),
    });
  });

export const forkSource = (options: ForkSourceOptions) => runExport(options);
