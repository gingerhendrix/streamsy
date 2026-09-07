import { Effect, Option } from "effect";
import {
  Protocol,
  Storage,
  Offset,
  StreamId,
  StreamRecord,
  type StorageFault,
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
type ForkSourceWindow = { after?: Offset; until?: Offset; limit: number };

export interface ForkSourceOptions {
  readonly pathPrefix?: string;
  readonly copyOnForkMaxBytes: number;
}

const securityHeaders = {
  "x-content-type-options": "nosniff",
  "cross-origin-resource-policy": "cross-origin",
};

const response = (
  body: string | ArrayBuffer | null,
  status: number,
  headers: HeadersInit = {},
): Response => {
  const responseHeaders = new Headers(securityHeaders);
  responseHeaders.set("cache-control", "no-store");
  if (headers instanceof Headers) {
    headers.forEach((value, name) => responseHeaders.set(name, value));
  } else if (Array.isArray(headers)) {
    for (const [name, value] of headers) responseHeaders.set(name, value);
  } else {
    for (const [name, value] of Object.entries(headers)) responseHeaders.set(name, value);
  }
  return new Response(body, { status, headers: responseHeaders });
};

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

type ParsedQuery =
  | { readonly ok: false; readonly response: Response }
  | {
      readonly ok: true;
      readonly stream: string;
      readonly until?: string;
      readonly tail: number;
      readonly budget: number;
    };

const recordHeaders = (record: StreamRecord): Headers => {
  const headers = new Headers({
    "streamsy-fork-source": FORK_SOURCE_MARKER,
    "streamsy-source-content-type": record.config.contentType,
    "streamsy-source-next-offset": record.currentOffset,
    "streamsy-source-created-at": String(record.config.createdAt),
  });
  if (record.config.ttlSeconds !== undefined)
    headers.set("streamsy-source-ttl", String(record.config.ttlSeconds));
  if (record.config.expiresAt !== undefined)
    headers.set("streamsy-source-expires-at", record.config.expiresAt);
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
      const window: ForkSourceWindow = { limit };
      if (cursor !== undefined) window.after = cursor;
      if (until !== undefined) window.until = until;
      const page = yield* storage.messages(id, window);
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

const readQuery = (options: ForkSourceOptions, url: URL): ParsedQuery => {
  const stream = url.searchParams.get("stream");
  if (stream === null || stream.length === 0) return { ok: false, response: invalidPath(options) };
  const rawUntil = url.searchParams.get("until");
  if (rawUntil !== null && !OFFSET_PATTERN.test(rawUntil))
    return { ok: false, response: response("Invalid fork-source until", 400) };
  const tail = parseUnsigned(url.searchParams.get("tail"), MAX_TAIL);
  if (tail === undefined) return { ok: false, response: response("Invalid fork-source tail", 400) };
  const budget = parseUnsigned(url.searchParams.get("budget"));
  if (budget === undefined)
    return { ok: false, response: response("Invalid fork-source budget", 400) };
  return {
    ok: true,
    stream,
    until: rawUntil === null ? undefined : rawUntil,
    tail,
    budget,
  };
};

const runExport = (
  options: ForkSourceOptions,
): Effect.Effect<Response, StorageFault, HttpServerRequest.HttpServerRequest | Storage> =>
  Effect.gen(function* (): Effect.gen.Return<
    Response,
    StorageFault,
    HttpServerRequest.HttpServerRequest | Storage
  > {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const storage = yield* Storage;
    const parsed = readQuery(options, new URL(request.originalUrl));
    if (!parsed.ok) return parsed.response;

    const id = StreamId.make(parsed.stream);
    const current = yield* Protocol.expireIfNeeded(storage, id);
    if (Option.isNone(current))
      return response("Stream not found", 404, { "streamsy-fork-source": FORK_SOURCE_MARKER });
    const record = current.value;
    if (record.lifecycle.softDeleted)
      return response("Stream is soft-deleted", 410, recordHeaders(record));

    const until = parsed.until === undefined ? record.currentOffset : Offset.make(parsed.until);
    if (parsed.until !== undefined && until > record.currentOffset) {
      const headers = recordHeaders(record);
      headers.set("streamsy-frames-omitted", "until-beyond-tail");
      headers.set("content-type", FORK_SOURCE_CONTENT_TYPE);
      headers.set("content-length", "0");
      return response(null, 200, headers);
    }

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
    const encoded = encodeFrames(messages);
    const body = new ArrayBuffer(encoded.byteLength);
    new Uint8Array(body).set(encoded);
    const headers = recordHeaders(record);
    headers.set("content-type", FORK_SOURCE_CONTENT_TYPE);
    headers.set("content-length", String(encoded.byteLength));
    if (truncated) headers.set("streamsy-frames-truncated", "1");
    return response(body, 200, headers);
  });

export const forkSource = (options: ForkSourceOptions) => runExport(options);
