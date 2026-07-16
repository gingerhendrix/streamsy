/**
 * Isolated, long-lived accommodations for the official `@durable-streams/client`.
 *
 * These are not bugs to be removed on the next patch: the upstream client is
 * independently versioned and uncontrolled, so these adjustments may live
 * indefinitely. Keeping them in one module means the adapter proper reads as an
 * obvious thin mapping.
 *
 * 1. Reads go through the top-level `stream()` function rather than
 *    `DurableStream.stream()` so `sseResilience` and per-call `offset`/`live`
 *    plumbing are honored the way the streaming entry point supports them.
 * 2. A terminal `streamClosed` transition with no trailing data is not
 *    surfaced by the official subscriber callbacks, so we synthesize an empty
 *    EOF batch to preserve the real close semantic (see
 *    {@link deliverCloseOnlyEof}).
 * 3. `DurableStream.create()` takes no per-call signal, so create uses a fresh
 *    handle constructed with the operation signal (see the client's
 *    `durableStream` helper and its use in the handle).
 */

import { stream as readOfficialStream } from "@durable-streams/client";
import type {
  BackoffOptions,
  HeadersRecord,
  ParamsRecord,
  SSEResilienceOptions,
  StreamErrorHandler,
  StreamResponse,
} from "@durable-streams/client";
import type { ClientLiveMode, JsonValue } from "@streamsy/core";
import type { ClientReadSession } from "@streamsy/core";

export interface OfficialReadConfig {
  url: string | URL;
  headers?: HeadersRecord;
  params?: ParamsRecord;
  fetch?: typeof globalThis.fetch;
  signal: AbortSignal;
  backoffOptions?: BackoffOptions;
  offset?: string;
  live: ClientLiveMode;
  onError?: StreamErrorHandler;
  sseResilience?: SSEResilienceOptions;
  warnOnHttp?: boolean;
}

export function openReadStream<T = unknown>(
  config: OfficialReadConfig,
): Promise<StreamResponse<T>> {
  return readOfficialStream<T>({
    url: config.url,
    headers: config.headers,
    params: config.params,
    fetch: config.fetch,
    signal: config.signal,
    backoffOptions: config.backoffOptions,
    offset: config.offset,
    live: config.live,
    onError: config.onError,
    sseResilience: config.sseResilience,
    warnOnHttp: config.warnOnHttp,
  });
}

/**
 * Delivers a synthesized empty EOF batch when the response reached a terminal
 * `streamClosed` state that the subscriber callbacks did not surface (a
 * close-only transition with no trailing payload). Returns whether a batch was
 * delivered.
 */
export async function deliverCloseOnlyEof<T extends JsonValue>(
  response: StreamResponse<T>,
  session: ClientReadSession<T>,
  mediaType: string | undefined,
): Promise<boolean> {
  if (!(response.streamClosed && !session.streamClosed && response.offset === session.offset)) {
    return false;
  }
  const meta = {
    offset: response.offset,
    cursor: response.cursor,
    upToDate: response.upToDate,
    streamClosed: true,
  };
  if (mediaType === "application/json") {
    await session.deliver({ kind: "json", items: [], ...meta });
  } else if (mediaType?.startsWith("text/")) {
    await session.deliver({ kind: "text", text: "", ...meta });
  } else {
    await session.deliver({ kind: "bytes", data: new Uint8Array(), ...meta });
  }
  return true;
}
