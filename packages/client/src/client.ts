import { BackoffDefaults, createFetchWithBackoff, DurableStream } from "@durable-streams/client";
import type {
  BackoffOptions,
  HeadersRecord,
  ParamsRecord,
  SSEResilienceOptions,
  StreamErrorHandler,
} from "@durable-streams/client";
import type { ClientFailure, StreamProtocolClient, StreamProtocolHandle } from "@streamsy/core";
import { abortedFailure, clientClosedFailure } from "./errors.ts";
import { wrapFetch } from "./fetch-fn.ts";
import { OfficialProtocolHandle } from "./handle.ts";

export interface OfficialProtocolClientOptions {
  /** Maps an opaque Streamsy id to the full endpoint URL. */
  urlFor(streamId: string): string | URL;
  headers?: HeadersRecord;
  params?: ParamsRecord;
  fetch?: typeof globalThis.fetch;
  signal?: AbortSignal;
  backoffOptions?: BackoffOptions;
  onError?: StreamErrorHandler;
  sseResilience?: SSEResilienceOptions;
  /** Retained for upstream handle compatibility; rich append is always one request per call. */
  batching?: boolean;
  warnOnHttp?: boolean;
}

export function officialProtocolClient(
  options: OfficialProtocolClientOptions,
): StreamProtocolClient {
  return new OfficialProtocolClient(options);
}

/**
 * Adapts the official `@durable-streams/client` to the transport-neutral client
 * seam. Reads and metadata delegate to official handles. Append uses a narrow
 * non-batching request path because the pinned official append API discards the
 * response offset and producer/CAS outcomes. Retry behavior still comes from
 * the official client's fetch utility.
 */
export class OfficialProtocolClient implements StreamProtocolClient {
  private readonly controller = new AbortController();
  private readonly baseSignal: AbortSignal;
  private readonly appendFetch: typeof globalThis.fetch;
  private disposed = false;

  constructor(readonly options: OfficialProtocolClientOptions) {
    this.baseSignal = combineSignals(options.signal, this.controller.signal);
    const baseFetch = options.fetch ?? wrapFetch(globalThis.fetch);
    this.appendFetch = createFetchWithBackoff(baseFetch, options.backoffOptions ?? BackoffDefaults);
  }

  stream(streamId: string): StreamProtocolHandle {
    return new OfficialProtocolHandle(this, streamId, this.options.urlFor(streamId));
  }

  async close(reason?: unknown): Promise<void> {
    if (this.disposed) return;
    this.disposed = true;
    this.controller.abort(reason);
  }

  get signal(): AbortSignal {
    return this.baseSignal;
  }

  /**
   * Runs one operation under the combined client/caller signal. A disposed
   * client and an already-aborted signal short-circuit to failures; a thrown
   * error is mapped by the operation-specific `onError` (which decides whether
   * it is a domain result member or a generic failure).
   */
  async run<R>(
    signal: AbortSignal | undefined,
    work: (signal: AbortSignal) => Promise<R>,
    onError: (error: unknown, signal: AbortSignal) => R,
  ): Promise<R | ClientFailure> {
    if (this.disposed) return clientClosedFailure();
    const combined = combineSignals(this.baseSignal, signal);
    if (combined.aborted) return abortedFailure(combined.reason);
    try {
      return await work(combined);
    } catch (error) {
      if (combined.aborted) return abortedFailure(combined.reason ?? error);
      return onError(error, combined);
    }
  }

  durableStream(url: string | URL, signal: AbortSignal): DurableStream {
    return new DurableStream({
      url,
      headers: this.options.headers,
      params: this.options.params,
      fetch: this.options.fetch,
      signal,
      backoffOptions: this.options.backoffOptions,
      onError: this.options.onError,
      batching: this.options.batching ?? false,
      warnOnHttp: this.options.warnOnHttp,
    });
  }

  async fetchAppend(url: string | URL, init: RequestInit): Promise<Response> {
    const fetchUrl = new URL(url);
    for (const [key, value] of Object.entries(this.options.params ?? {})) {
      if (value === undefined) continue;
      fetchUrl.searchParams.set(key, typeof value === "function" ? await value() : value);
    }
    return this.appendFetch(fetchUrl, init);
  }

  async appendHeaders(): Promise<Headers> {
    const headers = new Headers();
    for (const [key, value] of Object.entries(this.options.headers ?? {})) {
      headers.set(key, typeof value === "function" ? await value() : value);
    }
    return headers;
  }
}

export function combineSignals(...signals: (AbortSignal | undefined)[]): AbortSignal {
  const present = signals.filter((signal): signal is AbortSignal => signal !== undefined);
  if (present.length === 0) return new AbortController().signal;
  return present.length === 1 ? present[0]! : AbortSignal.any(present);
}
