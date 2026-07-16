import { DurableStream } from "@durable-streams/client";
import type {
  BackoffOptions,
  HeadersRecord,
  ParamsRecord,
  SSEResilienceOptions,
  StreamErrorHandler,
} from "@durable-streams/client";
import type { ClientFailure, StreamProtocolClient, StreamProtocolHandle } from "@streamsy/core";
import { abortedFailure, clientClosedFailure } from "./errors.ts";
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
 * seam. Its only real work is constructing official handles/reads and mapping
 * official thrown errors into result members (see `errors.ts`). It implements no
 * HTTP, SSE, retries, or wire decoding of its own.
 */
export class OfficialProtocolClient implements StreamProtocolClient {
  private readonly controller = new AbortController();
  private readonly baseSignal: AbortSignal;
  private disposed = false;

  constructor(readonly options: OfficialProtocolClientOptions) {
    this.baseSignal = combineSignals(options.signal, this.controller.signal);
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
}

export function combineSignals(...signals: (AbortSignal | undefined)[]): AbortSignal {
  const present = signals.filter((signal): signal is AbortSignal => signal !== undefined);
  if (present.length === 0) return new AbortController().signal;
  return present.length === 1 ? present[0]! : AbortSignal.any(present);
}
