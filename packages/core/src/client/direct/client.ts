import type { StreamProtocolFactory } from "../../types/protocol.ts";
import type {
  ClientFailure,
  StreamProtocolClient,
  StreamProtocolHandle,
  StreamsyProtocolClient,
} from "../types.ts";
import { DirectProtocolHandle } from "./handle.ts";
import { abortedFailure, clientClosedFailure, failureFromThrown } from "./results.ts";

/** Structural refinement test: is this client the exact-Streamsy direct adapter? */
export function hasStreamsyProtocol(
  client: StreamProtocolClient,
): client is StreamsyProtocolClient {
  return "streamsy" in client;
}

export function directProtocolClient(factory: StreamProtocolFactory): StreamsyProtocolClient {
  return new DirectProtocolClient(factory);
}

/**
 * Adapts a `StreamProtocolFactory` to the transport-neutral client seam. It
 * keeps a single client-level abort controller whose signal is threaded into
 * every operation and read session; `close()` aborts it and rejects new work.
 * There is no session registry: sessions observe the abort through their own
 * signal and end with `{ status: "cancelled" }`.
 */
export class DirectProtocolClient implements StreamsyProtocolClient {
  readonly streamsy: StreamProtocolFactory;
  private readonly controller = new AbortController();
  private disposed = false;

  constructor(factory: StreamProtocolFactory) {
    this.streamsy = factory;
  }

  stream(streamId: string): StreamProtocolHandle {
    return new DirectProtocolHandle(this, streamId);
  }

  async close(reason?: unknown): Promise<void> {
    if (this.disposed) return;
    this.disposed = true;
    this.controller.abort(reason);
  }

  /**
   * Runs one operation under the combined client/caller signal, converting a
   * disposed client, an already-aborted signal, and unexpected throws into
   * {@link ClientFailure} members. Domain results flow through untouched.
   */
  async run<R>(
    signal: AbortSignal | undefined,
    work: (signal: AbortSignal) => Promise<R>,
  ): Promise<R | ClientFailure> {
    if (this.disposed) return clientClosedFailure();
    const combined = combineSignals(this.controller.signal, signal);
    if (combined.aborted) return abortedFailure(combined.reason);
    try {
      return await work(combined);
    } catch (error) {
      if (combined.aborted) return abortedFailure(combined.reason ?? error);
      return failureFromThrown(error);
    }
  }
}

export function combineSignals(...signals: (AbortSignal | undefined)[]): AbortSignal {
  const present = signals.filter((signal): signal is AbortSignal => signal !== undefined);
  if (present.length === 0) return new AbortController().signal;
  return present.length === 1 ? present[0]! : AbortSignal.any(present);
}
