/**
 * Owns the adapter root directory and a per-id cache of `FsStream` handles.
 *
 * Caching keeps the in-process notifier and expiry timer stable across concurrent
 * lookups for the same id within one process. The cache is purely a runtime
 * optimization — the durable files under `root` remain the source of truth, so a
 * fresh process (the serverless case) reconstructs identical behaviour with an
 * empty cache.
 */
import { mkdirSync } from "node:fs";
import type { StreamId } from "@streamsy/core";
import { FsStream, type FsStreamOptions } from "./stream.ts";
import type { LockOptions } from "./lock.ts";

export type FsExpiryHandler = (streamId: StreamId) => Promise<void> | void;

export interface FsStreamStateOptions {
  /** Root directory holding one subdirectory per stream. Created lazily. */
  root: string;
  /** Lock acquisition tuning (timeout / stale TTL / retry backoff). */
  lock?: LockOptions;
  /** Add `fs.watch` as a cross-process wake source for `awaitChange`.
   * Default `false` (in-process notifier + capped-park polling). */
  watch?: boolean;
  /** Upper bound on a single parked `awaitChange` wait (`parkCapMs`). Default 1000ms. */
  watchPollMs?: number;
  onScheduledExpiry?: FsExpiryHandler;
}

export class FsStreamState {
  readonly root: string;
  private readonly streams = new Map<StreamId, FsStream>();
  private readonly streamOptions: FsStreamOptions;
  private readonly onScheduledExpiry?: FsExpiryHandler;

  constructor(options: FsStreamStateOptions) {
    this.root = options.root;
    this.onScheduledExpiry = options.onScheduledExpiry;
    this.streamOptions = {
      lock: options.lock,
      watch: options.watch,
      watchPollMs: options.watchPollMs,
    };
    mkdirSync(this.root, { recursive: true });
  }

  getStream(id: StreamId): FsStream {
    let stream = this.streams.get(id);
    if (!stream) {
      stream = new FsStream(
        this.root,
        id,
        () => this.streams.delete(id),
        this.streamOptions,
        () => this.onScheduledExpiry?.(id),
      );
      this.streams.set(id, stream);
    }
    return stream;
  }

  getExistingStream(id: StreamId): FsStream | undefined {
    return this.streams.get(id);
  }

  deleteFromCache(id: StreamId): void {
    this.streams.delete(id);
  }
}
