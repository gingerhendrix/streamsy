/**
 * Live-read orchestration for the durable streams protocol.
 *
 * Responsibilities:
 *
 *   1. record lookup, lifecycle gate (not-found / soft-deleted) with empty cursor
 *   2. closed-stream replay via `readChain` and `closed`/`upToDate` shaping
 *   3. fork-source upstream-tail replay via `readChain` when the requested
 *      offset is below the fork offset
 *   4. live-read touch (current implementation is a no-op for TTL extension)
 *   5. immediate own-message read via `readOwn`
 *   6. wait/timeout/abort via `store.waitForEvent` (with a setTimeout fallback
 *      when the adapter does not implement waiters)
 *   7. final metadata recheck after wake and final own-message read shaping
 *
 * Used by `StreamProtocol.readLive`. The lazy-expiry prelude
 * (`expireIfNeeded`) stays in `StreamProtocol`. The shared chain/own helpers
 * (`readChain`, `readOwn`) and the touch helper are injected callbacks because
 * they are also used by non-live reads and create/fork/append flows.
 */

import type { ReadLiveOptions, ReadLiveResult } from "../types/protocol.ts";
import type {
  Clock,
  Offset,
  StoredMessage,
  StreamId,
  StreamRecord,
  StreamStoreAdapter,
} from "../types/storage.ts";
import { compareOffsets } from "./helpers/offset-generator.ts";
import { generateCursor } from "./helpers/cursor-generator.ts";

export type LiveReadChain = (
  streamId: StreamId,
  record: StreamRecord,
  afterOffset?: string,
) => Promise<StoredMessage[]>;

export type LiveReadOwn = (
  streamId: StreamId,
  after?: Offset,
) => Promise<{ messages: StoredMessage[]; nextOffset: string }>;

export type LiveReadTouch = (streamId: StreamId, record: StreamRecord) => Promise<void>;

export interface LiveReadDeps {
  readChain: LiveReadChain;
  readOwn: LiveReadOwn;
  touch: LiveReadTouch;
}

export class LiveReadService {
  constructor(
    private store: StreamStoreAdapter,
    private clock: Clock,
    private longPollTimeoutMs: number,
    private deps: LiveReadDeps,
  ) {}

  async execute(streamId: StreamId, options: ReadLiveOptions): Promise<ReadLiveResult> {
    const record = await this.store.get(streamId);
    if (!record)
      return { status: "not-found", messages: [], nextOffset: "", upToDate: false, cursor: "" };
    if (record.lifecycle.softDeleted)
      return { status: "gone", messages: [], nextOffset: "", upToDate: false, cursor: "" };

    if (record.lifecycle.closed) {
      const messages = await this.deps.readChain(streamId, record, options.offset);
      const lastOffset =
        messages.length > 0 ? messages[messages.length - 1]!.offset : record.currentOffset;
      const nextOffset =
        compareOffsets(lastOffset, record.currentOffset) > 0 ? lastOffset : record.currentOffset;
      return {
        status: messages.length > 0 ? "ok" : "timeout",
        messages,
        nextOffset,
        upToDate: true,
        cursor: generateCursor(this.clock, options.cursor),
        closed: nextOffset === record.currentOffset,
      };
    }

    if (
      record.lifecycle.forkedFrom &&
      record.lifecycle.forkOffset &&
      compareOffsets(options.offset, record.lifecycle.forkOffset) < 0
    ) {
      const messages = await this.deps.readChain(streamId, record, options.offset);
      const lastOffset =
        messages.length > 0 ? messages[messages.length - 1]!.offset : record.currentOffset;
      return {
        status: "ok",
        messages,
        nextOffset:
          compareOffsets(lastOffset, record.currentOffset) > 0 ? lastOffset : record.currentOffset,
        upToDate: true,
        cursor: generateCursor(this.clock, options.cursor),
      };
    }

    await this.deps.touch(streamId, record);
    const immediate = await this.deps.readOwn(streamId, options.offset);
    if (immediate.messages.length > 0)
      return {
        status: "ok",
        ...immediate,
        upToDate: true,
        cursor: generateCursor(this.clock, options.cursor),
      };

    const wait = this.store.waitForEvent
      ? await this.store.waitForEvent(streamId, {
          timeoutMs: this.longPollTimeoutMs,
          signal: options.signal,
        })
      : await new Promise<{ status: "timeout" }>((resolve) =>
          setTimeout(() => resolve({ status: "timeout" }), this.longPollTimeoutMs),
        );
    const latest = await this.store.get(streamId);
    if (!latest)
      return { status: "not-found", messages: [], nextOffset: "", upToDate: false, cursor: "" };
    if (latest.lifecycle.softDeleted)
      return { status: "gone", messages: [], nextOffset: "", upToDate: false, cursor: "" };
    const r = await this.deps.readOwn(streamId, options.offset);
    const reachedTail = r.nextOffset === latest.currentOffset;
    const hasMessages = r.messages.length > 0;
    return {
      status: hasMessages
        ? "ok"
        : wait.status === "timeout" || wait.status === "aborted"
          ? "timeout"
          : "ok",
      messages: r.messages,
      nextOffset: r.nextOffset,
      upToDate: true,
      cursor: generateCursor(this.clock, options.cursor),
      closed: latest.lifecycle.closed === true && reachedTail,
    };
  }
}
