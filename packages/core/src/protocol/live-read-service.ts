/** Live-read orchestration for one storage-bound stream. */

import type { ReadLiveOptions, ReadLiveResult } from "../types/protocol.ts";
import type { Clock, Offset, StoredMessage, StreamRecord } from "../types/storage.ts";
import { compareOffsets } from "./helpers/offset-generator.ts";
import { generateCursor } from "./helpers/cursor-generator.ts";

/** Narrow view of the storage stream the live-read service depends on. */
export interface LiveReadStore {
  getRecord(): Promise<StreamRecord | null>;
  waitForEvent(
    options: import("../types/storage.ts").WaitForEventOptions,
  ): Promise<import("../types/storage.ts").WaitForEventResult>;
}

export type LiveReadChain = (
  record: StreamRecord,
  afterOffset?: string,
) => Promise<StoredMessage[]>;

export type LiveReadOwn = (
  after?: Offset,
) => Promise<{ messages: StoredMessage[]; nextOffset: string }>;

export interface LiveReadDeps {
  readChain: LiveReadChain;
  readOwn: LiveReadOwn;
}

export interface LiveReadServiceDeps extends LiveReadDeps {
  store: LiveReadStore;
  clock: Clock;
  longPollTimeoutMs: number;
}

export class LiveReadService {
  constructor(private deps: LiveReadServiceDeps) {}

  async execute(record: StreamRecord | null, options: ReadLiveOptions): Promise<ReadLiveResult> {
    if (!record)
      return { status: "not-found", messages: [], nextOffset: "", upToDate: false, cursor: "" };
    if (record.lifecycle.softDeleted)
      return { status: "gone", messages: [], nextOffset: "", upToDate: false, cursor: "" };

    if (record.lifecycle.closed) {
      const messages = await this.deps.readChain(record, options.offset);
      const lastOffset =
        messages.length > 0 ? messages[messages.length - 1]!.offset : record.currentOffset;
      const nextOffset =
        compareOffsets(lastOffset, record.currentOffset) > 0 ? lastOffset : record.currentOffset;
      return {
        status: messages.length > 0 ? "ok" : "timeout",
        messages,
        nextOffset,
        upToDate: true,
        cursor: generateCursor(this.deps.clock, options.cursor),
        closed: nextOffset === record.currentOffset,
      };
    }

    if (
      record.lifecycle.forkedFrom &&
      record.lifecycle.forkOffset &&
      compareOffsets(options.offset, record.lifecycle.forkOffset) < 0
    ) {
      const messages = await this.deps.readChain(record, options.offset);
      const lastOffset =
        messages.length > 0 ? messages[messages.length - 1]!.offset : record.currentOffset;
      return {
        status: "ok",
        messages,
        nextOffset:
          compareOffsets(lastOffset, record.currentOffset) > 0 ? lastOffset : record.currentOffset,
        upToDate: true,
        cursor: generateCursor(this.deps.clock, options.cursor),
      };
    }

    const immediate = await this.deps.readOwn(options.offset);
    if (immediate.messages.length > 0)
      return {
        status: "ok",
        ...immediate,
        upToDate: true,
        cursor: generateCursor(this.deps.clock, options.cursor),
      };

    const wait = await this.deps.store.waitForEvent({
      timeoutMs: this.deps.longPollTimeoutMs,
      signal: options.signal,
    });
    const latest = await this.deps.store.getRecord();
    if (!latest)
      return { status: "not-found", messages: [], nextOffset: "", upToDate: false, cursor: "" };
    if (latest.lifecycle.softDeleted)
      return { status: "gone", messages: [], nextOffset: "", upToDate: false, cursor: "" };
    const r = await this.deps.readOwn(options.offset);
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
      cursor: generateCursor(this.deps.clock, options.cursor),
      closed: latest.lifecycle.closed === true && reachedTail,
    };
  }
}
