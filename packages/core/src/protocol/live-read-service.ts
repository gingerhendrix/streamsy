/** Live-read orchestration for one storage-bound stream. */

import type { ReadLiveOptions, ReadLiveResult } from "../types/protocol.ts";
import type { Stream } from "../types/factory.ts";
import type { Clock, Offset, StoredMessage, StreamId, StreamRecord } from "../types/storage.ts";
import { notSupported } from "../types/factory.ts";
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
    private stream: Stream,
    private clock: Clock,
    private longPollTimeoutMs: number,
    private deps: LiveReadDeps,
  ) {}

  async execute(streamId: StreamId, options: ReadLiveOptions): Promise<ReadLiveResult> {
    const record = await this.stream.getRecord();
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

    if (!this.stream.events)
      return notSupported("live-read", "The active storage factory has no live-read event hub");
    const wait = await this.stream.events.waitForEvent({
      timeoutMs: this.longPollTimeoutMs,
      signal: options.signal,
    });
    const latest = await this.stream.getRecord();
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
