/** Live-read orchestration for one storage-bound stream. */

import type { ReadLiveOptions, ReadLiveResult } from "../types/protocol.ts";
import type {
  AwaitChangeOptions,
  AwaitChangeResult,
  Clock,
  Offset,
  StoredMessage,
  StreamChangeSnapshot,
  StreamRecord,
} from "../types/storage.ts";
import { compareOffsets, isValidOffset, type OffsetGenerator } from "./helpers/offset-generator.ts";
import { generateCursor } from "./helpers/cursor-generator.ts";
import { raceAbortAwaitChange } from "./helpers/race-abort.ts";

/** Narrow view of the storage stream the live-read service depends on. */
export interface LiveReadStore {
  getRecord(): Promise<StreamRecord | null>;
  awaitChange(options: AwaitChangeOptions): Promise<AwaitChangeResult>;
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
  offsets: OffsetGenerator;
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

    // Normalize the reader's parked position to a canonical offset before the
    // offset-comparison wait path. The HTTP layer passes the start sentinel `"0"`
    // (and other non-canonical forms), which sorts *below* `initialOffset` in a raw
    // string compare; left unnormalized, `awaitChange` and the visibility guard
    // would see a phantom "tail advanced" on an empty stream. `listMessages`
    // filtering (`offset > after`) is unaffected since every real offset exceeds
    // both `"0"` and `initialOffset`.
    const fromOffset = isValidOffset(this.deps.offsets, options.offset)
      ? options.offset
      : this.deps.offsets.initialOffset;

    const immediate = await this.deps.readOwn(fromOffset);
    if (immediate.messages.length > 0)
      return {
        status: "ok",
        ...immediate,
        upToDate: true,
        cursor: generateCursor(this.deps.clock, options.cursor),
      };

    // Control flow has already returned for the closed, soft-deleted, fork-catchup
    // and has-messages cases, so at the wait point `record` is known open and
    // live: both observed flags are false. Passing them lets `awaitChange` report
    // a change the moment the stream transitions to closed / soft-deleted / purged.
    const observed: StreamChangeSnapshot = {
      present: true,
      currentOffset: record.currentOffset,
      closed: false,
      softDeleted: false,
    };
    const wait = await raceAbortAwaitChange(
      this.deps.store.awaitChange({
        fromOffset,
        observedClosed: observed.closed,
        observedSoftDeleted: observed.softDeleted,
        timeoutMs: this.deps.longPollTimeoutMs,
      }),
      observed,
      options.signal,
    );
    const latest = await this.deps.store.getRecord();
    if (!latest)
      return { status: "not-found", messages: [], nextOffset: "", upToDate: false, cursor: "" };
    if (latest.lifecycle.softDeleted)
      return { status: "gone", messages: [], nextOffset: "", upToDate: false, cursor: "" };
    const r = await this.deps.readOwn(fromOffset);
    const reachedTail = r.nextOffset === latest.currentOffset;
    const hasMessages = r.messages.length > 0;
    return {
      // Caller-local abort surfaces as a timeout-shaped result, so the old
      // `timeout | aborted` mapping collapses to a single `timeout` check —
      // behaviorally identical to the previous edge-triggered path.
      status: hasMessages ? "ok" : wait.status === "timeout" ? "timeout" : "ok",
      messages: r.messages,
      nextOffset: r.nextOffset,
      upToDate: true,
      cursor: generateCursor(this.deps.clock, options.cursor),
      closed: latest.lifecycle.closed === true && reachedTail,
    };
  }
}
