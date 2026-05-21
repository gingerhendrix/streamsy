/**
 * Non-live read orchestration for the durable streams protocol.
 *
 * Responsibilities:
 *
 *   1. record lookup, lifecycle gate (not-found / soft-deleted)
 *   2. offset normalization ("" or "-1" -> undefined)
 *   3. read-chain delegation (own messages and any inherited fork upstream)
 *   4. nextOffset / upToDate / closed shaping
 *
 * Used by `StreamProtocol.read`. The lazy-expiry prelude (`expireIfNeeded`)
 * stays in `StreamProtocol`, and the recursive `readChain` helper is injected
 * because it is shared with live-read closed-stream replay and fork-source
 * upstream-tail handling.
 */

import type { ReadOptions, ReadResult } from "../types/protocol.ts";
import type {
  StoredMessage,
  StreamId,
  StreamRecord,
  StreamStoreAdapter,
} from "../types/storage.ts";
import { compareOffsets } from "./helpers/offset-generator.ts";

export type ReadChain = (
  streamId: StreamId,
  record: StreamRecord,
  afterOffset?: string,
) => Promise<StoredMessage[]>;

export function normalizeReadOffset(offset?: string): string | undefined {
  return !offset || offset === "-1" ? undefined : offset;
}

export class ReadService {
  constructor(
    private store: StreamStoreAdapter,
    private readChain: ReadChain,
  ) {}

  async execute(streamId: StreamId, options: ReadOptions): Promise<ReadResult> {
    const record = await this.store.get(streamId);
    if (!record) return { status: "not-found", messages: [], nextOffset: "", upToDate: false };
    if (record.lifecycle.softDeleted)
      return { status: "gone", messages: [], nextOffset: "", upToDate: false };

    const messages = await this.readChain(streamId, record, normalizeReadOffset(options.offset));
    const lastOffset =
      messages.length > 0 ? messages[messages.length - 1]!.offset : record.currentOffset;
    const nextOffset =
      compareOffsets(lastOffset, record.currentOffset) > 0 ? lastOffset : record.currentOffset;
    const upToDate = nextOffset === record.currentOffset;
    return {
      status: "ok",
      messages,
      nextOffset,
      upToDate,
      closed: record.lifecycle.closed === true && upToDate,
    };
  }
}
