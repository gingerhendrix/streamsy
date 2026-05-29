/** Non-live read orchestration for one storage-bound stream. */

import type { ReadOptions, ReadResult } from "../types/protocol.ts";
import type { StoredMessage, StreamRecord } from "../types/storage.ts";
import { compareOffsets } from "./helpers/offset-generator.ts";

export type ReadChain = (record: StreamRecord, afterOffset?: string) => Promise<StoredMessage[]>;

export function normalizeReadOffset(offset?: string): string | undefined {
  return !offset || offset === "-1" ? undefined : offset;
}

export interface ReadServiceDeps {
  readChain: ReadChain;
}

export class ReadService {
  constructor(private deps: ReadServiceDeps) {}

  async execute(record: StreamRecord | null, options: ReadOptions): Promise<ReadResult> {
    if (!record) return { status: "not-found" };
    if (record.lifecycle.softDeleted) return { status: "gone" };

    const messages = await this.deps.readChain(record, normalizeReadOffset(options.offset));
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
