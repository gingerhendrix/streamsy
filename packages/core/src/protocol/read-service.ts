/** Non-live read orchestration for one storage-bound stream. */

import type { ReadOptions, ReadResult } from "../types/protocol.ts";
import type { StreamId, StoredMessage, StreamRecord } from "../types/storage.ts";
import type { Stream } from "../types/factory.ts";
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
    private stream: Stream,
    private readChain: ReadChain,
  ) {}

  async execute(streamId: StreamId, options: ReadOptions): Promise<ReadResult> {
    const record = await this.stream.getRecord();
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
