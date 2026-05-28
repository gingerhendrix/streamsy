/** Stored message read helpers for storage-bound streams. */

import type { Stream } from "../../types/factory.ts";
import type { Offset, StoredMessage, StreamRecord } from "../../types/storage.ts";
import { compareOffsets } from "../helpers/offset-generator.ts";
import { ExpiryPolicy } from "../helpers/expiry-policy.ts";

export type ResolveStorageStream = (streamId: string) => Promise<Stream> | Stream;

export class StreamMessageReader {
  constructor(
    private resolve: ResolveStorageStream,
    private expiryPolicy: ExpiryPolicy,
  ) {}

  async readChain(
    streamId: string,
    record: StreamRecord,
    afterOffset?: string,
    capOffset?: string,
    touchOwnTtl = true,
  ): Promise<StoredMessage[]> {
    const out: StoredMessage[] = [];
    if (
      record.lifecycle.forkedFrom &&
      record.lifecycle.forkOffset &&
      (afterOffset === undefined || compareOffsets(afterOffset, record.lifecycle.forkOffset) < 0)
    ) {
      const sourceStream = await this.resolve(record.lifecycle.forkedFrom);
      const source = await sourceStream.getRecord();
      if (source) {
        const upstreamCap =
          capOffset && compareOffsets(capOffset, record.lifecycle.forkOffset) < 0
            ? capOffset
            : record.lifecycle.forkOffset;
        out.push(
          ...(await this.readChain(
            record.lifecycle.forkedFrom,
            source,
            afterOffset,
            upstreamCap,
            false,
          )),
        );
      }
    }
    if (touchOwnTtl) await this.expiryPolicy.touch(streamId, record, "read");
    const ownStart =
      record.lifecycle.forkOffset &&
      (afterOffset === undefined || compareOffsets(afterOffset, record.lifecycle.forkOffset) < 0)
        ? record.lifecycle.forkOffset
        : afterOffset;
    const own = await (
      await this.resolve(streamId)
    ).listMessages({ after: ownStart, until: capOffset });
    out.push(...own);
    return out;
  }

  async readOwn(
    streamId: string,
    after?: Offset,
  ): Promise<{ messages: StoredMessage[]; nextOffset: string }> {
    const stream = await this.resolve(streamId);
    const record = await stream.getRecord();
    if (!record) return { messages: [], nextOffset: "" };
    const messages = await stream.listMessages({ after });
    return {
      messages,
      nextOffset:
        messages.length > 0 ? messages[messages.length - 1]!.offset : record.currentOffset,
    };
  }
}
