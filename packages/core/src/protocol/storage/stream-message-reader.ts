/** Stored message read helpers for the durable streams protocol. */

import type {
  Offset,
  StoredMessage,
  StreamRecord,
  StreamStoreAdapter,
} from "../../types/storage.ts";
import { compareOffsets } from "../helpers/offset-generator.ts";
import { ExpiryPolicy } from "../helpers/expiry-policy.ts";

export class StreamMessageReader {
  constructor(
    private store: StreamStoreAdapter,
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
      const source = await this.store.get(record.lifecycle.forkedFrom);
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
    const own = await this.store.list(streamId, { after: ownStart, until: capOffset });
    out.push(...own);
    return out;
  }

  async readOwn(
    streamId: string,
    after?: Offset,
  ): Promise<{ messages: StoredMessage[]; nextOffset: string }> {
    const record = await this.store.get(streamId);
    if (!record) return { messages: [], nextOffset: "" };
    const messages = await this.store.list(streamId, { after });
    return {
      messages,
      nextOffset:
        messages.length > 0 ? messages[messages.length - 1]!.offset : record.currentOffset,
    };
  }
}
