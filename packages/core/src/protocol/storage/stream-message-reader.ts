/** Stored message read helpers for storage-bound streams. */

import type { Offset, StoredMessage, StreamRecord } from "../../types/storage.ts";
import type { BoundStream } from "../helpers/bind-stream.ts";
import { compareOffsets } from "../helpers/offset-generator.ts";
import { ExpiryPolicy } from "../helpers/expiry-policy.ts";

export type ResolveStorageStream = (streamId: string) => Promise<BoundStream> | BoundStream;

export interface StreamMessageReaderDeps {
  stream: BoundStream;
  resolve: ResolveStorageStream;
  expiryPolicy: ExpiryPolicy;
}

export class StreamMessageReader {
  constructor(private deps: StreamMessageReaderDeps) {}

  async readChain(
    record: StreamRecord,
    afterOffset?: string,
    capOffset?: string,
    touchOwnTtl = true,
  ): Promise<StoredMessage[]> {
    return this.readChainFor(this.deps.stream, record, afterOffset, capOffset, touchOwnTtl);
  }

  private async readChainFor(
    stream: BoundStream,
    record: StreamRecord,
    afterOffset?: string,
    capOffset?: string,
    touchOwnTtl = true,
  ): Promise<StoredMessage[]> {
    const out: StoredMessage[] = [];
    let current = record;
    if (
      current.lifecycle.forkedFrom &&
      current.lifecycle.forkOffset &&
      (afterOffset === undefined || compareOffsets(afterOffset, current.lifecycle.forkOffset) < 0)
    ) {
      const sourceStream = await this.deps.resolve(current.lifecycle.forkedFrom);
      const source = await sourceStream.getRecord();
      if (source) {
        const upstreamCap =
          capOffset && compareOffsets(capOffset, current.lifecycle.forkOffset) < 0
            ? capOffset
            : current.lifecycle.forkOffset;
        out.push(
          ...(await this.readChainFor(sourceStream, source, afterOffset, upstreamCap, false)),
        );
      }
    }
    if (touchOwnTtl) current = await this.deps.expiryPolicy.touch(stream, current, "read");
    const ownStart =
      current.lifecycle.forkOffset &&
      (afterOffset === undefined || compareOffsets(afterOffset, current.lifecycle.forkOffset) < 0)
        ? current.lifecycle.forkOffset
        : afterOffset;
    const own = await stream.listMessages({ after: ownStart, until: capOffset });
    out.push(...own);
    return out;
  }

  async readOwn(after?: Offset): Promise<{ messages: StoredMessage[]; nextOffset: string }> {
    const stream = this.deps.stream;
    const record = await stream.getRecord();
    if (!record) return { messages: [], nextOffset: "" };
    const messages = await stream.listMessages({ after });
    if (messages.length > 0) return { messages, nextOffset: messages[messages.length - 1]!.offset };
    // Visibility guard: with an eventually-consistent adapter, `awaitChange` may
    // report the tail advanced before the new messages are listable. When the
    // caller is parked behind the tail (`after < currentOffset`) and nothing is
    // listable yet, hold `nextOffset` at `after` instead of jumping to the tail —
    // do not advance and do not let the caller infer "closed". The next poll
    // repairs it. Adapters with atomic record+message visibility never hit this.
    if (after !== undefined && compareOffsets(after, record.currentOffset) < 0)
      return { messages, nextOffset: after };
    return { messages, nextOffset: record.currentOffset };
  }
}
