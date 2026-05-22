/** Stream record construction for the durable streams protocol. */

import type { CreateOptions } from "../../types/protocol.ts";
import type { Clock, StreamRecord } from "../../types/storage.ts";
import { ZERO_OFFSET, parseCounter } from "../helpers/offset-generator.ts";
import { ExpiryPolicy } from "../helpers/expiry-policy.ts";

export interface ForkRecordDescriptor {
  forkedFrom: string;
  forkOffset: string;
}

export class StreamRecordFactory {
  constructor(
    private clock: Clock,
    private expiryPolicy: ExpiryPolicy,
  ) {}

  newRecord(
    streamId: string,
    contentType: string,
    options: CreateOptions,
    fork?: ForkRecordDescriptor,
  ): StreamRecord {
    const forkOffset = fork?.forkOffset;
    const config = {
      contentType,
      ttlSeconds: options.ttlSeconds,
      expiresAt: options.expiresAt,
      createdAt: this.clock.now(),
    };
    return {
      id: streamId,
      config,
      lifecycle: {
        childRefCount: 0,
        forkedFrom: fork?.forkedFrom,
        forkOffset,
        expiresAtMs: this.expiryPolicy.computeExpiresAtMs(config),
      },
      currentOffset: forkOffset ?? ZERO_OFFSET,
      counter: forkOffset ? parseCounter(forkOffset) : 0,
    };
  }
}
