export type { CreateOptions } from "../policy/options.ts";
export interface ProducerOptions {
  readonly producerId: string;
  readonly producerEpoch: number;
  readonly producerSeq: number;
}

export interface AppendOptions {
  readonly data: Uint8Array;
  readonly contentType: string;
  readonly seq?: string;
  readonly producer?: ProducerOptions;
  readonly close?: boolean;
  /**
   * Optimistic-concurrency precondition: append only if the stream's tail
   * offset still equals this offset. On mismatch the append fails with a
   * `conflict`/`expected-offset` result carrying the actual tail, and no
   * state (messages, close flag, producer state) is changed. `ZERO_OFFSET`
   * means "append only if the stream is still empty". Streamsy extension to
   * the Durable Streams protocol.
   */
  readonly expectedOffset?: string;
}

export interface ReadOptions {
  offset?: string;
}

/**
 * Options for one transport-neutral read/wait operation. `readNext` returns
 * available messages immediately or waits once for a stream change, timeout,
 * or cancellation. HTTP live modes decide whether to invoke it once or repeat
 * it; they are intentionally not part of this protocol contract.
 */
export interface ReadNextOptions {
  offset: string;
  cursor?: string;
}
