/** Payload-only messages exposed by the public protocol; storage owns offsets and timestamps. */
export interface ReadMessage {
  readonly data: Uint8Array;
}
export type CreateResult =
  | {
      readonly _tag: "Created";
      readonly nextOffset: string;
      readonly contentType: string;
      readonly closed: boolean;
    }
  | {
      readonly _tag: "Exists";
      readonly nextOffset: string;
      readonly contentType: string;
      readonly closed: boolean;
    };
export type AppendResult =
  | {
      readonly _tag: "Appended";
      /** Tail after this write; an after-exclusive read cursor. */ readonly offset: string;
      readonly producerEpoch?: number;
      readonly producerSeq?: number;
      readonly closed: boolean;
    }
  | {
      readonly _tag: "Duplicate";
      /** Current tail at acknowledgement, at or after the original write. */ readonly offset: string;
      readonly producerEpoch: number;
      readonly producerSeq: number;
      readonly closed: boolean;
    };
export interface ReadResult {
  readonly messages: ReadonlyArray<ReadMessage>;
  readonly nextOffset: string;
  readonly upToDate: boolean;
  readonly closed: boolean;
}
export interface ReadNextResult extends ReadResult {
  readonly cursor: string;
  readonly timedOut: boolean;
}
export interface HeadResult {
  readonly contentType: string;
  readonly nextOffset: string;
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
  readonly closed: boolean;
}
