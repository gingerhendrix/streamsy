/**
 * The browser-safe public errors these sinks may return.
 *
 * Every failure a consumer can act on carries a `recovery`, so a client never
 * has to infer a retry policy from a status code. Neither union has an
 * authorization member: access control belongs at the HTTP and session boundary
 * that wraps these handlers, never inside a checked sink contract.
 */

export const STREAM_SINK_ERROR_TAGS = [
  "InvalidSinkParams",
  "ProtocolVersionUnsupported",
  "ResumeRejected",
  "FeedUnavailable",
  "WireDecodeFailed",
] as const;
export type StreamSinkErrorTag = (typeof STREAM_SINK_ERROR_TAGS)[number];

/** A stream sink is recovered by reading its feed again from the start. */
export type StreamSinkRecovery = "replay-from-start";
export type StreamResumeRejectedReason =
  | "invalid-offset"
  | "history-unavailable"
  | "contract-changed";

export type StreamSinkPublicError =
  | {
      readonly _tag: "InvalidSinkParams";
      readonly sink: string;
      readonly parameter: string;
      readonly detail: string;
    }
  | {
      readonly _tag: "ProtocolVersionUnsupported";
      readonly sink: string;
      readonly supported: number;
      readonly received: string;
      readonly recovery: StreamSinkRecovery;
    }
  | {
      readonly _tag: "ResumeRejected";
      readonly sink: string;
      readonly reason: StreamResumeRejectedReason;
      readonly recovery: StreamSinkRecovery;
    }
  | { readonly _tag: "FeedUnavailable"; readonly sink: string; readonly detail: string }
  | { readonly _tag: "WireDecodeFailed"; readonly sink: string; readonly detail: string };

export const DOCUMENT_SINK_ERROR_TAGS = [
  "InvalidSinkParams",
  "ContractChanged",
  "DocumentUnavailable",
  "WireDecodeFailed",
] as const;
export type DocumentSinkErrorTag = (typeof DOCUMENT_SINK_ERROR_TAGS)[number];

/** A document sink is recovered by fetching the document unconditionally. */
export type DocumentSinkRecovery = "refetch";

export type DocumentSinkPublicError =
  | {
      readonly _tag: "InvalidSinkParams";
      readonly sink: string;
      readonly parameter: string;
      readonly detail: string;
    }
  | {
      readonly _tag: "ContractChanged";
      readonly sink: string;
      readonly recovery: DocumentSinkRecovery;
    }
  | { readonly _tag: "DocumentUnavailable"; readonly sink: string; readonly detail: string }
  | { readonly _tag: "WireDecodeFailed"; readonly sink: string; readonly detail: string };
