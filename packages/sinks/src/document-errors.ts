/**
 * The browser-safe public errors a document sink may return.
 *
 * Every failure a consumer can act on carries a `recovery`, so a client never
 * has to infer a retry policy from a status code. The union has no
 * authorization member: access control belongs at the HTTP and session boundary
 * that wraps these handlers, never inside a checked sink contract.
 */

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
