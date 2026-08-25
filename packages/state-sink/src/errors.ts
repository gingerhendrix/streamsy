export const STATE_SINK_ERROR_TAGS = [
  "SinkUnauthorized",
  "InvalidSinkParams",
  "ProtocolVersionUnsupported",
  "ResumeRejected",
  "SnapshotUnavailable",
  "TransportUnavailable",
  "WireDecodeFailed",
] as const;

export type StateSinkErrorTag = (typeof STATE_SINK_ERROR_TAGS)[number];
export type StateSinkRecovery = "snapshot-then-live";
export type ResumeRejectedReason =
  | "invalid-offset"
  | "history-unavailable"
  | "protocol-incompatible"
  | "authorization-generation-changed";

export type StateSinkPublicError =
  | { readonly _tag: "SinkUnauthorized"; readonly sink: string; readonly required: string }
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
      readonly recovery: StateSinkRecovery;
    }
  | {
      readonly _tag: "ResumeRejected";
      readonly sink: string;
      readonly reason: ResumeRejectedReason;
      readonly recovery: StateSinkRecovery;
    }
  | { readonly _tag: "SnapshotUnavailable"; readonly sink: string; readonly detail: string }
  | { readonly _tag: "TransportUnavailable"; readonly sink: string; readonly detail: string }
  | { readonly _tag: "WireDecodeFailed"; readonly sink: string; readonly detail: string };

export function decodeStateSinkPublicError(value: unknown): StateSinkPublicError {
  if (!(value instanceof Object) || !("_tag" in value)) {
    throw new Error("state-sink error response has no _tag");
  }
  const tag = String(value._tag);
  if (!STATE_SINK_ERROR_TAGS.some((candidate) => candidate === tag)) {
    throw new Error(`unknown state-sink error: ${tag}`);
  }
  return value as StateSinkPublicError;
}
