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
  | "authorization-generation-changed"
  | "contract-changed";

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

/* oxlint-disable-next-line anti-slop/no-unknown-parameters -- This function is the public HTTP error decoder boundary. */
export function decodeStateSinkPublicError(value: unknown): StateSinkPublicError {
  const record = requireRecord(value);
  const tag = requireString(record, "_tag");
  const sink = requireString(record, "sink");
  switch (tag) {
    case "SinkUnauthorized":
      return { _tag: tag, sink, required: requireString(record, "required") };
    case "InvalidSinkParams":
      return {
        _tag: tag,
        sink,
        parameter: requireString(record, "parameter"),
        detail: requireString(record, "detail"),
      };
    case "ProtocolVersionUnsupported":
      return {
        _tag: tag,
        sink,
        supported: requireNumber(record, "supported"),
        received: requireString(record, "received"),
        recovery: requireRecovery(record),
      };
    case "ResumeRejected":
      return {
        _tag: tag,
        sink,
        reason: requireResumeReason(record),
        recovery: requireRecovery(record),
      };
    case "SnapshotUnavailable":
    case "TransportUnavailable":
    case "WireDecodeFailed":
      return { _tag: tag, sink, detail: requireString(record, "detail") };
    default:
      throw new Error(`unknown state-sink error: ${tag}`);
  }
}

/* oxlint-disable anti-slop/no-object-parameters, anti-slop/no-unknown-parameters, anti-slop/no-unknown-returns -- These helpers are the browser-safe JSON decoder boundary and immediately refine every field into the closed public union. */
function requireRecord(value: unknown): object {
  if (!(value instanceof Object) || Array.isArray(value)) {
    throw new Error("state-sink error response must be an object");
  }
  return value;
}

function requireString(record: object, field: string): string {
  const value = readField(record, field);
  const decoded = String(value);
  if (Object.prototype.toString.call(value) !== "[object String]" || value !== decoded) {
    throw new Error(`state-sink error field ${field} must be a string`);
  }
  return decoded;
}

function requireNumber(record: object, field: string): number {
  const value = readField(record, field);
  const decoded = Number(value);
  if (
    Object.prototype.toString.call(value) !== "[object Number]" ||
    value !== decoded ||
    !Number.isFinite(decoded)
  ) {
    throw new Error(`state-sink error field ${field} must be a finite number`);
  }
  return decoded;
}

function requireRecovery(record: object): StateSinkRecovery {
  const recovery = requireString(record, "recovery");
  if (recovery !== "snapshot-then-live") {
    throw new Error(`unknown state-sink recovery: ${recovery}`);
  }
  return recovery;
}

function requireResumeReason(record: object): ResumeRejectedReason {
  const reason = requireString(record, "reason");
  switch (reason) {
    case "invalid-offset":
    case "history-unavailable":
    case "protocol-incompatible":
    case "authorization-generation-changed":
    case "contract-changed":
      return reason;
    default:
      throw new Error(`unknown state-sink resume rejection: ${reason}`);
  }
}

function readField(record: object, field: string): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(record, field);
  return descriptor?.value;
}
/* oxlint-enable anti-slop/no-object-parameters, anti-slop/no-unknown-parameters, anti-slop/no-unknown-returns */
