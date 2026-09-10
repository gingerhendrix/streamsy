import { Schema } from "effect";
import { Offset } from "../schema/index.ts";

export { format, resultHeader } from "../protocol/remote-format.ts";
const text = Schema.String;
const optionalText = Schema.optionalKey(text);
const integer = Schema.Finite.pipe(
  Schema.check(
    Schema.isInt(),
    Schema.isGreaterThanOrEqualTo(0),
    Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
  ),
);
const optionalInteger = Schema.optionalKey(integer);
const closed = Schema.optionalKey(Schema.Boolean);
const unsupported = Schema.Struct({
  status: Schema.Literal("not-supported"),
  feature: text,
  message: optionalText,
});
const missing = Schema.Struct({ status: Schema.Literals(["not-found", "gone"]) });
const message = Schema.Struct({
  offset: Offset,
  timestamp: Schema.Finite,
  data: Schema.Array(integer.pipe(Schema.check(Schema.isLessThanOrEqualTo(255)))),
});
const batch = {
  messages: Schema.Array(message),
  nextOffset: Offset,
  upToDate: Schema.Boolean,
  closed,
};
export const head = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("ok"),
    contentType: Schema.String.pipe(Schema.check(Schema.isPattern(/^[^\s/;]+\/[^\s;]+(?:;.*)?$/))),
    nextOffset: Offset,
    ttlSeconds: optionalInteger,
    expiresAt: optionalText,
    closed,
  }),
  missing,
]);
export const read = Schema.Union([
  Schema.Struct({ status: Schema.Literal("ok"), ...batch }),
  missing,
]);
export const readNext = Schema.Union([
  Schema.Struct({ status: Schema.Literals(["ok", "timeout"]), ...batch, cursor: text }),
  Schema.Struct({
    status: Schema.Literals(["not-found", "gone"]),
    messages: Schema.Array(message),
    nextOffset: Schema.Literal(""),
    upToDate: Schema.Boolean,
    cursor: text,
    closed,
  }),
  unsupported,
]);
export const create = Schema.Union([
  Schema.Struct({
    status: Schema.Literals(["created", "exists"]),
    nextOffset: Offset,
    contentType: text,
    closed,
  }),
  Schema.Struct({
    status: Schema.Literals(["not-found", "bad-request"]),
    nextOffset: text,
    contentType: text,
    errorMessage: optionalText,
  }),
  Schema.Struct({
    status: Schema.Literal("conflict"),
    nextOffset: text,
    contentType: text,
    errorMessage: optionalText,
    conflictReason: Schema.optionalKey(
      Schema.Literals([
        "config-mismatch",
        "soft-deleted",
        "fork-content-type",
        "fork-source-soft-deleted",
        "fork-copy-limit",
      ]),
    ),
  }),
  unsupported,
]);
export const append = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("appended"),
    offset: Offset,
    producerEpoch: optionalInteger,
    producerSeq: optionalInteger,
    closed,
  }),
  Schema.Struct({
    status: Schema.Literal("duplicate"),
    offset: Offset,
    producerEpoch: integer,
    producerSeq: integer,
    closed,
  }),
  missing,
  Schema.Struct({
    status: Schema.Literal("conflict"),
    conflictReason: Schema.Literal("closed"),
    offset: Offset,
    closed: Schema.Literal(true),
  }),
  Schema.Struct({
    status: Schema.Literal("conflict"),
    conflictReason: Schema.Literal("expected-offset"),
    offset: Offset,
  }),
  Schema.Struct({
    status: Schema.Literal("conflict"),
    conflictReason: Schema.Literals(["content-type", "sequence"]),
  }),
  Schema.Struct({ status: Schema.Literals(["busy", "invalid-epoch-seq"]) }),
  Schema.Struct({ status: Schema.Literal("stale-epoch"), currentEpoch: integer }),
  Schema.Struct({
    status: Schema.Literal("producer-gap"),
    expectedSeq: integer,
    receivedSeq: integer,
  }),
  unsupported,
]);
export const remove = Schema.Struct({
  status: Schema.Literals(["ok", "not-found", "gone", "busy"]),
});
