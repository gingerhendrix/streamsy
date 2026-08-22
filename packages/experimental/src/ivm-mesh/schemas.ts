import { Schema, SchemaGetter, SchemaTransformation } from "effect";

export const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
export const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

export const DurablePosition = Schema.String.check(
  Schema.isMinLength(1),
  Schema.makeFilter((value) => value !== "-1" && value !== "now", {
    description: "a real durable-stream position",
  }),
);

// Identity strings feed hashes and durable keys, so canonically equivalent inputs need equal bytes.
export const NormalizedRequiredText = Schema.String.pipe(
  Schema.decodeTo(
    Schema.String,
    SchemaTransformation.make({
      decode: SchemaGetter.transform((value: string) => value.normalize("NFC")),
      encode: SchemaGetter.passthrough(),
    }),
  ),
).check(Schema.isMinLength(1), Schema.isMaxLength(512));

export const CatchUpLimits = Schema.Struct({
  maxItems: PositiveInt,
  maxPages: PositiveInt,
  maxBatches: PositiveInt,
  maxBytes: PositiveInt,
});

export const StateFactType = Schema.String.check(
  Schema.isMinLength(1),
  Schema.makeFilter((value) => !value.startsWith("__streamsy."), {
    description: "an application-owned State collection type",
  }),
);

const StateFactBase = {
  type: StateFactType,
  key: Schema.NonEmptyString,
};

export const StateFact = Schema.Union([
  Schema.Struct({
    ...StateFactBase,
    value: Schema.Json,
    headers: Schema.Struct({ operation: Schema.Literals(["insert", "update", "upsert"]) }),
  }),
  Schema.Struct({
    ...StateFactBase,
    headers: Schema.Struct({ operation: Schema.Literal("delete") }),
  }),
]);

export type StateFact = typeof StateFact.Type;

export const decodeStateFact = Schema.decodeUnknownSync(StateFact);
