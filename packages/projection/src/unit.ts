import { Schema } from "effect";

/** Length-safe encoding of inert string components; no persistence decoding here. */
export const encodeKey = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));

export const Counter = Schema.Finite.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0));

export const Range = Schema.Struct({ from: Schema.String, nextOffset: Schema.String });
export interface Range extends Schema.Schema.Type<typeof Range> {}

/** A range with the item count a retry must reproduce exactly. */
export const PinnedRange = Schema.Struct({
  from: Schema.String,
  nextOffset: Schema.String,
  count: Counter,
});
export interface PinnedRange extends Schema.Schema.Type<typeof PinnedRange> {}

/** A stream-output unit that was pinned before its append; settled by the next pass. */
export const PendingUnit = Schema.Struct({
  ranges: Schema.Record(Schema.String, PinnedRange),
  seq: Counter,
});
export interface PendingUnit extends Schema.Schema.Type<typeof PendingUnit> {}

export interface Unit {
  readonly projectionId: string;
  readonly generation: number;
  readonly params: Record<string, string>;
  /** Only the inputs that contributed items; a retry reproduces exactly these. */
  readonly ranges: Record<string, Range>;
  readonly key: string;
}

/** Deterministic JSON: keys sorted, so equal maps always encode identically. */
export const canonicalParams = (params: Record<string, string>): string =>
  JSON.stringify(
    Object.fromEntries(
      Object.keys(params)
        .toSorted()
        .map((k) => [k, params[k]]),
    ),
  );
export const canonicalRanges = (ranges: Record<string, Range>): string =>
  JSON.stringify(
    Object.fromEntries(
      Object.keys(ranges)
        .toSorted()
        .map((k) => [k, { from: ranges[k]?.from, nextOffset: ranges[k]?.nextOffset }]),
    ),
  );

/** The key is stable across stream-form retries; fused retries may read a longer tail. */
export const unitOf = (
  projectionId: string,
  generation: number,
  params: Record<string, string>,
  ranges: Record<string, Range>,
): Unit => ({
  projectionId,
  generation,
  params,
  ranges,
  key: encodeKey([
    projectionId,
    String(generation),
    canonicalParams(params),
    canonicalRanges(ranges),
  ]),
});
