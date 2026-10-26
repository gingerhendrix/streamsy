import { Array, Order, Record, Schema } from "effect";

/** Length-safe encoding of inert string components; no persistence decoding here. */
export const encodeKey = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));

export const Counter = Schema.Finite.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0));

const terminal = {
  closed: Schema.optionalKey(Schema.Boolean),
  gone: Schema.optionalKey(Schema.Literal(true)),
};

export const Range = Schema.Struct({ from: Schema.String, nextOffset: Schema.String, ...terminal });

export interface Range extends Schema.Schema.Type<typeof Range> {}

/** A range with the item count a retry must reproduce exactly. */
export const PinnedRange = Schema.Struct({
  from: Schema.String,
  nextOffset: Schema.String,
  count: Counter,
  upToDate: Schema.optionalKey(Schema.Boolean),
  ...terminal,
});

export interface PinnedRange extends Schema.Schema.Type<typeof PinnedRange> {}

/** An outputs unit that was pinned before its append; settled by the next pass. */
export const PendingUnit = Schema.Struct({
  ranges: Schema.Record(Schema.String, PinnedRange),
  seqs: Schema.optionalKey(Schema.Record(Schema.String, Counter)),
});

export interface PendingUnit extends Schema.Schema.Type<typeof PendingUnit> {}

/** `params` holds the decoded values the declaration was bound with. Keys use the encoded strings. */
export interface Unit<P = Record<string, string>> {
  readonly projectionId: string;
  /** The declared version; defaults to 1. */
  readonly version: number;
  readonly generation: number;
  readonly params: P;
  /** The inputs that contributed items or a terminal outcome; retries reproduce these. */
  readonly ranges: Record<string, Range>;
  readonly key: string;
}

/** Deterministic JSON: keys sorted, so equal maps always encode identically. */
export const canonicalParams = (params: Record<string, string>): string =>
  JSON.stringify(
    Record.fromEntries(
      Array.map(Array.sort(Record.keys(params), Order.String), (k) => [k, params[k]]),
    ),
  );

/** The terminal keys of a slice or range. A false or absent key is left out. */
export interface Terminal {
  closed?: true;
  gone?: true;
}

export const terminalOf = (source: {
  readonly closed?: boolean | undefined;
  readonly gone?: boolean | undefined;
}): Terminal => {
  const keys: Terminal = {};

  if (source.closed === true) keys.closed = true;

  if (source.gone === true) keys.gone = true;

  return keys;
};

export const canonicalRanges = (ranges: Record<string, Range>): string =>
  JSON.stringify(
    Record.fromEntries(
      Array.map(Array.sort(Record.keys(ranges), Order.String), (k) => {
        const range = ranges[k];

        return [
          k,
          { from: range?.from, nextOffset: range?.nextOffset, ...terminalOf(range ?? {}) },
        ];
      }),
    ),
  );

/** The key is stable across producer retries; transaction retries may read a longer tail. */
export const unitOf = (
  projectionId: string,
  version: number,
  generation: number,
  params: Record<string, string>,
  ranges: Record<string, Range>,
): Unit => ({
  projectionId,
  version,
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

/** The handler view of a kernel unit, with the decoded params. */
export const withParams = <P>(unit: Unit, params: P): Unit<P> => ({ ...unit, params });
