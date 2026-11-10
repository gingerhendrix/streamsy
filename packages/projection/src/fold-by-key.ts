import { Array, Effect, Option, Predicate } from "effect";
import { SqlClient } from "effect/sql";
import {
  isStreamsFault,
  type AppendError,
  type CreateError,
  type StreamAddress,
  type Streamsy,
  type StreamsFault,
} from "@streamsy/core";
import type { StreamRoute } from "@streamsy/core/protocol";
import type { InputMap, ItemOf, Slices } from "./batch.ts";
import { ProjectionFault } from "./fault.ts";
import { FoldByKeyError } from "./fold-by-key-error.ts";
import { items } from "./read.ts";
import type * as StateTable from "./state-table.ts";
import { deleteJson, upsertJson } from "./state-table-message.ts";
import { decodeRow, encodeRow, storedValues } from "./state-table-rows.ts";
import type { Step } from "./step.ts";
import type { Transaction } from "./transaction.ts";
import type { Unit } from "./unit.ts";

/** Return this from a fold to delete its row. The next record receives undefined. */
export const remove = Symbol("@streamsy/projection/FoldByKey.remove");

export type Remove = typeof remove;

type Output<C extends StreamAddress.Collections, R> = StreamAddress.StateAddress<C> &
  Streamsy.Address<
    StreamAddress.CollectionsEvent<C>,
    R,
    StreamAddress.CollectionsDecodingServices<C>,
    StreamAddress.CollectionsEncodingServices<C>
  >;

/**
 * A `Projection.process` handler for a declaration that lists the State stream
 * of the table in `outputs`, under the key `O`.
 */
export type Handler<
  C extends StreamAddress.Collections,
  O extends string,
  I extends InputMap,
  P,
  E,
  R,
  TR,
> = (
  batch: Slices<I>,
  unit: Unit<P>,
  context: { readonly outputs: Readonly<Record<O, Output<C, TR>>> },
) => Effect.Effect<
  void,
  E | FoldByKeyError | ProjectionFault,
  | R
  | TR
  | SqlClient.SqlClient
  | Transaction
  | StreamAddress.CollectionsDecodingServices<C>
  | StreamAddress.CollectionsEncodingServices<C>
>;

interface Spec<
  C extends StreamAddress.Collections,
  K extends keyof C & string,
  O extends string,
  A,
  P,
  E,
  R,
> {
  readonly output: O;
  readonly collection: K;
  readonly key: (record: A, unit: Unit<P>) => string | undefined;
  readonly fold: (
    previous: C[K]["schema"]["Type"] | undefined,
    record: A,
    unit: Unit<P>,
  ) => Step<C[K]["schema"]["Type"] | Remove, E, R>;
}

type Expanded<
  C extends StreamAddress.Collections,
  K extends keyof C & string,
  O extends string,
  I extends InputMap,
  A,
  P,
  E,
  R,
> = Spec<C, K, O, A, P, E, R> & {
  readonly records: (item: ItemOf<I[keyof I]>, unit: Unit<P>) => ReadonlyArray<A>;
};

const isEffect = <A, E, R>(step: Step<A, E, R>): step is Effect.Effect<A, E, R> =>
  Effect.isEffect(step);

const fault =
  (reason: "invalid-record" | "invalid-output" | "storage-failure", message: string) =>
  (cause: unknown) =>
    new ProjectionFault({ phase: "process", reason, message, cause });

const writeFault = (id: string) => (cause: StreamsFault | CreateError | AppendError) =>
  fault(
    isStreamsFault(cause) ? "storage-failure" : "invalid-output",
    `Cannot write output ${id}`,
  )(cause);

const make =
  <
    Codecs extends StreamRoute.ParamCodecs,
    C extends StreamAddress.Collections,
    TR,
    K extends keyof C & string,
    O extends string,
    I extends InputMap,
    A,
    P,
    E,
    R,
  >(
    table: StateTable.Declaration<Codecs, C, TR>,
    spec: Expanded<C, K, O, I, A, P, E, R>,
  ): Handler<C, O, I, P, E, R, TR> =>
  (batch, unit, { outputs }) =>
    Effect.gen(function* () {
      const output = outputs[spec.output];
      const collection = output.collections[spec.collection];
      const params = table.parse(output.id);

      // The compiler rejects an undeclared collection and a table of another State class.
      if (collection === undefined || Option.isNone(params))
        return yield* Effect.die(
          new RangeError(`${table.id} does not index ${spec.collection} of ${output.id}`),
        );
      const { wire } = yield* output.stream;

      yield* wire
        .create(output.id, { contentType: output.contentType })
        .pipe(Effect.mapError(writeFault(output.id)));
      yield* table.at(params.value).sync();
      const groups = new Map<string, A[]>();

      for (const { item } of items(batch)) {
        for (const record of spec.records(item, unit)) {
          const key = spec.key(record, unit);

          if (key === undefined) continue;
          const group = groups.get(key);

          if (group === undefined) groups.set(key, [record]);
          else group.push(record);
        }
      }

      const stored = yield* storedValues(yield* SqlClient.SqlClient, output.id, spec.collection, [
        ...groups.keys(),
      ]).pipe(Effect.mapError(fault("storage-failure", `Cannot read the rows of ${output.id}`)));

      type Row = C[K]["schema"];

      const schema: Row = collection.schema;
      const changes: string[] = [];

      for (const [key, records] of groups) {
        const before = stored.get(key);

        let value: Row["Type"] | undefined =
          before === undefined
            ? undefined
            : yield* decodeRow<
                Row["Type"],
                Row["Encoded"],
                Row["DecodingServices"],
                Row["EncodingServices"]
              >(schema, before).pipe(
                Effect.mapError(
                  fault("invalid-record", `Row ${key} of ${output.id} does not decode`),
                ),
              );

        for (const record of records) {
          const step = spec.fold(value, record, unit);
          const next = isEffect(step) ? yield* step : step;

          value = next === remove ? undefined : next;
        }

        if (value === undefined) {
          if (before !== undefined) changes.push(deleteJson(spec.collection, key));
          continue;
        }

        const field: PropertyKey = collection.key;
        const actual = Predicate.hasProperty(value, field) ? String(value[field]) : "undefined";

        if (actual !== key)
          return yield* new FoldByKeyError({
            message: `FoldByKey key "${key}" differs from folded row key "${actual}"`,
          });

        const after = yield* encodeRow<
          Row["Type"],
          Row["Encoded"],
          Row["DecodingServices"],
          Row["EncodingServices"]
        >(schema, value).pipe(
          Effect.mapError(fault("invalid-output", `Cannot encode row ${key} of ${output.id}`)),
        );

        // The table stores canonical JSON, so equal strings mean an unchanged row.
        if (after !== before) changes.push(upsertJson(spec.collection, key, after));
      }

      if (Array.isReadonlyArrayNonEmpty(changes))
        yield* wire
          .append(output.id, {
            contentType: output.contentType,
            data: new TextEncoder().encode(`[${Array.join(changes, ",")}]`),
          })
          .pipe(Effect.mapError(writeFault(output.id)));

      return yield* Effect.void;
    }).pipe(Effect.withSpan("FoldByKey.process"));

/** Build a transaction process handler for one collection, grouping records in first-seen order. */
export function process<
  Codecs extends StreamRoute.ParamCodecs,
  C extends StreamAddress.Collections,
  TR,
  K extends keyof C & string,
  O extends string,
  I extends InputMap,
  A,
  P,
  E = never,
  R = never,
>(
  table: StateTable.Declaration<Codecs, C, TR>,
  spec: Expanded<C, K, O, I, A, P, E, R>,
): Handler<C, O, I, P, E, R, TR>;
export function process<
  Codecs extends StreamRoute.ParamCodecs,
  C extends StreamAddress.Collections,
  TR,
  K extends keyof C & string,
  O extends string,
  I extends InputMap,
  P,
  E = never,
  R = never,
>(
  table: StateTable.Declaration<Codecs, C, TR>,
  spec: Spec<C, K, O, ItemOf<I[keyof I]>, P, E, R> & { readonly records?: never },
): Handler<C, O, I, P, E, R, TR>;
export function process<
  Codecs extends StreamRoute.ParamCodecs,
  C extends StreamAddress.Collections,
  TR,
  K extends keyof C & string,
  O extends string,
  I extends InputMap,
  A,
  P,
  E = never,
  R = never,
>(
  table: StateTable.Declaration<Codecs, C, TR>,
  spec:
    | Expanded<C, K, O, I, A, P, E, R>
    | (Spec<C, K, O, ItemOf<I[keyof I]>, P, E, R> & { readonly records?: never }),
) {
  if (spec.records !== undefined) return make(table, spec);

  return make<Codecs, C, TR, K, O, I, ItemOf<I[keyof I]>, P, E, R>(table, {
    ...spec,
    records: (item) => [item],
  });
}
