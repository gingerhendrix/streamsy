import { Array, Effect, Option, Schema } from "effect";
import { SqlClient, type SqlError } from "effect/sql";
import { compare, StreamAddress, type Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { Streams, StreamsWriter, type StreamRoute } from "@streamsy/core/protocol";
import type { Slice, Slices } from "./batch.ts";
import * as Commit from "./commit.ts";
import { declare, type Declaration as ProjectionDeclaration } from "./declaration.ts";
import { ProjectionFault } from "./fault.ts";
import { Transaction } from "./transaction.ts";
import { Change, message } from "./state-table-message.ts";
import { decodeRow, storedValues } from "./state-table-rows.ts";

interface Head {
  readonly applied_offset: string;
  readonly epoch: number;
  readonly snapshot_open: number;
}

const empty: Head = { applied_offset: ZERO_OFFSET, epoch: 0, snapshot_open: 0 };

const head = (sql: SqlClient.SqlClient, id: string) =>
  Effect.map(
    sql<Head>`SELECT applied_offset, epoch, snapshot_open FROM streamsy_state_heads WHERE stream_id = ${id}`,
    Array.head,
  );

/** The offset that the rows reflect. A table with no head has no rows. */
const appliedOffset = (sql: SqlClient.SqlClient, id: string) =>
  Effect.map(head(sql, id), (stored) => Option.getOrElse(stored, () => empty).applied_offset);

const clearRows = (sql: SqlClient.SqlClient, id: string) =>
  sql`DELETE FROM streamsy_state_rows WHERE stream_id = ${id}`;

const save = (sql: SqlClient.SqlClient, id: string, state: Head) =>
  sql`INSERT INTO streamsy_state_heads (stream_id, applied_offset, epoch, snapshot_open)
    VALUES (${id}, ${state.applied_offset}, ${state.epoch}, ${state.snapshot_open})
    ON CONFLICT (stream_id) DO UPDATE SET applied_offset = excluded.applied_offset,
      epoch = excluded.epoch, snapshot_open = excluded.snapshot_open`;

const sqlFault = (id: string) => (cause: SqlError.SqlError) =>
  new ProjectionFault({
    phase: "process",
    reason: "storage-failure",
    message: `Cannot update the rows of ${id}`,
    cause,
  });

const apply = Effect.fn("StateTable.apply")(
  function* (
    sql: SqlClient.SqlClient,
    id: string,
    before: Pick<Head, "epoch" | "snapshot_open">,
    items: ReadonlyArray<string>,
  ) {
    let epoch = before.epoch;
    let open = before.snapshot_open;

    for (const item of items) {
      yield* Change.$match(yield* message(item), {
        Control: ({ control }) => {
          if (control === "reset") {
            open = 0;

            return Effect.asVoid(clearRows(sql, id));
          }

          if (control === "snapshot-start") {
            epoch += 1;
            open = 1;

            return Effect.void;
          }

          if (open === 0) return Effect.void;
          open = 0;

          return Effect.asVoid(
            sql`DELETE FROM streamsy_state_rows WHERE stream_id = ${id} AND epoch < ${epoch}`,
          );
        },
        Upsert: ({ collection, key, value }) =>
          Effect.asVoid(sql`INSERT INTO streamsy_state_rows (stream_id, collection, row_key, value, epoch)
            VALUES (${id}, ${collection}, ${key}, ${value}, ${epoch})
            ON CONFLICT (stream_id, collection, row_key) DO UPDATE SET value = excluded.value, epoch = excluded.epoch`),
        Delete: ({ collection, key }) =>
          Effect.asVoid(sql`DELETE FROM streamsy_state_rows WHERE stream_id = ${id}
            AND collection = ${collection} AND row_key = ${key}`),
      });
    }

    return { epoch, snapshot_open: open };
  },
  (effect, _sql, id) =>
    Effect.catchTag(
      effect,
      "SchemaError",
      (cause) =>
        new ProjectionFault({
          phase: "process",
          reason: "invalid-record",
          message: `A stored message of ${id} is not a State message`,
          cause,
        }),
    ),
);

/** Create the SQLite row index at host startup, outside storage migrations. */
export const migrate = Effect.fn("StateTable.migrate")(function* (sql: SqlClient.SqlClient) {
  yield* sql.onDialectOrElse({
    sqlite: () => Effect.void,
    orElse: () =>
      Effect.fail(
        new ProjectionFault({
          phase: "load",
          reason: "unsupported-composition",
          message: "StateTable requires SQLite",
        }),
      ),
  });
  yield* sql`CREATE TABLE IF NOT EXISTS streamsy_state_heads (
    stream_id TEXT PRIMARY KEY,
    applied_offset TEXT NOT NULL,
    epoch INTEGER NOT NULL DEFAULT 0,
    snapshot_open INTEGER NOT NULL DEFAULT 0
  ) WITHOUT ROWID`;
  yield* sql`CREATE TABLE IF NOT EXISTS streamsy_state_rows (
    stream_id TEXT NOT NULL,
    collection TEXT NOT NULL,
    row_key TEXT NOT NULL,
    value TEXT NOT NULL,
    epoch INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (stream_id, collection, row_key)
  ) WITHOUT ROWID`;
});

/** The State stream as stored JSON text, so that apply never needs a collection schema. */
type Raw<R> = Streamsy.Address<string, R>;

const synchronize = <R>(address: Raw<R>): Effect.Effect<void, ProjectionFault, Transaction | R> =>
  Effect.gen(function* () {
    const { sql } = yield* Transaction;
    const { wire } = yield* address.stream;

    const unavailable = () =>
      new ProjectionFault({
        phase: "read",
        reason: "history-unavailable",
        message: `Required history of ${address.id} is unavailable`,
      });

    const storage = (cause: unknown) =>
      new ProjectionFault({
        phase: "read",
        reason: "storage-failure",
        message: `Cannot read ${address.id}`,
        cause,
      });

    const stored = yield* head(sql, address.id);

    const tail = yield* wire.head(address.id).pipe(
      Effect.catchTags({
        StreamNotFound: unavailable,
        StreamGone: unavailable,
        StorageFault: storage,
        TransportFault: storage,
      }),
    );

    let state = Option.getOrElse(stored, () => empty);

    if (Option.isNone(stored) || compare(state.applied_offset, tail.nextOffset) > 0) {
      yield* clearRows(sql, address.id);
      state = empty;
    }

    while (compare(state.applied_offset, tail.nextOffset) < 0) {
      const page = yield* wire.read(address.id, { offset: state.applied_offset }).pipe(
        Effect.catchTags({
          InvalidReadRequest: (cause) =>
            new ProjectionFault({
              phase: "read",
              reason: "invalid-source",
              message: cause.message,
              cause,
            }),
          StreamNotFound: unavailable,
          StreamGone: unavailable,
          StorageFault: storage,
          TransportFault: storage,
        }),
      );

      // A read that does not advance would loop for ever.
      if (compare(page.nextOffset, state.applied_offset) <= 0) return yield* unavailable();

      state = {
        ...(yield* apply(
          sql,
          address.id,
          state,
          Array.map(page.messages, (item) => new TextDecoder().decode(item.data)),
        )),
        applied_offset: page.nextOffset,
      };
    }

    return yield* save(sql, address.id, state);
  }).pipe(
    Effect.catchTag("SqlError", sqlFault(address.id)),
    Effect.asVoid,
    Effect.withSpan("StateTable.sync"),
  );

const applySlice = <R>(
  address: Raw<R>,
  slice: Slice<string>,
): Effect.Effect<void, ProjectionFault, Transaction | R> =>
  Effect.gen(function* () {
    const { sql } = yield* Transaction;
    const stored = yield* head(sql, address.id);

    if (
      (Option.isSome(stored) && stored.value.applied_offset === slice.from) ||
      (Option.isNone(stored) && slice.from === ZERO_OFFSET)
    ) {
      if (Option.isNone(stored)) yield* clearRows(sql, address.id);

      const next = yield* apply(
        sql,
        address.id,
        Option.getOrElse(stored, () => empty),
        slice.items,
      );

      yield* save(sql, address.id, { ...next, applied_offset: slice.nextOffset });
    } else yield* synchronize(address);
  }).pipe(
    Effect.catchTag("SqlError", sqlFault(address.id)),
    Effect.withSpan("StateTable.applySlice"),
  );

/** Keyset pages are not snapshots. Follow changes from the first page's offset. */
export interface ListOptions {
  readonly after?: string | undefined;
  readonly limit?: number | undefined;
}

/** A State stream bound to its params: a bound projection, plus the row reads and `sync`. */
export interface Bound<C extends StreamAddress.Collections, R> {
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly params: Record<string, string>;
  /** The State stream, read as stored JSON text. */
  readonly inputs: { readonly state: Streamsy.Address<string, R> };
  readonly commit: typeof Commit.transaction;
  readonly process: (
    batch: Slices<{ readonly state: Streamsy.Address<string, R> }>,
  ) => Effect.Effect<void, ProjectionFault, Transaction | R>;
  /** The row of `key`. `value` is undefined when the row is absent. */
  readonly get: <K extends keyof C & string>(
    collection: K,
    key: string,
  ) => Effect.Effect<
    { readonly offset: string; readonly value: C[K]["schema"]["Type"] | undefined },
    Schema.SchemaError | SqlError.SqlError,
    SqlClient.SqlClient | C[K]["schema"]["DecodingServices"]
  >;
  /** The rows of `keys` that exist, by key. */
  readonly getMany: <K extends keyof C & string>(
    collection: K,
    keys: ReadonlyArray<string>,
  ) => Effect.Effect<
    { readonly offset: string; readonly rows: ReadonlyMap<string, C[K]["schema"]["Type"]> },
    Schema.SchemaError | SqlError.SqlError,
    SqlClient.SqlClient | C[K]["schema"]["DecodingServices"]
  >;
  /** The rows in key order. `next` is the last key of a full page. */
  readonly list: <K extends keyof C & string>(
    collection: K,
    options?: ListOptions,
  ) => Effect.Effect<
    {
      readonly offset: string;
      readonly rows: ReadonlyArray<{
        readonly key: string;
        readonly value: C[K]["schema"]["Type"];
      }>;
      readonly next: string | undefined;
    },
    ProjectionFault | Schema.SchemaError | SqlError.SqlError,
    SqlClient.SqlClient | C[K]["schema"]["DecodingServices"]
  >;
  /** The stream offset that the rows reflect. */
  // oxlint-disable-next-line effecttsgo/lazy-effect -- Every member of a bound table is a call, so each call gets its own span.
  readonly offset: () => Effect.Effect<string, SqlError.SqlError, SqlClient.SqlClient>;
  /** Apply the stream from the head of the table to its tail, in the caller's transaction. */
  // oxlint-disable-next-line effecttsgo/lazy-effect -- Every member of a bound table is a call, so each call gets its own span.
  readonly sync: () => Effect.Effect<void, ProjectionFault, Transaction | R>;
}

/** The declaration of a State table. Runners and `attach` take it like any declaration. */
export interface Declaration<
  Codecs extends StreamRoute.ParamCodecs,
  C extends StreamAddress.Collections,
  R,
> extends Omit<ProjectionDeclaration<Codecs, Bound<C, R>>, "inputs" | "parse"> {
  readonly inputs: { readonly state: State<Codecs, C, R> };
  readonly parse: (id: string) => Option.Option<StreamRoute.Params<Codecs>>;
}

/** A `Streamsy.state` class with param codecs `Codecs` and collections `C`, served by `R`. */
type State<
  Codecs extends StreamRoute.ParamCodecs,
  C extends StreamAddress.Collections,
  R,
> = StreamRoute.StateRoute<StreamRoute.Params<Codecs>, C> &
  Streamsy.Any & {
    readonly paramCodecs: Codecs;
    readonly at: (
      params: StreamRoute.Params<Codecs>,
    ) => StreamAddress.StateAddress<C> &
      Streamsy.Address<
        StreamAddress.CollectionsEvent<C>,
        R,
        StreamAddress.CollectionsDecodingServices<C>,
        StreamAddress.CollectionsEncodingServices<C>
      >;
  };

const reads = <C extends StreamAddress.Collections>(
  id: string,
  collections: C,
): Pick<Bound<C, never>, "get" | "getMany" | "list" | "offset"> => {
  const readValue = <K extends keyof C & string>(
    collection: K,
    json: string,
  ): Effect.Effect<
    C[K]["schema"]["Type"],
    Schema.SchemaError,
    C[K]["schema"]["DecodingServices"]
  > => {
    const entry = collections[collection];

    // The compiler rejects an undeclared collection, so this is a defect.
    if (entry === undefined)
      return Effect.die(new RangeError(`Unknown State collection ${collection}`));

    return decodeRow<
      C[K]["schema"]["Type"],
      C[K]["schema"]["Encoded"],
      C[K]["schema"]["DecodingServices"],
      C[K]["schema"]["EncodingServices"]
    >(entry.schema, json);
  };

  return {
    offset: Effect.fn("StateTable.offset")(function* () {
      return yield* appliedOffset(yield* SqlClient.SqlClient, id);
    }),
    get: Effect.fn("StateTable.get")(function* <K extends keyof C & string>(
      collection: K,
      key: string,
    ) {
      const sql = yield* SqlClient.SqlClient;
      const offset = yield* appliedOffset(sql, id);

      const rows = yield* sql<{
        readonly value: string;
      }>`SELECT value FROM streamsy_state_rows WHERE stream_id = ${id} AND collection = ${collection} AND row_key = ${key}`;

      const row = rows[0];

      return {
        offset,
        value: row === undefined ? undefined : yield* readValue(collection, row.value),
      };
    }),
    getMany: Effect.fn("StateTable.getMany")(function* <K extends keyof C & string>(
      collection: K,
      keys: ReadonlyArray<string>,
    ) {
      const sql = yield* SqlClient.SqlClient;
      const offset = yield* appliedOffset(sql, id);
      const rows = new Map<string, C[K]["schema"]["Type"]>();

      for (const [key, value] of yield* storedValues(sql, id, collection, keys))
        rows.set(key, yield* readValue(collection, value));

      return { offset, rows };
    }),
    list: Effect.fn("StateTable.list")(function* <K extends keyof C & string>(
      collection: K,
      options: ListOptions = {},
    ) {
      if (
        options.limit !== undefined &&
        (!Number.isSafeInteger(options.limit) || options.limit <= 0)
      )
        return yield* new ProjectionFault({
          phase: "load",
          reason: "invalid-options",
          message: "StateTable list limit must be a positive safe integer",
        });
      const sql = yield* SqlClient.SqlClient;
      const offset = yield* appliedOffset(sql, id);

      const found = yield* sql<{
        readonly row_key: string;
        readonly value: string;
      }>`SELECT row_key, value FROM streamsy_state_rows
        WHERE stream_id = ${id} AND collection = ${collection}
        ${options.after === undefined ? sql`` : sql`AND row_key > ${options.after}`}
        ORDER BY row_key ${options.limit === undefined ? sql`` : sql`LIMIT ${options.limit}`}`;

      const rows = yield* Effect.forEach(found, (row) =>
        Effect.map(readValue(collection, row.value), (value) => ({ key: row.row_key, value })),
      );

      return {
        offset,
        rows,
        next:
          options.limit !== undefined && rows.length === options.limit
            ? rows[rows.length - 1]?.key
            : undefined,
      };
    }),
  };
};

/** A State stream's declaration, kept current by the ordinary projection runners. */
export function define<
  Codecs extends StreamRoute.ParamCodecs,
  C extends StreamAddress.Collections,
  R,
>(state: State<Codecs, C, R>): Declaration<Codecs, C, R> {
  // `declare` gives the identity and the param encoding. The input and `parse` are set
  // below, because `declare` types its inputs without their schema services.
  const declaration = declare(
    { id: `state-table/${state.key}`, params: state.paramCodecs, inputs: {} },
    ({ decoded, ...key }): Bound<C, R> => {
      const address = state.at(decoded);

      // Keep the same State stream and service, but retain raw JSON for schema-independent apply.
      const raw = Object.assign(StreamAddress.json(address.id, { schema: Schema.String }), {
        stream: address.stream,
        codec: Schema.String,
      });

      return {
        ...key,
        inputs: { state: raw },
        commit: Commit.transaction,
        process: (batch) => applySlice(raw, batch.state),
        ...reads(address.id, state.collections),
        sync: () => synchronize(raw),
      };
    },
  );

  return { ...declaration, inputs: { state }, parse: state.parse };
}

/** Discard the index and replay the State stream atomically, retaining the binding checkpoint. */
export const rebuild = Effect.fn("StateTable.rebuild")(function* <
  C extends StreamAddress.Collections,
  R,
>(table: Bound<C, R>) {
  const tx = yield* Transaction;
  const id = table.inputs.state.id;

  yield* tx.withTransaction(
    clearRows(tx.sql, id).pipe(
      Effect.andThen(tx.sql`DELETE FROM streamsy_state_heads WHERE stream_id = ${id}`),
      Effect.catchTag("SqlError", sqlFault(id)),
      Effect.andThen(table.sync()),
    ),
  );
});

/** Append a reset to the State stream. The next table run applies it. */
export const reset = Effect.fn("StateTable.reset")(function* <
  C extends StreamAddress.Collections,
  R,
>(table: Bound<C, R>) {
  const address = table.inputs.state;
  const { wire } = yield* address.stream;
  // The table's input codec is raw JSON; a control-only State address uses core's encoder.
  const state = StreamAddress.state(address.id, { collections: {} });

  return yield* Streams.append(state, [StreamAddress.reset()]).pipe(
    Effect.provideService(StreamsWriter, wire),
  );
});
