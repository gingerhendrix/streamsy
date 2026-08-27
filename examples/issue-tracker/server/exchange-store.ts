/**
 * Where an exchange's resume positions live: the global domain.
 *
 * A cursor belongs to neither side of the edge it describes. Putting it in the
 * source workspace would make one workspace's partition responsible for a fact
 * about another domain, and putting it in the destination would multiply one
 * position by every user the source feeds. It belongs to the host, and the
 * global domain is the host's own partition — so this store is the global
 * partition's whole reason to exist.
 *
 * Every value is read back through {@link ExchangeCursor}, which carries its
 * own position-domain literal. A native Durable Streams offset or an A4
 * `HistoryPosition` written into this table does not decode, and the read fails
 * typed rather than resuming from a position whose domain is unknown.
 */
import { Database } from "bun:sqlite";
import { Context, Effect, Layer, Schema } from "effect";
import { ExchangeCursor, initialCursor } from "../domain/exchange.ts";
import { partitionKeyString, type PartitionKey } from "../domain/domains.ts";
import { ExchangeCursorPoison, ExchangeStoreUnavailable } from "./domain-errors.ts";

export interface ExchangeCursorStoreService {
  /** This exchange's position against one source, or a fresh one if it has none. */
  readonly read: (
    exchange: string,
    version: number,
    source: PartitionKey,
  ) => Effect.Effect<ExchangeCursor, ExchangeStoreUnavailable | ExchangeCursorPoison>;
  /** Record an advanced position. Idempotent: the same cursor written twice is one row. */
  readonly advance: (cursor: ExchangeCursor) => Effect.Effect<void, ExchangeStoreUnavailable>;
  /** Every position this host holds, for the operator surface. */
  readonly list: Effect.Effect<
    readonly ExchangeCursor[],
    ExchangeStoreUnavailable | ExchangeCursorPoison
  >;
}

export class ExchangeCursorStore extends Context.Service<
  ExchangeCursorStore,
  ExchangeCursorStoreService
>()("issue-tracker/ExchangeCursorStore") {}

export const EXCHANGE_SCHEMA = `CREATE TABLE IF NOT EXISTS exchange_cursors (
  exchange TEXT NOT NULL,
  source   TEXT NOT NULL,
  value    TEXT NOT NULL,
  PRIMARY KEY (exchange, source)
);`;

interface CursorValueRow {
  readonly exchange: string;
  readonly source: string;
  readonly value: string;
}

const ExchangeCursorJson = Schema.fromJsonString(ExchangeCursor);
const encodeCursor = Schema.encodeUnknownSync(ExchangeCursorJson);
const cursorId = (exchange: string, source: string): string => `${exchange}\u0000${source}`;

const sqlite = <A>(operation: string, run: () => A) =>
  Effect.try({
    try: run,
    catch: (cause) =>
      new ExchangeStoreUnavailable({
        operation,
        detail: cause instanceof Error ? cause.message : String(cause),
      }),
  });

/**
 * Decode one durable cursor, or fail typed.
 *
 * The declaration's version is checked here rather than trusted: a cursor
 * written by an older exchange describes a position in a different record
 * stream, and resuming from it would skip or repeat work silently.
 */
const restore = (exchange: string, source: string, version: number | undefined, value: string) =>
  Schema.decodeEffect(ExchangeCursorJson)(value).pipe(
    Effect.mapError(
      (cause) =>
        new ExchangeCursorPoison({
          exchange,
          source,
          detail: String(cause),
        }),
    ),
    Effect.flatMap((cursor) =>
      version !== undefined && cursor.version !== version
        ? Effect.fail(
            new ExchangeCursorPoison({
              exchange,
              source,
              detail: `cursor is version ${cursor.version}, the exchange declares ${version}`,
            }),
          )
        : Effect.succeed(cursor),
    ),
  );

/** The in-memory cursor store. Same decode path, no durability. */
// oxlint-disable-next-line effecttsgo/lazy-effect -- This factory is the host's isolation boundary: each global partition must acquire its own mutable backing.
export const exchangeMemoryLayer = (): Layer.Layer<ExchangeCursorStore> =>
  Layer.sync(ExchangeCursorStore, () => {
    const stored = new Map<string, string>();
    return ExchangeCursorStore.of({
      read: Effect.fn("ExchangeCursorStore.read")(function* (exchange, version, source) {
        const key = partitionKeyString(source);
        const value = stored.get(cursorId(exchange, key));
        if (value === undefined) return initialCursor(exchange, version, source);
        return yield* restore(exchange, key, version, value);
      }),
      advance: (cursor) =>
        Effect.sync(() => {
          stored.set(
            cursorId(cursor.exchange, partitionKeyString(cursor.source)),
            encodeCursor(cursor),
          );
        }),
      list: Effect.gen(function* () {
        const cursors: ExchangeCursor[] = [];
        for (const [key, value] of [...stored].toSorted(([left], [right]) =>
          left.localeCompare(right),
        )) {
          const [exchange = "", source = ""] = key.split("\u0000");
          cursors.push(yield* restore(exchange, source, undefined, value));
        }
        return cursors;
      }),
    });
  });

/** The durable cursor store: one file, owned by the global partition. */
export const exchangeSqliteLayer = (options: {
  readonly filename: string;
}): Layer.Layer<ExchangeCursorStore> =>
  Layer.effect(
    ExchangeCursorStore,
    Effect.acquireRelease(
      Effect.sync(() => {
        const database = new Database(options.filename, { create: true });
        database.exec("PRAGMA journal_mode = WAL");
        database.exec(EXCHANGE_SCHEMA);
        return database;
      }),
      (database) => Effect.sync(() => database.close(false)),
    ).pipe(Effect.map(exchangeCursorService)),
  );

export function exchangeCursorService(database: Database): ExchangeCursorStoreService {
  const selectCursor = database.query<CursorValueRow, [string, string]>(
    "SELECT exchange, source, value FROM exchange_cursors WHERE exchange = ? AND source = ?",
  );
  const selectAll = database.query<CursorValueRow, []>(
    "SELECT exchange, source, value FROM exchange_cursors ORDER BY exchange, source",
  );
  const upsertCursor = database.query<never, [string, string, string]>(
    "INSERT INTO exchange_cursors (exchange, source, value) VALUES (?, ?, ?)" +
      " ON CONFLICT (exchange, source) DO UPDATE SET value = excluded.value",
  );

  return ExchangeCursorStore.of({
    read: Effect.fn("ExchangeCursorStore.read")(function* (exchange, version, source) {
      const key = partitionKeyString(source);
      const found = yield* sqlite("read", () => selectCursor.get(exchange, key));
      if (found === null || found === undefined) return initialCursor(exchange, version, source);
      return yield* restore(exchange, key, version, found.value);
    }),
    advance: (cursor) =>
      sqlite("advance", () => {
        upsertCursor.run(cursor.exchange, partitionKeyString(cursor.source), encodeCursor(cursor));
      }),
    list: Effect.gen(function* () {
      const found = yield* sqlite("list", () => selectAll.all());
      const cursors: ExchangeCursor[] = [];
      for (const row of found) {
        cursors.push(yield* restore(row.exchange, row.source, undefined, row.value));
      }
      return cursors;
    }),
  });
}
