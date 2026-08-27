/**
 * Which sources an exchange is responsible for, durably.
 *
 * B4's exchange read exactly the workspaces the host happened to hold open, so
 * "the inbox converges" was true only while somebody was using the source
 * workspace. That is a fine property for a batch that was proving the edge
 * works and a bad one for a product that claims a person's inbox is complete.
 *
 * The registry closes it by remembering every source the exchange has ever
 * seen. It lives in the global partition beside the cursors, because a list of
 * *other people's* partitions belongs to no workspace and to no user, and
 * because the exchange already leases the global partition on every pass.
 *
 * Reopening cold storage is not free, so a pass takes a bounded number of cold
 * sources and takes the ones it has looked at least recently. Every registered
 * source is therefore visited within a bounded number of passes rather than
 * whenever a request happens to open it. See
 * `integration-2-decisions.md` for the policy and its cost.
 */
import type { Database } from "bun:sqlite";
import { Context, Effect, Layer } from "effect";
import { parsePartitionKey, partitionKeyString, type PartitionKey } from "../domain/domains.ts";
import { ExchangeStoreUnavailable } from "./domain-errors.ts";

/** One source the exchange is responsible for, and when it last looked at it. */
export interface RegisteredSource {
  readonly key: PartitionKey;
  readonly registeredAtMs: number;
  /** Zero until the exchange has completed a pass over it. */
  readonly lastExchangedAtMs: number;
}

export interface ExchangeSourceRegistryService {
  /** Remember these sources. Registering one twice does not move its timestamps. */
  readonly register: (
    keys: readonly PartitionKey[],
    atMs: number,
  ) => Effect.Effect<void, ExchangeStoreUnavailable>;
  /** Every registered source, least recently exchanged first. */
  readonly list: Effect.Effect<readonly RegisteredSource[], ExchangeStoreUnavailable>;
  /** Record that a pass has just run over this source. */
  readonly touch: (
    key: PartitionKey,
    atMs: number,
  ) => Effect.Effect<void, ExchangeStoreUnavailable>;
}

export class ExchangeSourceRegistry extends Context.Service<
  ExchangeSourceRegistry,
  ExchangeSourceRegistryService
>()("issue-tracker/ExchangeSourceRegistry") {}

export const SOURCE_REGISTRY_SCHEMA = `CREATE TABLE IF NOT EXISTS exchange_sources (
  source              TEXT PRIMARY KEY,
  registered_at_ms    INTEGER NOT NULL,
  last_exchanged_at_ms INTEGER NOT NULL DEFAULT 0
);`;

interface SourceRow {
  readonly source: string;
  readonly registered_at_ms: number;
  readonly last_exchanged_at_ms: number;
}

/**
 * Order the exchange visits sources in.
 *
 * Least recently exchanged first, and the key string breaks ties, so a host
 * that has never exchanged anything still visits its sources in a stable order
 * rather than whichever one storage returned first.
 */
export function compareSources(left: RegisteredSource, right: RegisteredSource): number {
  if (left.lastExchangedAtMs !== right.lastExchangedAtMs) {
    return left.lastExchangedAtMs - right.lastExchangedAtMs;
  }
  return partitionKeyString(left.key).localeCompare(partitionKeyString(right.key));
}

const sqlite = <A>(operation: string, run: () => A) =>
  Effect.try({
    try: run,
    catch: (cause) =>
      new ExchangeStoreUnavailable({
        operation,
        detail: cause instanceof Error ? cause.message : String(cause),
      }),
  });

/** The in-memory registry. Same ordering rule, no durability. */
// oxlint-disable-next-line effecttsgo/lazy-effect -- This factory is the host's isolation boundary: each global partition must acquire its own mutable backing.
export const sourceRegistryMemoryLayer = (): Layer.Layer<ExchangeSourceRegistry> =>
  Layer.sync(ExchangeSourceRegistry, () => {
    const stored = new Map<string, RegisteredSource>();
    return ExchangeSourceRegistry.of({
      register: (keys, atMs) =>
        Effect.sync(() => {
          for (const key of keys) {
            const id = partitionKeyString(key);
            if (stored.has(id)) continue;
            stored.set(id, { key, registeredAtMs: atMs, lastExchangedAtMs: 0 });
          }
        }),
      list: Effect.sync(() => [...stored.values()].toSorted(compareSources)),
      touch: (key, atMs) =>
        Effect.sync(() => {
          const id = partitionKeyString(key);
          const existing = stored.get(id);
          stored.set(id, {
            key,
            registeredAtMs: existing?.registeredAtMs ?? atMs,
            lastExchangedAtMs: atMs,
          });
        }),
    });
  });

/** The durable registry, over the global partition's own connection. */
export function sourceRegistryService(database: Database): ExchangeSourceRegistryService {
  const insertSource = database.query<never, [string, number]>(
    "INSERT INTO exchange_sources (source, registered_at_ms) VALUES (?, ?)" +
      " ON CONFLICT (source) DO NOTHING",
  );
  const selectSources = database.query<SourceRow, []>(
    "SELECT source, registered_at_ms, last_exchanged_at_ms FROM exchange_sources",
  );
  const touchSource = database.query<never, [string, number, number]>(
    "INSERT INTO exchange_sources (source, registered_at_ms, last_exchanged_at_ms)" +
      " VALUES (?, ?, ?) ON CONFLICT (source) DO UPDATE SET" +
      " last_exchanged_at_ms = excluded.last_exchanged_at_ms",
  );
  const registerAll = database.transaction((keys: readonly PartitionKey[], atMs: number) => {
    for (const key of keys) insertSource.run(partitionKeyString(key), atMs);
  });

  return ExchangeSourceRegistry.of({
    register: (keys, atMs) =>
      sqlite("register", () => {
        registerAll(keys, atMs);
      }),
    list: sqlite("listSources", () => {
      const sources: RegisteredSource[] = [];
      for (const row of selectSources.all()) {
        // A row whose key no longer parses is a key this host does not serve.
        // Skipping it is right: it is not a source, and failing the pass over
        // it would stop every source this host *can* serve.
        const key = parsePartitionKey(row.source);
        if (key === undefined) continue;
        sources.push({
          key,
          registeredAtMs: row.registered_at_ms,
          lastExchangedAtMs: row.last_exchanged_at_ms,
        });
      }
      return sources.toSorted(compareSources);
    }),
    touch: (key, atMs) =>
      sqlite("touchSource", () => {
        touchSource.run(partitionKeyString(key), atMs, atMs);
      }),
  });
}
