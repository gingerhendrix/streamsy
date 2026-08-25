/** Catch-up ingestion for the four application-owned Durable State sources. */
/* oxlint-disable typescript/consistent-return -- Effect requires `return yield*` for a never-succeeding failure branch; successful catch-up branches intentionally return void. */
import type { ReadOptions, StreamProtocolFactory } from "@streamsy/core";
import type { StreamBinding } from "@streamsy/experimental/binding";
import {
  createDurableStateProtocol,
  type DurableStateMessage,
  type DurableStateProtocol,
  type ValuesByWireType,
} from "@streamsy/state";
import type { Change } from "@streamsy/views-ir";
import { Context, Effect, Layer } from "effect";
import {
  catalog,
  catalogRow,
  decodeCatalogRow,
  type CatalogCollection,
  type CatalogRow,
} from "../domain/catalog.ts";
import { labels, projects, users, workspaceMetadata } from "../domain/declaration.ts";
import { SourcePoison, StreamUnavailable, UnsupportedStateOperation } from "./errors.ts";
import { IssueStore } from "./store.ts";
import { Streams, type WorkspaceBindings } from "./streams.ts";

export type CatalogStateProtocol = DurableStateProtocol<typeof catalog>;
type CatalogStateMessage = DurableStateMessage<ValuesByWireType<typeof catalog>>;

export class StateSourceProtocol extends Context.Service<
  StateSourceProtocol,
  CatalogStateProtocol
>()("issue-tracker/StateSourceProtocol") {}

/** Bind the catalog's single schema/type/key table to the host protocol once. */
export const stateSourceProtocolLayer = (
  protocol: StreamProtocolFactory,
): Layer.Layer<StateSourceProtocol> =>
  Layer.succeed(StateSourceProtocol, createDurableStateProtocol(protocol, catalog));

/** Bound checkpoint slices keep catch-up memory and store transactions finite. */
const STATE_SOURCE_READ_LIMIT = 1_000;

export interface DecodedStateUpsert {
  readonly key: string;
  readonly row: CatalogRow;
}

export interface StateIngestionReport {
  readonly collection: CatalogCollection;
  readonly workspaceId: string;
  readonly checkpoint: string | undefined;
  readonly folded: number;
  readonly changes: readonly Change<CatalogRow>[];
}

export interface StateFoldResult {
  readonly rows: ReadonlyMap<string, CatalogRow>;
  readonly changes: readonly Change<CatalogRow>[];
}

/** Pure current-row fold used by both hosts and by repeated-delivery tests. */
export function foldStateBoundary(
  current: ReadonlyMap<string, CatalogRow>,
  upserts: readonly DecodedStateUpsert[],
): StateFoldResult {
  const final = new Map<string, CatalogRow>();
  for (const upsert of upserts) final.set(upsert.key, upsert.row);

  const changes: Change<CatalogRow>[] = [];
  for (const [key, row] of final) {
    const before = current.get(key);
    if (before === undefined) changes.push({ kind: "enter", key, after: row });
    else if (JSON.stringify(before) !== JSON.stringify(row)) {
      changes.push({ kind: "update", key, before, after: row });
    }
  }
  return { rows: final, changes };
}

/** Bring one State source to its current durable tail, committing each boundary atomically. */
export const catchUpStateSource = Effect.fn("StateIngestion.catchUpStateSource")(function* (
  collection: CatalogCollection,
  workspaceId: string,
) {
  const store = yield* IssueStore;
  const streams = yield* Streams;
  const state = yield* StateSourceProtocol;
  const sourceId = stateSourceId(collection);
  const binding = stateSourceBinding(streams.bindings, collection, workspaceId);
  let checkpoint = yield* store.stateCheckpoint(sourceId, workspaceId);
  let folded = 0;
  const changes: Change<CatalogRow>[] = [];

  const opened = yield* Effect.tryPromise({
    try: () => state.get(binding.streamId),
    catch: (cause) =>
      new StreamUnavailable({ streamId: binding.streamId, status: describe(cause) }),
  });
  if (opened.status === "not-found" || opened.status === "gone") {
    return { collection, workspaceId, checkpoint, folded, changes } satisfies StateIngestionReport;
  }
  if (opened.status !== "ok") {
    return yield* new StreamUnavailable({ streamId: binding.streamId, status: opened.status });
  }

  for (;;) {
    const options: ReadOptions = { limit: STATE_SOURCE_READ_LIMIT };
    if (checkpoint !== undefined) options.offset = checkpoint;
    const batch = yield* Effect.tryPromise({
      try: () => opened.stream.read(options),
      catch: (cause) =>
        new StreamUnavailable({ streamId: binding.streamId, status: describe(cause) }),
    });
    if (batch.status === "invalid-json") {
      return yield* poison(
        sourceId,
        batch.offset ?? checkpoint ?? "-1",
        collection,
        undefined,
        describe(batch.error),
      );
    }
    if (batch.status !== "ok") {
      return yield* new StreamUnavailable({ streamId: binding.streamId, status: batch.status });
    }

    // The Durable State reader validates every envelope and collection schema
    // before returning any message, so this entire store boundary is poison-free.
    const upserts: DecodedStateUpsert[] = [];
    for (const message of batch.messages) {
      upserts.push(
        yield* decodeStateMessage(collection, workspaceId, sourceId, message.offset, message.value),
      );
    }
    const currentRows = yield* store.stateRows(sourceId, collection, workspaceId);
    const current = new Map(
      currentRows.map((row) => {
        const decoded = decodeCatalogRow(collection, row);
        return [decoded.key, row] as const;
      }),
    );
    const result = foldStateBoundary(current, upserts);
    yield* store.commitState(sourceId, workspaceId, {
      checkpoint: batch.nextOffset,
      rows: result.rows,
    });
    checkpoint = batch.nextOffset;
    folded += batch.messages.length;
    changes.push(...result.changes);
    if (batch.upToDate) break;
  }

  return { collection, workspaceId, checkpoint, folded, changes } satisfies StateIngestionReport;
});

export const catchUpCatalog = Effect.fn("StateIngestion.catchUpCatalog")(function* (
  workspaceId: string,
) {
  return yield* Effect.forEach(["projects", "users", "labels", "metadata"] as const, (collection) =>
    catchUpStateSource(collection, workspaceId),
  );
});

function decodeStateMessage(
  collection: CatalogCollection,
  workspaceId: string,
  sourceId: string,
  position: string,
  message: CatalogStateMessage,
): Effect.Effect<DecodedStateUpsert, SourcePoison | UnsupportedStateOperation> {
  return Effect.gen(function* () {
    if (!("type" in message)) {
      return yield* poison(
        sourceId,
        position,
        collection,
        undefined,
        `expected a State change message, received control ${message.headers.control}`,
      );
    }
    if (message.type !== catalog[collection].type) {
      return yield* poison(
        sourceId,
        position,
        collection,
        message.key,
        `expected type ${catalog[collection].type}, received ${message.type}`,
      );
    }
    if (message.headers.operation === "delete") {
      return yield* new UnsupportedStateOperation({
        sourceId,
        position,
        collection,
        key: message.key,
        operation: "delete",
      });
    }
    const value = "value" in message ? message.value : undefined;
    if (value === null || value === undefined) {
      return yield* poison(sourceId, position, collection, message.key, "upsert has no value");
    }
    const decoded = yield* Effect.try({
      try: () => catalogRow(collection, value),
      catch: (cause) => poison(sourceId, position, collection, message.key, describe(cause)),
    });
    if (decoded.key !== message.key) {
      return yield* poison(
        sourceId,
        position,
        collection,
        message.key,
        `envelope key differs from row key ${decoded.key}`,
      );
    }
    if (decoded.workspaceId !== workspaceId) {
      return yield* poison(
        sourceId,
        position,
        collection,
        message.key,
        `row belongs to workspace ${decoded.workspaceId}`,
      );
    }
    return { key: message.key, row: decoded.row };
  });
}

function poison(
  sourceId: string,
  position: string,
  collection: string,
  key: string | undefined,
  detail: string,
): SourcePoison {
  const fields = { sourceId, position, collection, detail };
  return key === undefined ? new SourcePoison(fields) : new SourcePoison({ ...fields, key });
}

function describe(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause);
}

export function stateSourceId(collection: CatalogCollection): string {
  switch (collection) {
    case "projects":
      return projects.name;
    case "users":
      return users.name;
    case "labels":
      return labels.name;
    case "metadata":
      return workspaceMetadata.name;
  }
  collection satisfies never;
  throw new TypeError("unknown catalog collection");
}

export function stateSourceBinding(
  bindings: WorkspaceBindings,
  collection: CatalogCollection,
  workspaceId: string,
): StreamBinding {
  switch (collection) {
    case "projects":
      return bindings.projects(workspaceId);
    case "users":
      return bindings.users(workspaceId);
    case "labels":
      return bindings.labels(workspaceId);
    case "metadata":
      return bindings.metadata(workspaceId);
  }
  collection satisfies never;
  throw new TypeError("unknown catalog collection");
}
