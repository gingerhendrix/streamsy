/** Catch-up ingestion for the four application-owned Durable State sources. */
/* oxlint-disable typescript/consistent-return -- Effect requires `return yield*` for a never-succeeding failure branch; successful catch-up branches intentionally return void. */
import type { JsonValue, ReadStreamOptions } from "@streamsy/core";
import type { StreamBinding } from "@streamsy/experimental/binding";
import { ReadStreams } from "@streamsy/experimental/effect";
import { Effect, Schema } from "effect";
import {
  catalog,
  decodeCatalogRow,
  type CatalogCollection,
  type CatalogRow,
} from "../domain/catalog.ts";
import { labels, projects, users, workspaceMetadata } from "../domain/declaration.ts";
import type { Change } from "../views/contracts.ts";
import { SourcePoison, UnsupportedStateOperation } from "./errors.ts";
import { IssueStore } from "./store.ts";
import { Streams, type WorkspaceBindings } from "./streams.ts";

const StateFactType = Schema.String.check(
  Schema.isMinLength(1),
  Schema.makeFilter((value) => !value.startsWith("__streamsy."), {
    description: "an application-owned State collection type",
  }),
);

export const StateFact = Schema.Union([
  Schema.Struct({
    type: StateFactType,
    key: Schema.NonEmptyString,
    value: Schema.Json,
    headers: Schema.Struct({ operation: Schema.Literals(["insert", "update", "upsert"]) }),
  }),
  Schema.Struct({
    type: StateFactType,
    key: Schema.NonEmptyString,
    headers: Schema.Struct({ operation: Schema.Literal("delete") }),
  }),
]);
export type StateFact = typeof StateFact.Type;

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
  const sourceId = stateSourceId(collection);
  const binding = stateSourceBinding(streams.bindings, collection, workspaceId);
  let checkpoint = yield* store.stateCheckpoint(sourceId, workspaceId);
  let folded = 0;
  const changes: Change<CatalogRow>[] = [];

  yield* Effect.scoped(
    Effect.gen(function* () {
      const reads = yield* ReadStreams;
      const options: ReadStreamOptions = { live: false };
      if (checkpoint !== undefined) options.offset = checkpoint;
      const opened = yield* reads.open(binding, options);
      if (opened.status !== "ok") return undefined;

      for (;;) {
        const next = yield* opened.session.next;
        if (next.done === true) break;
        const batch = next.value;
        if (batch.kind !== "json") {
          return yield* new SourcePoison({
            sourceId,
            position: batch.offset,
            collection,
            detail: `expected a json batch, received ${batch.kind}`,
          });
        }

        const upserts: DecodedStateUpsert[] = [];
        for (const value of batch.items) {
          upserts.push(
            yield* decodeStateItem(collection, workspaceId, sourceId, batch.offset, value),
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
          checkpoint: batch.offset,
          rows: result.rows,
        });
        checkpoint = batch.offset;
        folded += batch.items.length;
        changes.push(...result.changes);
        if (batch.upToDate) break;
      }
      return undefined;
    }),
  );

  return { collection, workspaceId, checkpoint, folded, changes } satisfies StateIngestionReport;
});

export const catchUpCatalog = Effect.fn("StateIngestion.catchUpCatalog")(function* (
  workspaceId: string,
) {
  return yield* Effect.forEach(["projects", "users", "labels", "metadata"] as const, (collection) =>
    catchUpStateSource(collection, workspaceId),
  );
});

function decodeStateItem(
  collection: CatalogCollection,
  workspaceId: string,
  sourceId: string,
  position: string,
  value: JsonValue,
): Effect.Effect<DecodedStateUpsert, SourcePoison | UnsupportedStateOperation> {
  return Effect.gen(function* () {
    const fact = yield* Schema.decodeUnknownEffect(StateFact)(value).pipe(
      Effect.mapError((issue) => poison(sourceId, position, collection, undefined, String(issue))),
    );
    if (fact.type !== catalog[collection].type) {
      return yield* poison(
        sourceId,
        position,
        collection,
        fact.key,
        `expected type ${catalog[collection].type}, received ${fact.type}`,
      );
    }
    if (fact.headers.operation === "delete") {
      return yield* new UnsupportedStateOperation({
        sourceId,
        position,
        collection,
        key: fact.key,
        operation: "delete",
      });
    }
    if (!("value" in fact)) {
      return yield* poison(sourceId, position, collection, fact.key, "upsert has no value");
    }
    const decoded = yield* Effect.try({
      try: () => decodeCatalogRow(collection, fact.value),
      catch: (cause) =>
        poison(
          sourceId,
          position,
          collection,
          fact.key,
          cause instanceof Error ? cause.message : String(cause),
        ),
    });
    if (decoded.key !== fact.key) {
      return yield* poison(
        sourceId,
        position,
        collection,
        fact.key,
        `envelope key differs from row key ${decoded.key}`,
      );
    }
    if (decoded.workspaceId !== workspaceId) {
      return yield* poison(
        sourceId,
        position,
        collection,
        fact.key,
        `row belongs to workspace ${decoded.workspaceId}`,
      );
    }
    return { key: fact.key, row: decoded.row };
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
