import { describe, expect, test } from "bun:test";
import { Effect, Exit } from "effect";
import type { MaintenanceCommit, NamespaceRef, StoreError, ViewStoreService } from "./contracts.ts";

export interface ConformanceBackend {
  readonly store: ViewStoreService;
  readonly restart: () => Promise<ConformanceBackend>;
  readonly close: () => Promise<void>;
}
export type ConformanceFactory = () => Promise<ConformanceBackend>;
interface CurrentBackend {
  backend?: ConformanceBackend;
}
const identity = {
  planName: "conformance",
  planHash: "hash-1",
  partition: "p1",
  sourceId: "source",
} as const;
const relation: NamespaceRef = { ...identity, id: "rows" };
const operator: NamespaceRef = { ...identity, id: "operator" };
const reducer: NamespaceRef = { ...identity, id: "reducer" };
const commit = (
  cursor: string,
  expectedCursor: string | undefined,
  suffix: Partial<MaintenanceCommit> = {},
): MaintenanceCommit => ({
  identity,
  expectedCursor,
  afterExclusiveCursor: cursor,
  batchId: `batch-${cursor}`,
  committedAtMs: Number(cursor),
  ...suffix,
});

export function viewStoreConformance(name: string, factory: ConformanceFactory): void {
  describe(`${name} ViewStore conformance`, () => {
    test("atomically maintains every surface in deterministic order", () =>
      withBackend(factory, (backend) =>
        Effect.gen(function* () {
          yield* backend.store.commit(
            commit("1", undefined, {
              rows: [
                { kind: "put", namespace: relation, key: ["b", 2], value: { n: 2 } },
                { kind: "put", namespace: relation, key: ["a", 1], value: { n: 1 } },
              ],
              operatorValues: [
                { kind: "put", namespace: operator, key: "private", value: { sum: 3 } },
              ],
              operatorIndexes: [
                {
                  kind: "put",
                  namespace: operator,
                  indexName: "members",
                  indexKey: "group",
                  sortKey: 2,
                  rowKey: "b",
                },
                {
                  kind: "put",
                  namespace: operator,
                  indexName: "members",
                  indexKey: "group",
                  sortKey: 1,
                  rowKey: "a",
                  value: "cover",
                },
              ],
              reducerStates: [
                { kind: "put", namespace: reducer, key: "a", value: { hidden: true } },
              ],
              changes: [{ kind: "enter", relationId: relation.id, key: ["a", 1], after: { n: 1 } }],
            }),
          );
          expect((yield* backend.store.snapshotRows(relation)).rows.map((row) => row.key)).toEqual([
            ["a", 1],
            ["b", 2],
          ]);
          expect(yield* backend.store.getOperatorValue(operator, "private")).toEqual({ sum: 3 });
          expect(
            (yield* backend.store.lookupIndex(operator, "members", "group")).map(
              (row) => row.rowKey,
            ),
          ).toEqual(["a", "b"]);
          expect(yield* backend.store.getReducerState(reducer, "a")).toEqual({ hidden: true });
          expect(
            (yield* backend.store.changesAfter(identity, undefined, 10))[0]?.changes,
          ).toHaveLength(1);
        }),
      ));
    test("rejects a stale cursor without partial writes and accepts idempotent replay", () =>
      withBackend(factory, (backend) =>
        Effect.gen(function* () {
          const first = commit("1", undefined, {
            rows: [{ kind: "put", namespace: relation, key: "a", value: 1 }],
          });
          yield* backend.store.commit(first);
          const duplicate = yield* backend.store.commit(first);
          expect(duplicate.sequence).toBe(1);
          const mismatchedReplay = yield* Effect.exit(
            backend.store.commit({ ...first, batchId: "different-batch" }),
          );
          expect(Exit.isFailure(mismatchedReplay)).toBe(true);
          const exit = yield* Effect.exit(
            backend.store.commit(
              commit("2", undefined, {
                rows: [{ kind: "put", namespace: relation, key: "b", value: 2 }],
              }),
            ),
          );
          expect(Exit.isFailure(exit)).toBe(true);
          expect(yield* backend.store.getRow(relation, "b")).toBeUndefined();
        }),
      ));
    test("prunes complete batches and reports an expired position", () =>
      withBackend(factory, (backend) =>
        Effect.gen(function* () {
          yield* backend.store.commit(
            commit("1", undefined, {
              changes: [{ kind: "enter", relationId: relation.id, key: "a", after: 1 }],
            }),
            { keepLastBatches: 2 },
          );
          yield* backend.store.commit(
            commit("2", "1", {
              changes: [{ kind: "update", relationId: relation.id, key: "a", before: 1, after: 2 }],
            }),
            { keepLastBatches: 2 },
          );
          yield* backend.store.commit(
            commit("3", "2", {
              changes: [{ kind: "exit", relationId: relation.id, key: "a", before: 2 }],
            }),
            { keepLastBatches: 2 },
          );
          expect(yield* backend.store.historyBounds(identity)).toEqual({
            epoch: 1,
            first: 2,
            latest: 3,
          });
          const expired = yield* Effect.exit(
            backend.store.changesAfter(identity, { epoch: 1, sequence: 0 }, 10),
          );
          expect(Exit.isFailure(expired)).toBe(true);
        }),
      ));
    test("activates compatible checkpoint generations and survives restart", () => {
      const current: CurrentBackend = {};
      return Effect.runPromise(
        Effect.gen(function* () {
          let backend = yield* Effect.promise(factory);
          current.backend = backend;
          yield* backend.store.saveCheckpoint({
            ...identity,
            reducerId: "r",
            reducerVersion: 1,
            sourceCursor: "1",
            createdAtMs: 1,
            entries: [{ key: "a", value: { n: 1 } }],
          });
          yield* backend.store.saveCheckpoint({
            ...identity,
            reducerId: "r",
            reducerVersion: 1,
            sourceCursor: "2",
            createdAtMs: 2,
            entries: [{ key: "a", value: { n: 2 } }],
          });
          backend = yield* Effect.promise(backend.restart);
          current.backend = backend;
          const loaded = yield* backend.store.loadCheckpoint({
            ...identity,
            reducerId: "r",
            reducerVersion: 1,
          });
          expect(loaded?.generation).toBe(2);
          expect(loaded?.entries[0]?.value).toEqual({ n: 2 });
          const incompatible = yield* Effect.exit(
            backend.store.loadCheckpoint({ ...identity, reducerId: "r", reducerVersion: 2 }),
          );
          expect(Exit.isFailure(incompatible)).toBe(true);
        }).pipe(
          Effect.ensuring(
            Effect.suspend(() =>
              current.backend === undefined ? Effect.void : Effect.promise(current.backend.close),
            ),
          ),
        ),
      );
    });
  });
}

function withBackend(
  factory: ConformanceFactory,
  use: (backend: ConformanceBackend) => Effect.Effect<void, StoreError>,
): Promise<void> {
  return Effect.runPromise(
    Effect.acquireUseRelease(Effect.promise(factory), use, (backend) =>
      Effect.promise(backend.close),
    ),
  );
}
