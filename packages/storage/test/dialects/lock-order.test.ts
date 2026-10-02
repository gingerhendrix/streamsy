/* oxlint-disable effecttsgo/async-function -- Bun owns the database fixtures. */
/**
 * The row lock must be the first statement of every mutation transaction. On
 * MySQL at REPEATABLE READ, a plain read before it fixes a stale snapshot, so
 * storage reads after the lock are locking reads there. A recording tracer
 * captures each SQL statement with its transaction, so the check works on any
 * server dialect. Add a dialect to `dialects` to run it there.
 */
import { describe, expect, test } from "bun:test";
import { Context, Effect, Layer, Option, Tracer } from "effect";
import {
  CommitBoundary,
  Offset,
  ProducerId,
  Storage,
  StreamId,
  ZERO_OFFSET,
  type Mutation,
  type StreamLifecycle,
} from "@streamsy/core";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { storageLayer } from "../../src/storage.ts";
import { dialectClient, freshDatabase, sqlDialects, type SqlDialect } from "./support.ts";

const dialects: ReadonlyArray<SqlDialect> = [sqlDialects.pg, sqlDialects.mysql];

const LOCK =
  /^SELECT stream_id FROM streamsy_streams WHERE stream_id IN \(.*\)\s+ORDER BY stream_id FOR UPDATE$/s;

/** Records every span. Statement spans carry their SQL text as `db.query.text`. */
const recorder = () => {
  const spans: Array<Tracer.NativeSpan> = [];
  const tracer = Tracer.make({
    span: (options) => {
      const span = new Tracer.NativeSpan(options);
      spans.push(span);
      return span;
    },
  });
  /** Statements of each outermost transaction, in the order they ran. */
  const transactions = () => {
    const grouped = new Map<Tracer.AnySpan, Array<string>>();
    for (const span of spans) {
      if (span.name !== "sql.execute") continue;
      let outer: Tracer.AnySpan | undefined;
      for (let parent = span.parent; Option.isSome(parent); ) {
        const value: Tracer.AnySpan = parent.value;
        if (!(value instanceof Tracer.NativeSpan)) break;
        if (value.name === "sql.transaction") outer = value;
        parent = value.parent;
      }
      if (outer === undefined) continue;
      const statements = grouped.get(outer) ?? [];
      statements.push(String(span.attributes.get("db.query.text")));
      grouped.set(outer, statements);
    }
    return [...grouped.values()];
  };
  return { tracer, transactions };
};

const offset = (n: number) => Offset.make(`${String(n).padStart(16, "0")}_${"0".repeat(16)}`);
const record = (id: string, lifecycle: Partial<StreamLifecycle> = {}) => ({
  id: StreamId.make(id),
  config: { contentType: "text/plain", createdAt: 0 },
  lifecycle: { closed: false, softDeleted: false, ...lifecycle },
  currentOffset: ZERO_OFFSET,
});
const message = (n: number) => ({
  offset: offset(n),
  timestamp: n,
  data: new TextEncoder().encode(String(n)),
});

const createParent: Mutation = {
  operations: [{ _tag: "Create", record: record("parent"), initialMessages: [message(1)] }],
};

/** Every mutation shape storage applies, including rejections and a purge chain. */
const workload: ReadonlyArray<Mutation> = [
  createParent,
  {
    operations: [
      {
        _tag: "Append",
        streamId: StreamId.make("parent"),
        expectedOffset: ZERO_OFFSET,
        messages: [message(2)],
        patch: { currentOffset: offset(2) },
        producer: {
          producerId: ProducerId.make("p"),
          expected: Option.none(),
          next: { epoch: 0, lastSeq: 0 },
        },
      },
    ],
  },
  {
    operations: [
      {
        _tag: "Create",
        record: record("child", { forkedFrom: StreamId.make("parent"), forkOffset: offset(2) }),
        initialMessages: [],
        forkSource: { id: StreamId.make("parent"), liveAtOffset: offset(2) },
      },
    ],
  },
  // Rejected: the offset is stale.
  {
    operations: [
      {
        _tag: "Append",
        streamId: StreamId.make("parent"),
        expectedOffset: ZERO_OFFSET,
        messages: [message(3)],
        patch: { currentOffset: offset(3) },
      },
    ],
  },
  // Rejected: the stream exists.
  { operations: [{ _tag: "Create", record: record("parent"), initialMessages: [] }] },
  // Soft delete: the child depends on the parent.
  { operations: [{ _tag: "Delete", streamId: StreamId.make("parent"), reason: "delete" }] },
  // Purge the child, then the parent through the chain.
  { operations: [{ _tag: "Delete", streamId: StreamId.make("child"), reason: "delete" }] },
  {
    operations: [
      { _tag: "Create", record: record("a"), initialMessages: [] },
      { _tag: "Create", record: record("b"), initialMessages: [] },
    ],
  },
];

for (const dialect of dialects) {
  const url = dialect.url;
  describe.skipIf(url === undefined)(`${dialect.name} lock order`, () => {
    test("the row lock is the first statement of every mutation transaction", async () => {
      if (url === undefined) return;
      const { tracer, transactions } = recorder();
      const result = await Effect.runPromise(
        freshDatabase(dialect, url).pipe(
          Effect.flatMap((fresh) =>
            Effect.gen(function* () {
              const context = yield* Layer.build(
                storageLayer().pipe(Layer.provideMerge(dialectClient(dialect, fresh))),
              );
              const storage = Context.get(context, Storage);
              const outcomes: Array<string> = [];
              yield* Effect.gen(function* () {
                for (const mutation of workload)
                  outcomes.push(
                    yield* storage.mutate(mutation).pipe(
                      Effect.map(({ results }) => results.map(({ _tag }) => _tag).join("+")),
                      Effect.catchTag("MutationRejected", ({ reason }) =>
                        Effect.succeed(`rejected:${reason}`),
                      ),
                    ),
                  );
              }).pipe(Effect.withTracer(tracer));
              return outcomes;
            }).pipe(Effect.scoped),
          ),
          Effect.scoped,
        ),
      );
      expect(result).toEqual([
        "Created",
        "Appended",
        "Created",
        "rejected:offset",
        "rejected:exists",
        "SoftDeleted",
        "Purged",
        "Created+Created",
      ]);
      const recorded = transactions();
      expect(recorded.length).toBe(workload.length);
      expect(recorded.filter((statements) => !LOCK.test(statements[0] ?? ""))).toEqual([]);
      // The purge of `child` locks `parent` before it checks for other dependents.
      const purge = recorded[6] ?? [];
      const parentLock = purge.findIndex((statement, index) => index > 0 && LOCK.test(statement));
      const dependents = purge.findLastIndex((statement) => statement.includes("forked_from ="));
      expect(parentLock).toBeGreaterThan(0);
      expect(dependents).toBeGreaterThan(parentLock);
    });

    test("a fused commit that reads before its mutation is caught by the check", async () => {
      if (url === undefined) return;
      const { tracer, transactions } = recorder();
      await Effect.runPromise(
        freshDatabase(dialect, url).pipe(
          Effect.flatMap((fresh) =>
            Effect.gen(function* () {
              const context = yield* Layer.build(
                storageLayer().pipe(Layer.provideMerge(dialectClient(dialect, fresh))),
              );
              const storage = Context.get(context, Storage);
              const boundary = Context.get(context, CommitBoundary);
              const sql = Context.get(context, SqlClient.SqlClient);
              // App SQL runs first in the owner transaction, so the storage lock is second.
              yield* boundary
                .withTransaction(
                  Effect.gen(function* () {
                    yield* sql`SELECT COUNT(*) AS count FROM streamsy_streams`;
                    yield* storage.mutate(createParent);
                  }),
                )
                .pipe(Effect.withTracer(tracer));
            }).pipe(Effect.scoped),
          ),
          Effect.scoped,
        ),
      );
      const recorded = transactions();
      expect(recorded.length).toBe(1);
      expect(LOCK.test(recorded[0]?.[0] ?? "")).toBe(false);
      expect(LOCK.test(recorded[0]?.[1] ?? "")).toBe(true);
    });

    test("a fused commit that reads first still sees commits made after its read", async () => {
      if (url === undefined) return;
      const stream = StreamId.make("stream");
      const append = (expectedOffset: Offset, n: number): Mutation => ({
        operations: [
          {
            _tag: "Append",
            streamId: stream,
            expectedOffset,
            messages: [message(n)],
            patch: { currentOffset: offset(n) },
          },
        ],
      });
      const result = await Effect.runPromise(
        freshDatabase(dialect, url).pipe(
          Effect.flatMap((fresh) =>
            Effect.gen(function* () {
              const open = Layer.build(
                storageLayer().pipe(Layer.provideMerge(dialectClient(dialect, fresh))),
              );
              const context = yield* open;
              const storage = Context.get(context, Storage);
              const boundary = Context.get(context, CommitBoundary);
              const sql = Context.get(context, SqlClient.SqlClient);
              // A second pool stands in for another process.
              const other = Context.get(yield* open, Storage);
              yield* other.mutate({
                operations: [{ _tag: "Create", record: record("stream"), initialMessages: [] }],
              });
              yield* other.mutate(createParent);
              yield* other.mutate({
                operations: [
                  {
                    _tag: "Create",
                    record: record("child", { forkedFrom: StreamId.make("parent") }),
                    initialMessages: [],
                    forkSource: { id: StreamId.make("parent"), liveAtOffset: ZERO_OFFSET },
                  },
                ],
              });
              const outcome = (mutation: Mutation) =>
                storage.mutate(mutation).pipe(
                  Effect.map(({ results }) => results.map(({ _tag }) => _tag).join("+")),
                  Effect.catchTag("MutationRejected", ({ reason }) =>
                    Effect.succeed(`rejected:${reason}`),
                  ),
                );
              const outcomes = yield* boundary.withTransaction(
                Effect.gen(function* () {
                  // On MySQL this plain read fixes the transaction's snapshot.
                  yield* sql`SELECT COUNT(*) AS count FROM streamsy_streams`;
                  // Another process appends and removes the only child after that read.
                  yield* other.mutate(append(ZERO_OFFSET, 1));
                  yield* other.mutate({
                    operations: [
                      { _tag: "Delete", streamId: StreamId.make("child"), reason: "delete" },
                    ],
                  });
                  return [
                    // Planned from the old tail, so storage must reject it.
                    yield* outcome(append(ZERO_OFFSET, 1)),
                    yield* outcome(append(offset(1), 2)),
                    // The child is gone, so the parent has no dependents left.
                    yield* outcome({
                      operations: [
                        { _tag: "Delete", streamId: StreamId.make("parent"), reason: "delete" },
                      ],
                    }),
                  ];
                }),
              );
              return {
                outcomes,
                messages: (yield* other.messages(stream, {})).map((stored) => stored.offset),
                parent: Option.isSome(yield* other.record(StreamId.make("parent"))),
              };
            }).pipe(Effect.scoped),
          ),
          Effect.scoped,
        ),
      );
      expect(result).toEqual({
        outcomes: ["rejected:offset", "Appended", "Purged"],
        messages: [offset(1), offset(2)],
        parent: false,
      });
    });
  });
}
