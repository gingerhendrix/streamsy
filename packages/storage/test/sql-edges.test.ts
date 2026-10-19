/* oxlint-disable effecttsgo/async-function, effecttsgo/crypto-random-uuid -- Bun owns retained SQLite edge fixtures. */
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { expect, test } from "vitest";
import {
  Array,
  Cause,
  Config,
  Context,
  Effect,
  Exit,
  Layer,
  Option,
  Order,
  Predicate,
} from "effect";
import {
  Offset,
  ProducerId,
  Storage,
  StreamId,
  ZERO_OFFSET,
  type StreamRecord,
} from "@streamsy/core";
import { SqlClient } from "effect/sql";
import { sharedSqlClientLayer } from "../src/boundary.ts";
import { storageLayer as sqlLayer } from "../src/storage.ts";

const scratch = Effect.runSync(
  Config.String("STREAMSY_STORAGE_SCRATCH").pipe(Config.withDefault("/tmp")),
);

let nextFixture = 0;

const died = <A, E>(exit: Exit.Exit<A, E>): boolean =>
  Exit.isFailure(exit) && Cause.hasDies(exit.cause);

interface FixtureLifecycle {
  closed: boolean;
  softDeleted: boolean;
  expiresAtMs?: number;
}

const record = (id: string, expiresAtMs?: number): StreamRecord => {
  const lifecycle: FixtureLifecycle = { closed: false, softDeleted: false };

  if (expiresAtMs !== undefined) lifecycle.expiresAtMs = expiresAtMs;

  return {
    id: StreamId.make(id),
    config: { contentType: "text/plain", createdAt: 0 },
    lifecycle,
    currentOffset: ZERO_OFFSET,
  };
};

const withStore = <A, E>(
  label: string,
  use: (storage: ReturnType<typeof Storage.of>, sql: SqlClient.SqlClient) => Effect.Effect<A, E>,
) =>
  Effect.gen(function* () {
    const filename = `${scratch}/${label}-${process.pid}-${nextFixture++}.sqlite`;

    const client = Layer.effectContext(
      sharedSqlClientLayer(SqliteClient.make({ filename, busyTimeout: "50 millis" })),
    );

    const context = yield* Layer.build(
      sqlLayer({ repairIntervalMs: 1_000 }).pipe(Layer.provideMerge(client)),
    );

    return yield* use(Context.get(context, Storage), Context.get(context, SqlClient.SqlClient));
  }).pipe(Effect.scoped);

test("persisted record, producer, and expiry corruption defects at decoded edges", async () => {
  const result = await Effect.runPromise(
    withStore("corruption", (storage, sql) =>
      Effect.gen(function* () {
        yield* storage.mutate({
          operations: [
            {
              _tag: "Create",
              record: record("bad", 10),
              initialMessages: [],
            },
          ],
        });
        yield* storage.mutate({
          operations: [
            {
              _tag: "Append",
              streamId: StreamId.make("bad"),
              messages: [],
              patch: {},
              producer: {
                producerId: ProducerId.make("p"),
                expected: Option.none(),
                next: { epoch: 1, lastSeq: 1 },
              },
            },
          ],
        });
        yield* sql.unsafe(
          "UPDATE streamsy_streams SET current_offset='invalid' WHERE stream_id='bad'",
        );
        const badRecord = yield* storage.record(StreamId.make("bad")).pipe(Effect.exit);
        yield* sql.unsafe(
          "UPDATE streamsy_streams SET current_offset='0000000000000000_0000000000000000' WHERE stream_id='bad'",
        );
        yield* sql.unsafe(
          "INSERT INTO streamsy_messages VALUES ('bad','0000000000000001_0000000000000000','invalid',X'01')",
        );
        const badMessage = yield* storage.messages(StreamId.make("bad"), {}).pipe(Effect.exit);
        yield* sql.unsafe("DELETE FROM streamsy_messages WHERE stream_id='bad'");
        yield* sql.unsafe("UPDATE streamsy_producers SET epoch='invalid' WHERE stream_id='bad'");

        const badProducer = yield* storage
          .producer(StreamId.make("bad"), ProducerId.make("p"))
          .pipe(Effect.exit);

        yield* sql.unsafe("DELETE FROM streamsy_producers WHERE stream_id='bad'");
        yield* sql.unsafe(
          "UPDATE streamsy_streams SET expires_at_ms='invalid' WHERE stream_id='bad'",
        );
        const badExpiry = yield* storage.nextExpiry.pipe(Effect.exit);

        return [died(badRecord), died(badMessage), died(badProducer), died(badExpiry)];
      }),
    ),
  );

  expect(result).toEqual([true, true, true, true]);
});

test("message SQL excludes rows outside the window and performs no row read for limit zero", async () => {
  const result = await Effect.runPromise(
    withStore("bounded-message", (storage, sql) =>
      Effect.gen(function* () {
        const one = Offset.make("0000000000000001_0000000000000000");
        const two = Offset.make("0000000000000002_0000000000000000");
        yield* storage.mutate({
          operations: [
            {
              _tag: "Create",
              record: record("bounded"),
              initialMessages: [
                { offset: one, timestamp: 1, data: new TextEncoder().encode("included") },
              ],
            },
          ],
        });
        yield* sql.unsafe("INSERT INTO streamsy_messages VALUES ('bounded',?,'invalid',X'01')", [
          two,
        ]);
        const included = yield* storage.messages(StreamId.make("bounded"), { until: one });
        const zero = yield* storage.messages(StreamId.make("bounded"), { limit: 0 });
        const full = yield* storage.messages(StreamId.make("bounded"), {}).pipe(Effect.exit);

        return {
          included: Array.map(included, ({ offset }) => offset),
          zero,
          fullDefected: died(full),
        };
      }),
    ),
  );

  expect(result).toEqual({
    included: [Offset.make("0000000000000001_0000000000000000")],
    zero: [],
    fullDefected: true,
  });
});

test("operational SQL failures map to StorageFault while concurrent producer CAS has one winner", async () => {
  const result = await Effect.runPromise(
    withStore("fault-race", (storage, sql) =>
      Effect.gen(function* () {
        yield* storage.mutate({
          operations: [{ _tag: "Create", record: record("race"), initialMessages: [] }],
        });
        yield* storage.mutate({
          operations: [
            {
              _tag: "Append",
              streamId: StreamId.make("race"),
              messages: [],
              patch: {},
              producer: {
                producerId: ProducerId.make("p"),
                expected: Option.none(),
                next: { epoch: 0, lastSeq: 0 },
              },
            },
          ],
        });

        const write = (lastSeq: number) =>
          storage.mutate({
            operations: [
              {
                _tag: "Append",
                streamId: StreamId.make("race"),
                messages: [],
                patch: {},
                producer: {
                  producerId: ProducerId.make("p"),
                  expected: Option.some({ epoch: 0, lastSeq: 0 }),
                  next: { epoch: 0, lastSeq },
                },
              },
            ],
          });

        const outcomes = yield* Effect.forEach(
          [write(1), write(2)],
          (effect) => effect.pipe(Effect.catchTag("MutationRejected", Effect.succeed)),
          { concurrency: 2 },
        );

        yield* sql.unsafe("DROP TABLE streamsy_streams");
        const fault = yield* storage.record(StreamId.make("race")).pipe(Effect.exit);

        return {
          tags: Array.sort(
            Array.map(outcomes, ({ _tag }) => _tag),
            Order.String,
          ),
          typedFault: Exit.isFailure(fault)
            ? !Cause.hasDies(fault.cause) &&
              Option.match(Cause.findErrorOption(fault.cause), {
                onNone: () => false,
                onSome: Predicate.isTagged("StorageFault"),
              })
            : false,
        };
      }),
    ),
  );

  expect(result).toEqual({ tags: ["Applied", "MutationRejected"], typedFault: true });
});

test("stream ids longer than 512 characters fail before any insert", async () => {
  const result = await Effect.runPromise(
    withStore("stream-id-limit", (storage, sql) => {
      const create = (id: string) =>
        storage.mutate({
          operations: [{ _tag: "Create", record: record(id), initialMessages: [] }],
        });

      return Effect.gen(function* () {
        // 512 code points pass, even when they take 1024 UTF-16 units.
        yield* create("a".repeat(512));
        yield* create("\u{1F600}".repeat(512));
        const tooLong = yield* create("b".repeat(513)).pipe(Effect.flip);

        const rows = yield* sql.unsafe<{ readonly count: number }>(
          "SELECT COUNT(*) count FROM streamsy_streams",
        );

        return {
          fault: Predicate.isTagged(tooLong, "StorageFault")
            ? { operation: tooLong.operation, retryable: tooLong.retryable }
            : tooLong,
          count: rows[0]?.count,
        };
      });
    }),
  );

  expect(result).toEqual({
    fault: { operation: "mutate.stream-id", retryable: false },
    count: 2,
  });
});

test("producer ids longer than 255 characters fail before any write", async () => {
  const result = await Effect.runPromise(
    withStore("producer-id-limit", (storage, sql) =>
      Effect.gen(function* () {
        yield* storage.mutate({
          operations: [{ _tag: "Create", record: record("producers"), initialMessages: [] }],
        });
        const one = Offset.make("0000000000000001_0000000000000000");

        const write = (producerId: string) =>
          storage.mutate({
            operations: [
              {
                _tag: "Append",
                streamId: StreamId.make("producers"),
                messages: [{ offset: one, timestamp: 1, data: new TextEncoder().encode("x") }],
                patch: { currentOffset: one },
                producer: {
                  producerId: ProducerId.make(producerId),
                  expected: Option.none(),
                  next: { epoch: 0, lastSeq: 0 },
                },
              },
            ],
          });

        const tooLong = yield* write("p".repeat(256)).pipe(Effect.flip);

        const producers = yield* sql.unsafe<{ readonly count: number }>(
          "SELECT COUNT(*) count FROM streamsy_producers",
        );

        const messages = yield* sql.unsafe<{ readonly count: number }>(
          "SELECT COUNT(*) count FROM streamsy_messages",
        );

        // 255 code points pass, even when they take 510 UTF-16 units.
        yield* write("\u{1F600}".repeat(255));

        return {
          fault: Predicate.isTagged(tooLong, "StorageFault")
            ? { operation: tooLong.operation, retryable: tooLong.retryable }
            : tooLong,
          before: [producers[0]?.count, messages[0]?.count],
          after: Option.getOrThrow(yield* storage.record(StreamId.make("producers"))).currentOffset,
        };
      }),
    ),
  );

  expect(result).toEqual({
    fault: { operation: "mutate.producer-id", retryable: false },
    before: [0, 0],
    after: Offset.make("0000000000000001_0000000000000000"),
  });
});
