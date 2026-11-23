/* oxlint-disable effecttsgo/async-function, effecttsgo/global-console -- Bun owns the database fixtures and prints the race counts. */
/**
 * Writers on two connection pools race on one database. These cases are the
 * proof for the row lock: without it, Postgres and MySQL commit two branches of
 * one stream, purge a fork source under a new child, or leave a parent behind.
 * Add a dialect to `dialects` to run the same races on it.
 */
import { describe, expect, test } from "vitest";
import {
  Array,
  Context,
  Effect,
  Layer,
  Option,
  Order,
  Predicate,
  Result,
  type Scope,
} from "effect";
import { CommitBoundary, Storage, StreamId, ZERO_OFFSET, type Mutation } from "@streamsy/core";
import { Protocol, StreamsWriter } from "@streamsy/core/protocol";
import { SqlClient } from "effect/sql";
import { makeBoundaryTestProbe } from "../../src/boundary.ts";
import { layerWithTestProbe } from "../../src/storage.ts";
import { dialectClient, freshDatabase, sqlDialects, type SqlDialect } from "./support.ts";

const dialects: ReadonlyArray<SqlDialect> = [sqlDialects.pg, sqlDialects.mysql];

const APPENDS = 200;

const POOL_SIZE = 20;

const RACES = 20;

const TIMEOUT_MS = 120_000;

const contentType = "application/octet-stream";

/** One host: protocol and storage over its own pool. The probe counts transactions. */
const host = (dialect: SqlDialect, url: string) => {
  const probe = makeBoundaryTestProbe();

  return Layer.build(
    Protocol.layer().pipe(
      Layer.provideMerge(layerWithTestProbe({}, probe)),
      Layer.provideMerge(dialectClient(dialect, url, { maxConnections: POOL_SIZE })),
    ),
  ).pipe(
    Effect.map((context) => ({
      writer: Context.get(context, StreamsWriter),
      storage: Context.get(context, Storage),
      sql: Context.get(context, SqlClient.SqlClient),
      boundary: Context.get(context, CommitBoundary),
      probe,
    })),
  );
};

type Host = Effect.Success<ReturnType<typeof host>>;

/** Two hosts on one fresh database, as two processes would be. */
const withHosts = <A, E>(
  dialect: SqlDialect,
  baseUrl: string,
  use: (left: Host, right: Host) => Effect.Effect<A, E, Scope.Scope>,
) =>
  freshDatabase(dialect, baseUrl).pipe(
    Effect.flatMap((url) =>
      Effect.gen(function* () {
        // Build one host first, so the second finds the schema migrated.
        const left = yield* host(dialect, url);
        const right = yield* host(dialect, url);

        return yield* use(left, right);
      }).pipe(Effect.scoped),
    ),
    Effect.scoped,
  );

/** The offset of the n-th message: one octet-stream append writes one message. */
const nthOffset = (n: number) => `${String(n).padStart(16, "0")}_${"0".repeat(16)}`;

/** "ok", or the tag of the failure. */
const tagOf = <A, E extends { readonly _tag: string }>(result: Result.Result<A, E>) =>
  Result.match(result, { onSuccess: () => "ok", onFailure: ({ _tag: tag }) => tag });

/** A storage-level create of an empty stream. */
const create = (id: string): Mutation => ({
  operations: [
    {
      _tag: "Create",
      record: {
        id: StreamId.make(id),
        config: { contentType, createdAt: 0 },
        lifecycle: { closed: false, softDeleted: false },
        currentOffset: ZERO_OFFSET,
      },
      initialMessages: [],
    },
  ],
});

/** The applied tag, or the rejection reason. */
const mutateTag = (storage: Host["storage"], mutation: Mutation) =>
  storage.mutate(mutation).pipe(
    Effect.map(({ _tag }) => _tag),
    Effect.catchTag("MutationRejected", ({ reason }) => Effect.succeed(`rejected:${reason}`)),
  );

const text = (value: string) => new TextEncoder().encode(value);

const payload = (index: number) => new TextEncoder().encode(`${index}:${"x".repeat(index % 7)}`);

for (const dialect of dialects) {
  const url = dialect.url;
  describe.skipIf(url === undefined)(`${dialect.name} writer races`, () => {
    test(
      `${APPENDS} parallel appends from two pools give one gap-free stream`,
      async () => {
        if (url === undefined) return;
        const id = StreamId.make("race/appends");

        const result = await Effect.runPromise(
          withHosts(dialect, url, (left, right) =>
            Effect.gen(function* () {
              yield* left.writer.create(id, { contentType });
              let busy = 0;

              // A writer that loses every replan gets StreamBusy; a client retries.
              const append = (writer: Host["writer"], index: number) =>
                writer.append(id, { data: payload(index), contentType }).pipe(
                  Effect.tapError((error) =>
                    Effect.sync(() => {
                      if (Predicate.isTagged(error, "StreamBusy")) busy += 1;
                    }),
                  ),
                  Effect.retry({ while: Predicate.isTagged("StreamBusy") }),
                );

              const indexes = globalThis.Array.from({ length: APPENDS }, (_, index) => index);
              yield* Effect.all(
                [
                  Effect.forEach(
                    Array.filter(indexes, (index) => index % 2 === 0),
                    (index) => append(left.writer, index),
                    { concurrency: POOL_SIZE, discard: true },
                  ),
                  Effect.forEach(
                    Array.filter(indexes, (index) => index % 2 === 1),
                    (index) => append(right.writer, index),
                    { concurrency: POOL_SIZE, discard: true },
                  ),
                ],
                { concurrency: 2, discard: true },
              );
              const messages = yield* left.storage.messages(id, {});
              const record = Option.getOrThrow(yield* left.storage.record(id));

              const gapFree = Array.every(
                messages,
                ({ offset }, index) => offset === nthOffset(index + 1),
              );

              const decoder = new TextDecoder();

              return {
                count: messages.length,
                unique: new Set(Array.map(messages, ({ offset }) => offset)).size,
                gapFree,
                tail: record.currentOffset === messages.at(-1)?.offset,
                bytes: Array.reduce(messages, 0, (total, { data }) => total + data.byteLength),
                payloads: Array.sort(
                  Array.map(messages, ({ data }) => decoder.decode(data)),
                  Order.String,
                ),
                busy,
              };
            }),
          ),
        );

        console.log(`${dialect.name} appends: ${APPENDS}, StreamBusy retries: ${result.busy}`);
        const sent = globalThis.Array.from({ length: APPENDS }, (_, index) => payload(index));
        expect({ ...result, busy: undefined }).toEqual({
          count: APPENDS,
          unique: APPENDS,
          gapFree: true,
          tail: true,
          bytes: Array.reduce(sent, 0, (total, data) => total + data.byteLength),
          payloads: Array.sort(
            Array.map(sent, (data) => new TextDecoder().decode(data)),
            Order.String,
          ),
          busy: undefined,
        });
      },
      TIMEOUT_MS,
    );

    test(
      "a fused append after a plain read wins over an append from another pool",
      async () => {
        if (url === undefined) return;
        const id = StreamId.make("race/fused");

        const result = await Effect.runPromise(
          withHosts(dialect, url, (left, right) =>
            Effect.gen(function* () {
              yield* right.writer.create(id, { contentType });

              const outcome = yield* left.boundary
                .withTransaction(
                  Effect.gen(function* () {
                    // On MySQL this plain read fixes the transaction's snapshot.
                    yield* left.sql`SELECT COUNT(*) AS count FROM streamsy_streams`;
                    // Another process appends after that read.
                    yield* right.writer.append(id, { data: text("other"), contentType });

                    return yield* left.writer.append(id, { data: text("fused"), contentType });
                  }),
                )
                .pipe(Effect.result);

              const messages = yield* right.storage.messages(id, {});
              const record = Option.getOrThrow(yield* right.storage.record(id));
              const decoder = new TextDecoder();

              return {
                outcome: tagOf(outcome),
                offsets: Array.map(messages, ({ offset }) => String(offset)),
                payloads: Array.map(messages, ({ data }) => decoder.decode(data)),
                tail: String(record.currentOffset),
              };
            }),
          ),
        );

        expect(result).toEqual({
          outcome: "ok",
          offsets: [nthOffset(1), nthOffset(2)],
          payloads: ["other", "fused"],
          tail: nthOffset(2),
        });
      },
      TIMEOUT_MS,
    );

    test(
      "a fork racing the delete of its source never leaves an orphan child",
      async () => {
        if (url === undefined) return;

        const result = await Effect.runPromise(
          withHosts(dialect, url, (left, right) =>
            Effect.gen(function* () {
              const outcomes: globalThis.Array<string> = [];

              for (let race = 0; race < RACES; race++) {
                const parent = StreamId.make(`fork-race/${race}/parent`);
                const child = StreamId.make(`fork-race/${race}/child`);
                yield* left.writer.create(parent, { contentType });
                yield* left.writer.append(parent, { data: payload(race), contentType });

                const [forked, removed] = yield* Effect.all(
                  [
                    left.writer.fork(child, parent).pipe(Effect.result),
                    right.writer.remove(parent).pipe(Effect.result),
                  ],
                  { concurrency: 2 },
                );

                outcomes.push(`fork:${tagOf(forked)} remove:${tagOf(removed)}`);
              }

              // Every child must still reach its source row.
              const orphans = yield* left.sql<{ readonly stream_id: string }>`
SELECT c.stream_id FROM streamsy_streams c
 LEFT JOIN streamsy_streams p ON p.stream_id = c.forked_from
 WHERE c.forked_from IS NOT NULL AND p.stream_id IS NULL`;

              return { outcomes, orphans: Array.map(orphans, ({ stream_id }) => stream_id) };
            }),
          ),
        );

        console.log(
          `${dialect.name} fork-and-delete outcomes: ${Array.join(result.outcomes, ", ")}`,
        );
        expect(result.orphans).toEqual([]);
        // Either the fork wins and the delete soft-deletes the source, or the delete wins
        // and the fork finds no source.
        expect(
          Array.every(result.outcomes, (outcome) =>
            ["fork:ok remove:ok", "fork:ForkSourceNotFound remove:ok"].includes(outcome),
          ),
        ).toBe(true);
      },
      TIMEOUT_MS,
    );

    test(
      "two children purging at once remove their soft-deleted parent",
      async () => {
        if (url === undefined) return;

        const result = await Effect.runPromise(
          withHosts(dialect, url, (left, right) =>
            Effect.gen(function* () {
              const kept: globalThis.Array<string> = [];

              for (let race = 0; race < RACES; race++) {
                const parent = StreamId.make(`purge-race/${race}/parent`);
                const first = StreamId.make(`purge-race/${race}/a`);
                const second = StreamId.make(`purge-race/${race}/b`);
                yield* left.writer.create(parent, { contentType });
                yield* left.writer.fork(first, parent);
                yield* left.writer.fork(second, parent);
                // The parent has dependents, so this only soft-deletes it.
                yield* left.writer.remove(parent);
                yield* Effect.all([left.writer.remove(first), right.writer.remove(second)], {
                  concurrency: 2,
                  discard: true,
                });

                if (Option.isSome(yield* left.storage.record(parent))) kept.push(parent);
              }

              const rows = yield* left.sql<{
                readonly count: unknown;
              }>`SELECT COUNT(*) AS count FROM streamsy_streams`;

              return { kept, rows: Number(rows[0]?.count) };
            }),
          ),
        );

        expect(result).toEqual({ kept: [], rows: 0 });
      },
      TIMEOUT_MS,
    );

    test(
      "concurrent creates give one stream per id and a clean exists rejection",
      async () => {
        if (url === undefined) return;

        const result = await Effect.runPromise(
          withHosts(dialect, url, (left, right) =>
            Effect.gen(function* () {
              const same: globalThis.Array<string> = [];
              const distinct: globalThis.Array<string> = [];

              for (let race = 0; race < RACES; race++) {
                // On MySQL, the lock on an id that does not exist yet is a gap lock.
                // Two creates in one gap can deadlock; the boundary retries them.
                const [a, b, c, d] = yield* Effect.all(
                  [
                    mutateTag(left.storage, create(`create-race/${race}/same`)),
                    mutateTag(right.storage, create(`create-race/${race}/same`)),
                    mutateTag(left.storage, create(`create-race/${race}/left`)),
                    mutateTag(right.storage, create(`create-race/${race}/right`)),
                  ],
                  { concurrency: 4 },
                );

                same.push(Array.join(Array.sort([a, b], Order.String), " "));
                distinct.push(`${c} ${d}`);
              }

              const retries = Array.reduce(
                [left.probe, right.probe],
                0,
                (total, probe) => total + probe.transactionAttempts - probe.completedMutations,
              );

              const rows = yield* left.sql<{
                readonly count: unknown;
              }>`SELECT COUNT(*) AS count FROM streamsy_streams`;

              return { same, distinct, retries, rows: Number(rows[0]?.count) };
            }),
          ),
        );

        console.log(
          `${dialect.name} create races: ${RACES * 4}, transaction retries: ${result.retries}`,
        );
        expect({ ...result, retries: undefined }).toEqual({
          same: globalThis.Array.from({ length: RACES }, () => "Applied rejected:exists"),
          distinct: globalThis.Array.from({ length: RACES }, () => "Applied Applied"),
          retries: undefined,
          rows: RACES * 3,
        });
      },
      TIMEOUT_MS,
    );
  });
}
