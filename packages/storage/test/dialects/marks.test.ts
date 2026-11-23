/* oxlint-disable effecttsgo/async-function -- Bun owns the database fixtures. */
import { describe, expect, test } from "vitest";
import { Array, Context, Effect, Layer, Option } from "effect";
import { ProjectionMarks, fitKey } from "@streamsy/core/internal";
import { SqlClient, Statement } from "effect/sql";
import { storageLayer } from "../../src/storage.ts";
import { dialectClient, freshDatabase, sqlDialects } from "./support.ts";

const dialect = sqlDialects.mysql;

const url = dialect.url;

const marksContext = (baseUrl: string) =>
  Effect.gen(function* () {
    const fresh = yield* freshDatabase(dialect, baseUrl);

    return yield* Layer.build(
      storageLayer().pipe(Layer.provideMerge(dialectClient(dialect, fresh))),
    );
  });

describe.skipIf(url === undefined)("mysql projection marks", () => {
  test("reads due marks in index order without a filesort", async () => {
    if (url === undefined) return;

    await Effect.runPromise(
      Effect.gen(function* () {
        const context = yield* marksContext(url);
        const marks = Context.get(context, ProjectionMarks);
        const sql = Context.get(context, SqlClient.SqlClient);
        const first = `{"id":"${"x".repeat(590)}a"}`;
        const second = `{"id":"${"x".repeat(590)}b"}`;
        const firstKey = yield* fitKey(first, 512);
        const secondKey = yield* fitKey(second, 512);
        // The fitted keys sort in the reverse order from the full params.
        expect(firstKey > secondKey).toBe(true);
        yield* marks.sleep("due", first, 0, 1);
        yield* marks.sleep("due", second, 0, 1);
        yield* marks.mark("due", ['{"id":"other"}']);
        yield* marks.mark(
          "noise",
          Array.map(Array.range(1, 400), (id) => `{"id":"${id}"}`),
        );
        yield* sql`INSERT INTO streamsy_projection_pending (projection, params, seq, due_at)
 SELECT 'due', params, seq, 1000 FROM streamsy_projection_pending WHERE projection = 'noise'`;
        yield* sql`ANALYZE TABLE streamsy_projection_pending`;

        const statements: Array<readonly [string, ReadonlyArray<unknown>]> = [];

        const found = yield* marks.due("due", 1, 2).pipe(
          Effect.provideService(Statement.CurrentTransformer, (statement) => {
            statements.push(statement.compile());

            return Effect.succeed(statement);
          }),
        );

        expect(statements).toHaveLength(1);
        const [query, bindings] = statements[0] ?? ["", []];

        const plan = yield* sql.unsafe<{ readonly key: string; readonly Extra: string }>(
          `EXPLAIN ${query}`,
          bindings,
        );

        yield* Effect.logInfo("MySQL due EXPLAIN", plan);
        expect(plan[0]?.Extra).not.toMatch(/filesort/iu);
        expect(plan[0]?.key).toBe("idx_streamsy_projection_pending_due");
        expect(Array.map(found, (mark) => mark.params)).toEqual(['{"id":"other"}', second]);

        const failure = { at: 1, reason: Option.none(), message: "stopped" };

        for (const param of [first, second])
          yield* marks.fail("due", param, { attempts: 1, dueAt: Option.none(), failure });
        const failed = yield* marks.failed("due", 10);
        expect(Array.map(failed, (mark) => mark.params)).toEqual([first, second]);
      }).pipe(Effect.scoped),
    );
  });

  test("fits pending params only after 512 code points", async () => {
    if (url === undefined) return;

    await Effect.runPromise(
      Effect.gen(function* () {
        const context = yield* marksContext(url);
        const marks = Context.get(context, ProjectionMarks);
        const sql = Context.get(context, SqlClient.SqlClient);
        const projection = "fit-boundary";

        for (const length of [512, 513]) {
          const params = `{"id":"${"𐀀".repeat(length - 9)}"}`;
          yield* marks.mark(projection, [params]);
          const stored = yield* fitKey(params, 512);

          const rows = yield* sql`SELECT params, full_params FROM streamsy_projection_pending
 WHERE projection = ${projection} AND full_params = ${params}`;

          expect(rows).toEqual([{ params: stored, full_params: params }]);
          expect((yield* marks.read(projection, [params])).get(params)?.params).toBe(params);
        }
      }).pipe(Effect.scoped),
    );
  });

  test("round-trips 600-character params", async () => {
    if (url === undefined) return;

    await Effect.runPromise(
      Effect.gen(function* () {
        const context = yield* marksContext(url);

        const marks = Context.get(context, ProjectionMarks);
        const sql = Context.get(context, SqlClient.SqlClient);
        const projection = "long-params";
        const params = `{"id":"${"𐀀".repeat(591)}"}`;
        const neighbour = `{"id":"${"𐀀".repeat(590)}x"}`;
        yield* marks.mark(projection, [params, neighbour]);

        const stored = yield* fitKey(params, 512);
        expect(
          yield* sql`SELECT params, full_params FROM streamsy_projection_pending WHERE params = ${stored}`,
        ).toEqual([{ params: stored, full_params: params }]);
        const due = yield* marks.due(projection, 0, 10);
        expect(new Set(Array.map(due, (mark) => mark.params))).toEqual(
          new Set([params, neighbour]),
        );
        expect((yield* marks.read(projection, [params])).get(params)).toMatchObject({
          params,
          seq: 1,
        });
        yield* marks.sleep(projection, params, 1, 1000);
        expect((yield* marks.read(projection, [params])).get(params)).toMatchObject({
          params,
          wakeAt: Option.some(1000),
        });
        yield* marks.mark(projection, [params]);
        const failure = { at: 1, reason: Option.none(), message: "stopped" };
        yield* marks.fail(projection, params, { attempts: 1, dueAt: Option.none(), failure });
        expect(yield* marks.failed(projection, 10)).toEqual([
          expect.objectContaining({ params, seq: 2, failure: Option.some(failure) }),
        ]);
        expect(yield* marks.retry(projection, params)).toBe(1);
        yield* marks.fail(projection, params, { attempts: 1, dueAt: Option.none(), failure });
        expect(yield* marks.resume(projection)).toBe(1);
        expect(yield* marks.resume(projection)).toBe(0);
        expect(yield* marks.due(projection, 0, 10)).toContainEqual(
          expect.objectContaining({ params, seq: 2, attempts: 1, stopped: false }),
        );
        yield* marks.clear(projection, params, 1);
        expect((yield* marks.read(projection, [params])).get(params)).toMatchObject({
          params,
          seq: 2,
          attempts: 0,
        });
        yield* marks.clear(projection, params, 2);
        expect((yield* marks.read(projection, [params])).size).toBe(0);

        yield* marks.sleep(projection, params, 0, 1000);
        expect((yield* marks.read(projection, [params])).get(params)).toMatchObject({
          params,
          seq: 1,
        });
        yield* marks.remove(projection, params);
        yield* marks.fail(projection, params, { attempts: 2, dueAt: Option.none(), failure });
        expect((yield* marks.read(projection, [params])).get(params)).toMatchObject({
          params,
          attempts: 2,
        });
        yield* marks.remove(projection, params);
        expect([...(yield* marks.read(projection, [params, neighbour])).keys()]).toEqual([
          neighbour,
        ]);
      }).pipe(Effect.scoped),
    );
  });
});
