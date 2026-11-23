/* oxlint-disable effecttsgo/async-function -- Bun owns the database fixtures. */
import { describe, expect, test } from "vitest";
import { Array, Context, Effect, Layer, Option } from "effect";
import { ProjectionMarks, fitKey } from "@streamsy/core/internal";
import { SqlClient } from "effect/sql";
import { storageLayer } from "../../src/storage.ts";
import { dialectClient, freshDatabase, sqlDialects } from "./support.ts";

const dialect = sqlDialects.mysql;

const url = dialect.url;

describe.skipIf(url === undefined)("mysql projection marks", () => {
  test("round-trips 600-character params", async () => {
    if (url === undefined) return;

    await Effect.runPromise(
      Effect.gen(function* () {
        const fresh = yield* freshDatabase(dialect, url);

        const context = yield* Layer.build(
          storageLayer().pipe(Layer.provideMerge(dialectClient(dialect, fresh))),
        );

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
