/* oxlint-disable effecttsgo/async-function -- Bun owns test execution. */
import { expect, it } from "bun:test";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Context, Effect, Layer, Option, Schema, Stream } from "effect";
import { Changes, Storage, type StorageFault, StreamFamily, StreamGroup } from "@streamsy/core";
import { StreamsReader, StreamsWriter } from "@streamsy/core/protocol";
import type * as Reactivity from "effect/unstable/reactivity/Reactivity";
import type * as SqlClient from "effect/unstable/sql/SqlClient";
import { sharedSqlClientLayer } from "../src/boundary.ts";
import * as BunStorage from "@streamsy/storage/bun";
import * as SqlStorage from "@streamsy/storage";
import * as DurableObjectStorage from "@streamsy/storage/durable-object";

class Notes extends StreamFamily.json<Notes>()("StorageFamilies/Notes", "notes", {
  params: {},
  schema: Schema.String,
}) {}
const group = StreamGroup.make(Notes);
const options = { client: { filename: ":memory:" } };
const bun = BunStorage.layer(options, group);
const sql = SqlStorage.layer({}, Notes).pipe(
  Layer.provide(
    Layer.effectContext(sharedSqlClientLayer(SqliteClient.make({ filename: ":memory:" }))),
  ),
);

it("Bun and SQL family layers share storage and feeds without exporting the generic pair", async () => {
  for (const host of [bun, BunStorage.layer(options, Notes), sql]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(host);
          expect(Option.isNone(Context.getOption(context, StreamsReader))).toBe(true);
          expect(Option.isNone(Context.getOption(context, StreamsWriter))).toBe(true);
          const notes = Context.get(context, Notes);
          const feed = Context.get(context, Changes(Notes));
          expect(feed.owner).toBe(notes.owner);
          yield* notes.create({});
          yield* notes.append({}, ["hello"]);
          const snapshot = yield* Stream.runCollect(
            feed.changes(Notes.at({}).id).pipe(Stream.take(1)),
          );
          expect(snapshot).toHaveLength(1);
          expect(Option.isSome(yield* Context.get(context, Storage).record(Notes.at({}).id))).toBe(
            true,
          );
          expect((yield* Stream.runCollect(notes.read({})))[0]?.items).toEqual(["hello"]);
        }),
      ),
    );
  }
});

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
const hidden: Equal<
  Extract<Layer.Success<typeof bun>, StreamsReader | StreamsWriter>,
  never
> = true;
const feeds: Equal<Extract<Layer.Success<typeof bun>, Changes<Notes>>, Changes<Notes>> = true;
function durableTypes(options: DurableObjectStorage.DurableObjectStorageOptions) {
  const host = DurableObjectStorage.layer(options, group);
  const hidden: Equal<
    Extract<Layer.Success<typeof host>, StreamsReader | StreamsWriter>,
    never
  > = true;
  const feeds: Equal<Extract<Layer.Success<typeof host>, Changes<Notes>>, Changes<Notes>> = true;
  return [hidden, feeds];
}
void [hidden, feeds, durableTypes];

type SqlOutputs = Storage | SqlStorage.CommitBoundary;
type ClientServices = SqlClient.SqlClient | Reactivity.Reactivity;
type HostOutputs = SqlOutputs | ClientServices;
const bunExact: Equal<Layer.Success<typeof bun>, HostOutputs | Notes | Changes<Notes>> = true;
const bunRequirements: Equal<Layer.Services<typeof bun>, never> = true;
const bunErrors: Equal<Layer.Error<typeof bun>, StorageFault> = true;
const oldBun: Equal<Layer.Success<ReturnType<typeof BunStorage.layer>>, HostOutputs> = true;
const oldSql: Equal<Layer.Success<ReturnType<typeof SqlStorage.layer>>, SqlOutputs> = true;
const oldDo: Equal<
  Layer.Success<ReturnType<typeof DurableObjectStorage.layer>>,
  HostOutputs
> = true;
const rawSql = SqlStorage.layer({}, group);
const sqlExact: Equal<Layer.Success<typeof rawSql>, SqlOutputs | Notes | Changes<Notes>> = true;
const sqlRequirements: Equal<Layer.Services<typeof rawSql>, ClientServices> = true;
const sqlErrors: Equal<Layer.Error<typeof rawSql>, StorageFault> = true;
const mappedBun = Effect.map(Effect.succeed(options), BunStorage.layer);
const pointFreeBun: Equal<Layer.Success<Effect.Success<typeof mappedBun>>, HostOutputs> = true;
const mappedSql = Effect.map(Effect.succeed({}), SqlStorage.layer);
const pointFreeSql: Equal<Layer.Success<Effect.Success<typeof mappedSql>>, SqlOutputs> = true;
const hasFeed: Effect.Effect<void, StorageFault> = Effect.asVoid(Changes(Notes)).pipe(
  Effect.provide(bun),
);
function exactDurable(options: DurableObjectStorage.DurableObjectStorageOptions) {
  const host = DurableObjectStorage.layer(options, group);
  const outputs: Equal<Layer.Success<typeof host>, HostOutputs | Notes | Changes<Notes>> = true;
  const requirements: Equal<Layer.Services<typeof host>, never> = true;
  const errors: Equal<Layer.Error<typeof host>, StorageFault> = true;
  const mapped = Effect.map(Effect.succeed(options), DurableObjectStorage.layer);
  const pointFree: Equal<Layer.Success<Effect.Success<typeof mapped>>, HostOutputs> = true;
  return [outputs, requirements, errors, pointFree];
}
// @ts-expect-error Protocol options without families must not silently fall through to storage-only.
BunStorage.layer({ client: { filename: ":memory:" }, longPollTimeoutMs: 5 });
void [
  bunExact,
  bunRequirements,
  bunErrors,
  oldBun,
  oldSql,
  oldDo,
  sqlExact,
  sqlRequirements,
  sqlErrors,
  pointFreeBun,
  pointFreeSql,
  hasFeed,
  exactDurable,
];
