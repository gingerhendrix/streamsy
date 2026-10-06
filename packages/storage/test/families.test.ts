/* oxlint-disable effecttsgo/async-function -- Bun owns test execution. */
import { expect, it } from "bun:test";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Context, Effect, Layer, Option, Schema, Stream } from "effect";
import { type CommitBoundary, Storage, type StorageFault, Streamsy } from "@streamsy/core";
import type { OwnerRecords } from "@streamsy/core/internal";
import { StreamsReader, StreamsWriter } from "@streamsy/core/protocol";
import type * as Reactivity from "effect/reactivity/Reactivity";
import type * as SqlClient from "effect/sql/SqlClient";
import { sharedSqlClientLayer } from "../src/boundary.ts";
import { BunStorage } from "@streamsy/storage/bun";
import { SqlStorage } from "@streamsy/storage";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";

class Notes extends Streamsy.json<Notes>()("StorageFamilies/Notes", "notes", {
  params: {},
  schema: Schema.String,
}) {}

const group = Streamsy.group(Notes);

const options = { client: { filename: ":memory:" } };

const bun = BunStorage.layer(group, options);

const sql = SqlStorage.layer(Streamsy.group(Notes)).pipe(
  Layer.provide(
    Layer.effectContext(sharedSqlClientLayer(SqliteClient.make({ filename: ":memory:" }))),
  ),
);

it("Bun and SQL stream layers share storage and feeds without exporting the generic pair", async () => {
  for (const host of [bun, BunStorage.layer(Streamsy.group(Notes), options), sql]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(host);
          expect(Option.isNone(Context.getOption(context, StreamsReader))).toBe(true);
          expect(Option.isNone(Context.getOption(context, StreamsWriter))).toBe(true);
          const notes = Context.get(context, Notes);
          const feed = Context.get(context, Streamsy.ChangeFeed(Notes));
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

type SqlOutputs = Storage | CommitBoundary | OwnerRecords;

type ClientServices = SqlClient.SqlClient | Reactivity.Reactivity;

type HostOutputs = SqlOutputs | ClientServices;

const bunExact: Equal<
  Layer.Success<typeof bun>,
  HostOutputs | Notes | Streamsy.ChangeFeed<Notes>
> = true;

const bunRequirements: Equal<Layer.Services<typeof bun>, never> = true;

const bunErrors: Equal<Layer.Error<typeof bun>, StorageFault> = true;

const rawSql = SqlStorage.layer(group);

const sqlExact: Equal<
  Layer.Success<typeof rawSql>,
  SqlOutputs | Notes | Streamsy.ChangeFeed<Notes>
> = true;

const sqlRequirements: Equal<Layer.Services<typeof rawSql>, ClientServices> = true;

const sqlErrors: Equal<Layer.Error<typeof rawSql>, StorageFault> = true;

const hasFeed: Effect.Effect<void, StorageFault> = Effect.asVoid(Streamsy.ChangeFeed(Notes)).pipe(
  Effect.provide(bun),
);

function exactDurable(options: DurableObjectStorage.Options) {
  const host = DurableObjectStorage.layer(group, options);

  const outputs: Equal<
    Layer.Success<typeof host>,
    HostOutputs | Notes | Streamsy.ChangeFeed<Notes>
  > = true;

  const requirements: Equal<Layer.Services<typeof host>, never> = true;
  const errors: Equal<Layer.Error<typeof host>, StorageFault> = true;

  return [outputs, requirements, errors];
}

void [
  bunExact,
  bunRequirements,
  bunErrors,
  sqlExact,
  sqlRequirements,
  sqlErrors,
  hasFeed,
  exactDurable,
];
