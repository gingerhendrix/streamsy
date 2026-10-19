import { expect, layer } from "@effect/vitest";
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

for (const [index, host] of [
  bun,
  BunStorage.layer(Streamsy.group(Notes), options),
  sql,
].entries()) {
  layer(host)((it) => {
    it.effect(
      `Bun and SQL stream layers share storage without exporting the generic pair (${index})`,
      () =>
        Effect.gen(function* () {
          const context = yield* Effect.context<Storage | Notes>();
          expect(Option.isNone(Context.getOption(context, StreamsReader))).toBe(true);
          expect(Option.isNone(Context.getOption(context, StreamsWriter))).toBe(true);
          const notes = yield* Notes;
          yield* notes.create({});
          yield* notes.append({}, ["hello"]);

          expect(Option.isSome(yield* (yield* Storage).record(Notes.at({}).id))).toBe(true);
          expect((yield* Stream.runCollect(notes.read({})))[0]?.items).toEqual(["hello"]);
        }),
    );
  });
}

type Assignability<To> = <From>(value: From) => From extends To ? 1 : 2;

type Equal<A, B> = Assignability<A> extends Assignability<B> ? true : false;

type SqlOutputs = Storage | CommitBoundary | OwnerRecords;

type ClientServices = SqlClient.SqlClient | Reactivity.Reactivity;

type HostOutputs = SqlOutputs | ClientServices;

const bunExact: Equal<Layer.Success<typeof bun>, HostOutputs | Notes> = true;

const bunRequirements: Equal<Layer.Services<typeof bun>, never> = true;

const bunErrors: Equal<Layer.Error<typeof bun>, StorageFault> = true;

const rawSql = SqlStorage.layer(group);

const sqlExact: Equal<Layer.Success<typeof rawSql>, SqlOutputs | Notes> = true;

const sqlRequirements: Equal<Layer.Services<typeof rawSql>, ClientServices> = true;

const sqlErrors: Equal<Layer.Error<typeof rawSql>, StorageFault> = true;

function exactDurable(durableOptions: DurableObjectStorage.Options) {
  const host = DurableObjectStorage.layer(group, durableOptions);

  const outputs: Equal<Layer.Success<typeof host>, HostOutputs | Notes> = true;

  const requirements: Equal<Layer.Services<typeof host>, never> = true;
  const errors: Equal<Layer.Error<typeof host>, StorageFault> = true;

  return [outputs, requirements, errors];
}

void [bunExact, bunRequirements, bunErrors, sqlExact, sqlRequirements, sqlErrors, exactDurable];
