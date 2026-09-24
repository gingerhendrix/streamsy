/* oxlint-disable effecttsgo/async-function -- Bun owns test execution. */
import { expect, it } from "bun:test";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Context, Effect, Layer, Option, Schema, Stream } from "effect";
import {
  Changes,
  Storage,
  StreamFamily,
  StreamGroup,
  StreamsReader,
  StreamsWriter,
} from "@streamsy/core";
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
