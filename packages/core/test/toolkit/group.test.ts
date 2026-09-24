/* oxlint-disable effecttsgo/async-function -- Bun owns test execution. */
import { expect, it } from "bun:test";
import { Context, Effect, Layer, Option, Schema } from "effect";
import {
  AnyStream,
  Changes,
  Memory,
  provides,
  Storage,
  StreamFamily,
  StreamGroup,
} from "../../src/index.ts";
import { Backend, StreamsReader, StreamsWriter } from "../../src/protocol/index.ts";
import * as Protocol from "../../src/protocol/index.ts";

class Notes extends StreamFamily.json<Notes>()("Group/Notes", "notes/:user", {
  params: { user: Schema.String },
  schema: Schema.String,
}) {}
class Counts extends StreamFamily.bytes<Counts>()("Group/Counts", "counts/:user", {
  params: { user: Schema.String },
}) {}
class DuplicateName extends StreamFamily.bytes<DuplicateName>()("Group/Notes", "other", {
  params: {},
}) {}
class DuplicateTemplate extends StreamFamily.bytes<DuplicateTemplate>()(
  "Group/Duplicate",
  "notes/:user",
  { params: { user: Schema.String } },
) {}
const group = StreamGroup.make(Notes, Counts);

it("groups freeze their ordered declarations and reject duplicates and misplaced catch-alls", () => {
  expect(group.families).toEqual([Notes, Counts]);
  expect(Object.isFrozen(group)).toBe(true);
  expect(Object.isFrozen(group.families)).toBe(true);
  expect(() => StreamGroup.make(Notes, DuplicateName)).toThrow("duplicate family name");
  expect(() => StreamGroup.make(Notes, DuplicateTemplate)).toThrow("duplicate family template");
  expect(() => StreamGroup.make(AnyStream, Notes)).toThrow("AnyStream must be last");
  expect(StreamGroup.make(Notes, AnyStream).families).toHaveLength(2);
});

it("memory families expose feeds and storage but no generic protocol keys", async () => {
  for (const host of [Memory.layer({}, group), Memory.layer({}, Notes, Counts)]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(host);
          expect(Option.isNone(Context.getOption(context, StreamsReader))).toBe(true);
          expect(Option.isNone(Context.getOption(context, StreamsWriter))).toBe(true);
          const notes = Context.get(context, Notes);
          const feed = Context.get(context, Changes(Notes));
          expect(feed.owner).toBe(notes.owner);
          expect(Context.get(context, Counts).owner).toBe(notes.owner);
          yield* notes.create({ user: "ann" });
          const record = yield* Context.get(context, Storage).record(Notes.at({ user: "ann" }).id);
          expect(Option.isSome(record)).toBe(true);
        }),
      ),
    );
  }
});

it("provides accepts groups for both protocol graphs and named backends", async () => {
  const backend = Backend.make("group-test");
  const protocol = Protocol.layerMemory();
  for (const host of [
    provides(protocol, group),
    provides(backend, group).pipe(Layer.provide(backend.layer(protocol))),
  ]) {
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* (yield* Notes).create({ user: "ann" });
        yield* (yield* Counts).create({ user: "ann" });
      }).pipe(Effect.provide(host)),
    );
  }
});
