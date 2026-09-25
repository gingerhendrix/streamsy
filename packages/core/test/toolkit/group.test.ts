import { Protocol } from "@streamsy/core/protocol";
/* oxlint-disable effecttsgo/async-function -- Bun owns test execution. */
import { expect, it } from "bun:test";
import { Context, Effect, Layer, Option, Schema } from "effect";
import { Streamsy, Memory, Storage } from "../../src/index.ts";
import { Streams, StreamsReader, StreamsWriter } from "../../src/protocol/index.ts";

class Notes extends Streamsy.json<Notes>()("Group/Notes", "notes/:user", {
  params: { user: Schema.String },
  schema: Schema.String,
}) {}
class Counts extends Streamsy.bytes<Counts>()("Group/Counts", "counts/:user", {
  params: { user: Schema.String },
}) {}
class DuplicateName extends Streamsy.bytes<DuplicateName>()("Group/Notes", "other", {
  params: {},
}) {}
class DuplicateTemplate extends Streamsy.bytes<DuplicateTemplate>()(
  "Group/Duplicate",
  "notes/:user",
  { params: { user: Schema.String } },
) {}
const group = Streamsy.group(Notes, Counts);

it("groups freeze their ordered declarations and reject duplicates and misplaced catch-alls", () => {
  expect(group.streams).toEqual([Notes, Counts]);
  expect(Object.isFrozen(group)).toBe(true);
  expect(Object.isFrozen(group.streams)).toBe(true);
  expect(() => Streamsy.group(Notes, DuplicateName)).toThrow("duplicate stream name");
  expect(() => Streamsy.group(Notes, DuplicateTemplate)).toThrow("duplicate stream template");
  expect(() => Streamsy.group(Streamsy.Wildcard, Notes)).toThrow("Wildcard must be last");
  expect(Streamsy.group(Notes, Streamsy.Wildcard).streams).toHaveLength(2);
});

it("memory streams expose feeds and storage but no generic protocol keys", async () => {
  for (const host of [Memory.layer({}, group), Memory.layer({}, Streamsy.group(Notes, Counts))]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(host);
          expect(Option.isNone(Context.getOption(context, StreamsReader))).toBe(true);
          expect(Option.isNone(Context.getOption(context, StreamsWriter))).toBe(true);
          const notes = Context.get(context, Notes);
          const feed = Context.get(context, Streamsy.ChangeFeed(Notes));
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

it("provides accepts groups for protocol graphs", async () => {
  const host = Protocol.bind(Streams.layerMemory(), group);
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* (yield* Notes).create({ user: "ann" });
      yield* (yield* Counts).create({ user: "ann" });
    }).pipe(Effect.provide(host)),
  );
});
