import { expect, it } from "bun:test";
import { Effect, Layer, Schema, Stream } from "effect";
import { AnyStream, Memory, StreamFamily, StreamGroup } from "../../src/index.ts";

class Drafts extends StreamFamily.json<Drafts>()("Bindings/Drafts", "drafts/:user", {
  params: { user: Schema.String },
  schema: Schema.String,
}) {}
class Journal extends StreamFamily.json<Journal>()("Bindings/Journal", "journal/:user", {
  params: { user: Schema.String },
  schema: Schema.String,
}) {}

it("independent family hosts and an explicit AnyStream cover declared and arbitrary ids", async () => {
  const host = Layer.merge(
    Memory.layer({}, Drafts),
    Memory.layer({}, StreamGroup.make(Journal, AnyStream)),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      const drafts = yield* Drafts;
      const journal = yield* Journal;
      const any = yield* AnyStream;
      yield* drafts.create({ user: "ann" });
      yield* drafts.append({ user: "ann" }, ["draft"]);
      yield* journal.create({ user: "ann" });
      yield* journal.append({ user: "ann" }, ["entry"]);
      yield* journal.fork({ user: "copy" }, { user: "ann" });
      expect(
        (yield* Stream.runCollect(journal.read({ user: "copy" }))).flatMap((b) => b.items),
      ).toEqual(["entry"]);
      expect(
        (yield* Stream.runCollect(drafts.read({ user: "ann" }))).flatMap((b) => b.items),
      ).toEqual(["draft"]);
      yield* any.create({ id: "arbitrary/path" });
      expect((yield* any.head({ id: "arbitrary/path" })).contentType).toBe(
        "application/octet-stream",
      );
    }).pipe(Effect.provide(host)),
  );
});

it("family binding shares a counted storage graph with siblings in either merge order", async () => {
  for (const familyFirst of [true, false]) {
    let builds = 0;
    const counted = Layer.effectDiscard(
      Effect.sync(() => {
        builds += 1;
      }),
    );
    const graph = Memory.layer({}, Drafts).pipe(Layer.provideMerge(counted));
    const sibling = Layer.effectDiscard(Effect.void).pipe(Layer.provide(counted));
    const host = familyFirst ? Layer.merge(graph, sibling) : Layer.merge(sibling, graph);
    await Effect.runPromise(
      Effect.flatMap(Drafts, (drafts) => drafts.create({ user: "ann" })).pipe(Effect.provide(host)),
    );
    expect(builds).toBe(1);
  }
});
