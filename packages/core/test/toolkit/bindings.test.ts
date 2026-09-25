import { Protocol } from "@streamsy/core/protocol";
import { expect, it } from "bun:test";
import { Effect, Layer, Schema, Stream } from "effect";
import { Streams } from "../../src/protocol/index.ts";
import { Streamsy, Memory } from "../../src/index.ts";

class Drafts extends Streamsy.json<Drafts>()("Bindings/Drafts", "drafts/:user", {
  params: { user: Schema.String },
  schema: Schema.String,
}) {}
class Journal extends Streamsy.json<Journal>()("Bindings/Journal", "journal/:user", {
  params: { user: Schema.String },
  schema: Schema.String,
}) {}

it("declared streams and Streamsy.Wildcard share one store for declared and arbitrary ids", async () => {
  const host = Memory.layer({}, Streamsy.group(Drafts, Journal, Streamsy.Wildcard));
  await Effect.runPromise(
    Effect.gen(function* () {
      const drafts = yield* Drafts;
      const journal = yield* Journal;
      const any = yield* Streamsy.Wildcard;
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
      expect(yield* any.head({ id: Drafts.at({ user: "ann" }).id })).toEqual(
        yield* drafts.head({ user: "ann" }),
      );
      yield* any.create({ id: "arbitrary/path" });
      expect((yield* any.head({ id: "arbitrary/path" })).contentType).toBe(
        "application/octet-stream",
      );
    }).pipe(Effect.provide(host)),
  );
});

it("stream binding shares a counted storage graph with siblings in either merge order", async () => {
  for (const familyFirst of [true, false]) {
    let builds = 0;
    const counted = Layer.effectDiscard(
      Effect.sync(() => {
        builds += 1;
      }),
    );
    const protocol = Streams.layerMemory().pipe(Layer.provideMerge(counted));
    const graph = Protocol.bind(protocol, Streamsy.group(Drafts));
    const sibling = Layer.effectDiscard(Effect.void).pipe(Layer.provide(counted));
    const host = familyFirst ? Layer.merge(graph, sibling) : Layer.merge(sibling, graph);
    await Effect.runPromise(
      Effect.flatMap(Drafts, (drafts) => drafts.create({ user: "ann" })).pipe(Effect.provide(host)),
    );
    expect(builds).toBe(1);
  }
});
