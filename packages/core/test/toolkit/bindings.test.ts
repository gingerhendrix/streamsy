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
  const host = Memory.layer(Streamsy.group(Drafts, Journal, Streamsy.Wildcard));
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
      yield* any.append({ id: "arbitrary/path" }, [new TextEncoder().encode("hello")]);
      expect(
        (yield* Stream.runCollect(any.read({ id: "arbitrary/path" }))).flatMap((b) => b.items),
      ).toEqual([new TextEncoder().encode("hello")]);
    }).pipe(Effect.provide(host)),
  );
});

it("stream binding shares one storage graph with siblings in either merge order", async () => {
  for (const familyFirst of [true, false]) {
    const protocol = Streams.layerMemory();
    const graph = Protocol.bind(protocol, Streamsy.group(Drafts));
    const sibling = Protocol.bind(protocol, Streamsy.group(Streamsy.Wildcard));
    const host = familyFirst ? Layer.merge(graph, sibling) : Layer.merge(sibling, graph);
    const seen = await Effect.runPromise(
      Effect.gen(function* () {
        const drafts = yield* Drafts;
        yield* drafts.create({ user: "ann" });
        yield* drafts.append({ user: "ann" }, ["draft"]);
        const any = yield* Streamsy.Wildcard;
        const batches = yield* Stream.runCollect(any.read({ id: Drafts.at({ user: "ann" }).id }));
        return batches
          .flatMap((batch) => batch.items)
          .map((item) => new TextDecoder().decode(item));
      }).pipe(Effect.provide(host)),
    );
    expect(seen).toEqual(['"draft"']);
  }
});
