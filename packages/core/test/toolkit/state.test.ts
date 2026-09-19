import { expect, it } from "bun:test";
import { Effect, Schema, Stream } from "effect";
import * as State from "../../src/toolkit/state.ts";
import * as StreamRef from "../../src/toolkit/ref.ts";
import * as Streams from "../../src/toolkit/streams.ts";

const Story = Schema.Struct({ id: Schema.Finite, title: Schema.String });
const stories = StreamRef.state("stories", {
  schema: Story,
  type: "story",
  key: "id",
});

it("state changes round trip with Durable State wire fields and source headers", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const first = { id: 7, title: "first" };
      const second = { id: 8, title: "second" };
      const facts = State.changes(stories, { offset: "source:4" }, [
        State.upsert(first),
        State.delete(second),
      ]);

      yield* Streams.create(stories);
      yield* Streams.append(stories, facts);

      expect(yield* Streams.read(stories).pipe(Streams.items, Stream.runCollect)).toEqual(facts);
      expect(facts.map((fact) => fact.headers)).toEqual([
        { operation: "upsert", offset: "source:4", txid: "source:4:0" },
        { operation: "delete", offset: "source:4", txid: "source:4:1" },
      ]);

      const raw = yield* Streams.read(StreamRef.bytes("stories")).pipe(
        Streams.items,
        Stream.runCollect,
      );
      expect(raw.map((item) => new TextDecoder().decode(item))).toEqual([
        '{"type":"story","key":"7","value":{"id":7,"title":"first"},"headers":{"operation":"upsert","offset":"source:4","txid":"source:4:0"}}',
        '{"type":"story","key":"8","old_value":{"id":8,"title":"second"},"headers":{"operation":"delete","offset":"source:4","txid":"source:4:1"}}',
      ]);
    }).pipe(Effect.provide(Streams.layerMemory())),
  ));
