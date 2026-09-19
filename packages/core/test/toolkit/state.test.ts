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
const Slug = Schema.Struct({ slug: Schema.String, title: Schema.String });
const slugs = StreamRef.state("slugs", { schema: Slug, type: "slug", key: "slug" });

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

      expect(yield* Streams.read(stories).pipe(Streams.items, Stream.runCollect)).toEqual([
        ...facts,
      ]);
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

it("supports string keys and rejects empty derived keys", () => {
  expect(
    State.changes(slugs, { offset: "source:5" }, [State.upsert({ slug: "a", title: "A" })]),
  ).toEqual([
    {
      type: "slug",
      key: "a",
      value: { slug: "a", title: "A" },
      headers: { operation: "upsert", offset: "source:5", txid: "source:5:0" },
    },
  ]);
  expect(() =>
    State.changes(slugs, { offset: "source:6" }, [State.upsert({ slug: "", title: "empty" })]),
  ).toThrow('Invalid state key for type "slug": field "slug"');
});

it("reads multiple Durable State types through a union of stateChange codecs", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const Project = Schema.Struct({ id: Schema.String, name: Schema.String });
      const workspace = StreamRef.json("workspace", {
        schema: Schema.Union([
          StreamRef.stateChange({ schema: Project, type: "project" }),
          StreamRef.stateChange({ schema: Story, type: "story" }),
        ]),
      });
      const facts = [
        {
          type: "project" as const,
          key: "p1",
          value: { id: "p1", name: "Streamsy" },
          headers: { operation: "upsert" as const },
        },
        {
          type: "story" as const,
          key: "7",
          value: { id: 7, title: "first" },
          headers: { operation: "upsert" as const },
        },
      ];

      yield* Streams.create(workspace);
      yield* Streams.append(workspace, facts);

      expect(yield* Streams.read(workspace).pipe(Streams.items, Stream.runCollect)).toEqual(facts);
    }).pipe(Effect.provide(Streams.layerMemory())),
  ));

it("decodes foreign-writer optional headers and delete null values", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      expect(
        yield* Schema.decodeEffect(stories.codec)(
          '{"type":"story","key":"7","value":{"id":7,"title":"first"},"headers":{"operation":"upsert","timestamp":"2026-09-20T10:00:00Z","from":"source"}}',
        ),
      ).toEqual({
        type: "story",
        key: "7",
        value: { id: 7, title: "first" },
        headers: {
          operation: "upsert",
          timestamp: "2026-09-20T10:00:00Z",
          from: "source",
        },
      });
      expect(
        yield* Schema.decodeEffect(stories.codec)(
          '{"type":"story","key":"8","value":null,"old_value":{"id":8,"title":"second"},"headers":{"operation":"delete"}}',
        ),
      ).toEqual({
        type: "story",
        key: "8",
        old_value: { id: 8, title: "second" },
        headers: { operation: "delete" },
      });
    }),
  ));
