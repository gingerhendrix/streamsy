import { expect, it } from "bun:test";
import { Effect, Schema, Stream } from "effect";
import * as State from "../../src/toolkit/state.ts";
import * as StreamRef from "../../src/toolkit/ref.ts";
import * as Streams from "../../src/toolkit/streams.ts";

const Story = Schema.Struct({ id: Schema.Finite, title: Schema.String });
const stories = StreamRef.state("stories", {
  collections: { story: { schema: Story, key: "id" } },
});
const Slug = Schema.Struct({ slug: Schema.String, title: Schema.String });
const slugs = StreamRef.state("slugs", { collections: { slug: { schema: Slug, key: "slug" } } });

it("state changes round trip with Durable State wire fields and source headers", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const first = { id: 7, title: "first" };
      const second = { id: 8, title: "second" };
      const facts = State.changes(stories, { offset: "source:4" }, [
        State.upsert("story", first),
        State.delete("story", second),
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
    State.changes(slugs, { offset: "source:5" }, [State.upsert("slug", { slug: "a", title: "A" })]),
  ).toEqual([
    {
      type: "slug",
      key: "a",
      value: { slug: "a", title: "A" },
      headers: { operation: "upsert", offset: "source:5", txid: "source:5:0" },
    },
  ]);
  expect(() =>
    State.changes(slugs, { offset: "source:6" }, [
      State.upsert("slug", { slug: "", title: "empty" }),
    ]),
  ).toThrow('Invalid state key for type "slug": field "slug"');
});

it("rejects an omitted optional key at runtime", () => {
  const OptionalSlug = Schema.Struct({
    slug: Schema.optionalKey(Schema.String),
    title: Schema.String,
  });
  const optionalSlugs = StreamRef.state("optional-slugs", {
    collections: {
      // SAFETY: bypass the compile-time key constraint to cover the runtime boundary.
      slug: { schema: OptionalSlug, key: "slug" as never },
    },
  });
  expect(() =>
    State.changes(optionalSlugs, { offset: "source:7" }, [
      State.upsert("slug", { title: "missing" }),
    ]),
  ).toThrow(TypeError);
});

it("lets a change override txid and add source headers without replacing offset", () => {
  const facts = State.changes(stories, { offset: "source:8" }, [
    State.upsert(
      "story",
      { id: 7, title: "first" },
      {
        headers: { txid: "client-uuid", timestamp: "2026-09-20T10:00:00Z" },
      },
    ),
    State.upsert("story", { id: 8, title: "second" }),
  ]);
  expect(facts.map((fact) => fact.headers)).toEqual([
    {
      operation: "upsert",
      offset: "source:8",
      txid: "client-uuid",
      timestamp: "2026-09-20T10:00:00Z",
    },
    { operation: "upsert", offset: "source:8", txid: "source:8:1" },
  ]);
  const encoded = Schema.encodeSync(stories.codec)(facts[0]!);
  // SAFETY: `StreamRef.state` constructs a JSON ref whose codec encodes to a string.
  const encodedText = encoded as string;
  expect(JSON.parse(encodedText).headers).toEqual(facts[0]!.headers);
});

it("writes and reads several collections through one state ref", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const Project = Schema.Struct({ id: Schema.String, name: Schema.String });
      const workspace = StreamRef.state("workspace", {
        collections: {
          project: { schema: Project, key: "id" },
          story: { schema: Story, key: "id" },
        },
      });
      const facts = State.changes(workspace, { offset: "source:9" }, [
        State.upsert("project", { id: "p1", name: "Streamsy" }),
        State.upsert("story", { id: 7, title: "first" }),
        State.delete("project", { id: "p1", name: "Streamsy" }),
      ]);
      expect(facts.map((fact) => [fact.type, fact.key, fact.headers.operation])).toEqual([
        ["project", "p1", "upsert"],
        ["story", "7", "upsert"],
        ["project", "p1", "delete"],
      ]);

      yield* Streams.create(workspace);
      yield* Streams.append(workspace, facts);

      const items = yield* Streams.read(workspace).pipe(Streams.items, Stream.runCollect);
      expect(items).toEqual([...facts]);
      const names = items.flatMap((item) =>
        item.type === "project" && "value" in item ? [item.value.name] : [],
      );
      expect(names).toEqual(["Streamsy"]);
    }).pipe(Effect.provide(Streams.layerMemory())),
  ));

it("fails to decode an event whose type is not a declared collection", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const result = yield* Schema.decodeEffect(stories.codec)(
        '{"type":"comment","key":"1","value":{"id":1,"title":"x"},"headers":{"operation":"upsert"}}',
      ).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
    }),
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

it("normalizes Durable State insert, update, and upsert operations", () => {
  for (const operation of ["insert", "update", "upsert"] as const) {
    const decoded = Schema.decodeSync(stories.codec)(
      JSON.stringify({
        type: "story",
        key: "7",
        value: { id: 7, title: operation },
        headers: { operation },
      }),
    );
    expect(decoded.headers.operation).toBe("upsert");
  }

  const encoded = Schema.encodeSync(stories.codec)({
    type: "story",
    key: "7",
    value: { id: 7, title: "canonical" },
    headers: { operation: "upsert" },
  });
  expect(JSON.parse(encoded as string).headers.operation).toBe("upsert");
});
