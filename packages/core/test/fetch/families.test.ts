/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch -- This test owns a real local HTTP server. */
import { expect, it } from "bun:test";
import { Context, Effect, Layer, Option, Schema, Stream } from "effect";
import { FetchHttpClient, HttpRouter } from "effect/unstable/http";
import { Changes, Http, Memory, StreamFamily, StreamGroup } from "@streamsy/core";
import { StreamsReader, StreamsWriter } from "../../src/protocol/index.ts";
import * as Fetch from "@streamsy/core/fetch";

class Notes extends StreamFamily.json<Notes>()("FetchFamilies/Notes", "notes/:user", {
  params: { user: Schema.String },
  schema: Schema.String,
}) {}
const group = StreamGroup.make(Notes);
const program = Effect.gen(function* () {
  const notes = yield* Notes;
  const params = { user: "ann" };
  yield* notes.create(params);
  yield* notes.append(params, ["hello", "world"]);
  const batches = yield* Stream.runCollect(notes.read(params));
  yield* notes.remove(params);
  return batches.flatMap((batch) => batch.items);
});

it("one application program runs unchanged on memory and Fetch families", async () => {
  const local = await Effect.runPromise(program.pipe(Effect.provide(Memory.layer({}, group))));
  const edge = HttpRouter.toWebHandler(
    Http.routes(group, { prefix: "/streams", access: "full" }).pipe(
      Layer.provideMerge(Memory.layer({}, group)),
    ),
    { disableLogger: true },
  );
  const server = Bun.serve({ port: 0, fetch: (request) => edge.handler(request) });
  try {
    for (const host of [
      Fetch.layer({ baseUrl: `${server.url.href}streams` }, group),
      Fetch.layer({ baseUrl: `${server.url.href}streams` }, Notes),
    ]) {
      const remote = host.pipe(Layer.provide(FetchHttpClient.layer));
      const inspected = Effect.gen(function* () {
        const context = yield* Effect.context<Notes>();
        expect(Option.isNone(Context.getOption(context, StreamsReader))).toBe(true);
        expect(Option.isNone(Context.getOption(context, StreamsWriter))).toBe(true);
        expect(Option.isNone(Context.getOption(context, Changes(Notes)))).toBe(true);
        return yield* program;
      });
      expect(await Effect.runPromise(inspected.pipe(Effect.provide(remote)))).toEqual(local);
    }
    expect(local).toEqual(["hello", "world"]);
    expect(
      (await fetch(`${server.url.href}streams/private/secret`, { method: "PUT" })).status,
    ).toBe(404);
  } finally {
    await server.stop(true);
    await edge.dispose();
  }
});

it("group and family-list HTTP routes apply explicit host access", async () => {
  for (const routes of [
    Http.routes(group, { access: "read" }),
    Http.routes([Notes], { access: () => "read" }),
  ]) {
    const edge = HttpRouter.toWebHandler(routes.pipe(Layer.provideMerge(Memory.layer({}, Notes))), {
      disableLogger: true,
    });
    try {
      expect(
        (await edge.handler(new Request("http://local/notes/ann", { method: "PUT" }))).status,
      ).toBe(405);
      expect(
        (
          await edge.handler(new Request("http://local/notes/ann", { method: "OPTIONS" }))
        ).headers.get("access-control-allow-methods"),
      ).toBe("GET, HEAD, OPTIONS");
    } finally {
      await edge.dispose();
    }
  }
});
