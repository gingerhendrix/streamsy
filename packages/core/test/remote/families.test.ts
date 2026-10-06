/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch -- This test owns a real local HTTP server. */
import { expect, it } from "bun:test";
import { Context, Effect, Layer, Option, Schema, Stream } from "effect";
import { FetchHttpClient, HttpRouter } from "effect/http";
import { Http, MemoryStorage, Streamsy } from "@streamsy/core";
import { StreamsReader, StreamsWriter } from "../../src/protocol/index.ts";
import { RemoteStreams } from "@streamsy/core/remote";

class Notes extends Streamsy.json<Notes>()("FetchFamilies/Notes", "notes/:user", {
  params: { user: Schema.String },
  schema: Schema.String,
}) {}

const group = Streamsy.group(Notes);

const program = Effect.gen(function* () {
  const notes = yield* Notes;
  const params = { user: "ann" };
  yield* notes.create(params);
  yield* notes.append(params, ["hello", "world"]);
  const batches = yield* Stream.runCollect(notes.read(params));
  yield* notes.remove(params);

  return batches.flatMap((batch) => batch.items);
});

it("one application program runs unchanged on memory and remote streams", async () => {
  const local = await Effect.runPromise(program.pipe(Effect.provide(MemoryStorage.layer(group))));

  const edge = HttpRouter.toWebHandler(
    Http.routes(group, { prefix: "/streams", access: "full" }).pipe(
      Layer.provideMerge(MemoryStorage.layer(group)),
    ),
    { disableLogger: true },
  );

  const server = Bun.serve({ port: 0, fetch: (request) => edge.handler(request) });

  try {
    for (const host of [
      RemoteStreams.layer(group, { baseUrl: `${server.url.href}streams` }),
      RemoteStreams.layer(Streamsy.group(Notes), { baseUrl: `${server.url.href}streams` }),
    ]) {
      const remote = host.pipe(Layer.provide(FetchHttpClient.layer));

      const inspected = Effect.gen(function* () {
        const context = yield* Effect.context<Notes>();
        expect(Option.isNone(Context.getOption(context, StreamsReader))).toBe(true);
        expect(Option.isNone(Context.getOption(context, StreamsWriter))).toBe(true);
        expect(Option.isNone(Context.getOption(context, Streamsy.ChangeFeed(Notes)))).toBe(true);

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
