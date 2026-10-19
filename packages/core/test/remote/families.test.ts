import { expect, it as baseIt } from "@effect/vitest";
import { Context, Effect, Layer, Option, Schema, Stream } from "effect";
import { FetchHttpClient, HttpClient, HttpRouter } from "effect/http";
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

baseIt.live("one application program runs unchanged on memory and remote streams", () =>
  Effect.gen(function* () {
    const local = yield* program.pipe(
      Effect.provideContext(yield* Layer.build(MemoryStorage.layer(group))),
    );

    const edge = yield* Effect.acquireRelease(
      Effect.sync(() =>
        HttpRouter.toWebHandler(
          Http.routes(group, { prefix: "/streams", access: "full" }).pipe(
            Layer.provideMerge(MemoryStorage.layer(group)),
          ),
          { disableLogger: true },
        ),
      ),
      (acquired) => Effect.promise(() => acquired.dispose()),
    );

    const server = yield* Effect.acquireRelease(
      Effect.sync(() => Bun.serve({ port: 0, fetch: (request) => edge.handler(request) })),
      (acquired) => Effect.promise(() => acquired.stop(true)),
    );

    for (const host of [
      RemoteStreams.layer(group, { baseUrl: `${server.url.href}streams` }),
      RemoteStreams.layer(Streamsy.group(Notes), { baseUrl: `${server.url.href}streams` }),
    ]) {
      const remote = host.pipe(Layer.provide(FetchHttpClient.layer));

      const inspected = Effect.gen(function* () {
        const context = yield* Effect.context<Notes>();
        expect(Option.isNone(Context.getOption(context, StreamsReader))).toBe(true);
        expect(Option.isNone(Context.getOption(context, StreamsWriter))).toBe(true);

        return yield* program;
      });

      expect(yield* inspected.pipe(Effect.provideContext(yield* Layer.build(remote)))).toEqual(
        local,
      );
    }

    expect(local).toEqual(["hello", "world"]);
    const client = yield* Layer.build(FetchHttpClient.layer);

    const response = yield* HttpClient.put(`${server.url.href}streams/private/secret`).pipe(
      Effect.provideContext(client),
    );

    expect(response.status).toBe(404);
  }),
);
