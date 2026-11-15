import { expect, it } from "@effect/vitest";
import { Effect, Fiber, Layer, Schema } from "effect";
import { FetchHttpClient, HttpRouter } from "effect/http";
import { DurableState, Http, MemoryStorage, Offset, StreamId, Streamsy } from "@streamsy/core";
import { RemoteStreams } from "@streamsy/core/remote";

class Accepted extends Streamsy.wake<Accepted>()("RemoteAwait/Accepted", "accepted/:id", {
  params: { id: Schema.String },
}) {}

const group = Streamsy.group(Accepted);

it.live("awaitWake follows an accepted row over the remote layer", () =>
  Effect.gen(function* () {
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

    const remote = RemoteStreams.layer(group, { baseUrl: `${server.url.href}streams` }).pipe(
      Layer.provide(FetchHttpClient.layer),
    );

    yield* Effect.gen(function* () {
      const accepted = yield* Accepted;
      const params = { id: "one" };
      const stream = StreamId.make("source");
      const offset = Offset.make("0000000000000001_0000000000000000");
      yield* accepted.create(params);

      const waiting = yield* Streamsy.awaitWake(Accepted, params, { stream, offset }).pipe(
        Effect.forkScoped,
      );

      yield* accepted.append(
        params,
        DurableState.changes(Accepted.at(params), {}, [
          DurableState.upsert("stream", { stream, offset }),
        ]),
      );
      expect(yield* Fiber.join(waiting).pipe(Effect.timeout("2 seconds"))).toBeUndefined();
    }).pipe(Effect.provideContext(yield* Layer.build(remote)));
  }),
);
