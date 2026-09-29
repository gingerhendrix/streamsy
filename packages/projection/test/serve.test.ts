import { Rows } from "@streamsy/core";
import { Commit } from "@streamsy/projection";
import * as FamilyFixture from "./support/families.ts";
import { Streamsy } from "@streamsy/core";
/* oxlint-disable effecttsgo/async-function, effecttsgo/strict-effect-provide -- These integration tests own the managed runtime and Web handler boundary. */
import { expect, test } from "bun:test";
import { Context, Effect, Layer, ManagedRuntime, Schema, Stream } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { Streams } from "@streamsy/core/protocol";
import { Projection } from "@streamsy/projection";
import * as Memory from "@streamsy/projection/memory";
import { Serve } from "@streamsy/serve";

const params = { seat: Schema.FiniteFromString };
class numbers extends Streamsy.json<numbers>()("serve.test/numbers", "numbers/:seat", {
  params,
  schema: Schema.Finite,
}) {}
const Card = Schema.Struct({ id: Schema.String, total: Schema.FiniteFromString });
class board extends Streamsy.state<board>()("serve.test/board", "board/:seat", {
  params,
  collections: { board: { schema: Card, key: "id" } },
}) {}
// The route owns the transforming codec used by projection writes and served reads.
class transitions extends Streamsy.json<transitions>()(
  "serve.test/transitions",
  "transitions/:seat",
  { params, schema: Schema.FiniteFromString },
) {}
const tracker = Projection.mapAccumBatch({
  commit: Commit.producer,
  id: "tracker",
  params,
  inputs: { numbers },
  outputs: {
    board: board,
    transitions: transitions,
  },
  state: { schema: Schema.Struct({ total: Schema.FiniteFromString }), initial: { total: 0 } },
  mapAccumBatch: (state, batch) =>
    Effect.sync(() => {
      const total = state.total + batch.numbers.items.reduce((sum, n) => sum + n, 0);
      return [
        { total },
        {
          board: [Rows.upsert({ id: "total", total }), Rows.remove("old")],
          transitions: [total],
        },
      ] as const;
    }),
});
class Seat extends Context.Service<Seat, { readonly seat: number }>()("join/Seat") {}
const auth = HttpRouter.middleware<{ provides: Seat }>()((handler) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    return request.headers.authorization === "Bearer seat"
      ? yield* Effect.provideService(handler, Seat, { seat: 42 })
      : HttpServerResponse.empty({ status: 401 });
  }),
);

test("declared rows, stream and state share one memory app and their declared codecs", async () => {
  expect(tracker.at({ seat: 42 }).streams.board?.stream.key).toBe(board.key);
  expect(tracker.at({ seat: 42 }).streams.transitions?.stream.key).toBe(transitions.key);
  const runtime = ManagedRuntime.make(
    FamilyFixture.withFamilies(Memory.layerMemory(), numbers, board, transitions),
  );
  const services = await runtime.runPromise(Effect.context());
  const App = Layer.mergeAll(
    Serve.state(board, "/board/:seat"),
    Serve.stream(transitions, "/transitions/:seat"),
    Serve.value(tracker.state, "/summary/:seat"),
    Serve.stream(transitions, "/me", { params: Seat }).pipe(Layer.provide(auth.layer)),
    Serve.state(board, "/my-board", { params: Seat }).pipe(Layer.provide(auth.layer)),
  );
  const web = HttpRouter.toWebHandler(
    App.pipe(HttpRouter.provideRequest(Layer.succeedContext(services))),
    { disableLogger: true },
  );
  try {
    const bound = tracker.at({ seat: 42 });
    expect(bound.params).toEqual({ seat: "42" });
    await runtime.runPromise(
      Effect.gen(function* () {
        yield* Streams.create(bound.inputs.numbers);
        yield* Streams.append(bound.inputs.numbers, [2, 3]);
        yield* Projection.run(bound);
        const changes = yield* Streams.read(board.at({ seat: 42 })).pipe(
          Streams.items,
          Stream.runCollect,
        );
        expect(changes).toEqual([
          {
            type: "board",
            key: "total",
            value: { id: "total", total: 5 },
            headers: { operation: "upsert" },
          },
          { type: "board", key: "old", headers: { operation: "delete" } },
        ]);
        const items = yield* Streams.read(transitions.at({ seat: 42 })).pipe(
          Streams.items,
          Stream.runCollect,
        );
        expect(items).toEqual([5]);
      }),
    );
    const rows = await web.handler(new Request("http://host/board/42?offset=-1"));
    expect(rows.status).toBe(200);
    expect(rows.headers.get("x-streamsy-state-version")).toBe("1");
    expect(await rows.json()).toEqual([
      {
        type: "board",
        key: "total",
        value: { id: "total", total: "5" },
        headers: { operation: "upsert" },
      },
      { type: "board", key: "old", headers: { operation: "delete" } },
    ]);
    expect(
      await (await web.handler(new Request("http://host/transitions/42?offset=-1"))).json(),
    ).toEqual(["5"]);
    const document = await web.handler(new Request("http://host/summary/42"));
    expect(document.status).toBe(200);
    expect(await document.text()).toBe('{"total":"5"}');
    const etag = document.headers.get("etag");
    expect(etag).toBeTruthy();
    expect(
      (
        await web.handler(
          new Request("http://host/summary/42", { headers: { "if-none-match": etag! } }),
        )
      ).status,
    ).toBe(304);
    for (const path of ["me", "my-board"]) {
      expect((await web.handler(new Request(`http://host/${path}`))).status).toBe(401);
      expect(
        (
          await web.handler(
            new Request(`http://host/${path}`, { headers: { authorization: "Bearer seat" } }),
          )
        ).status,
      ).toBe(200);
    }
    expect((await web.handler(new Request("http://host/unknown/42"))).status).toBe(404);
    expect((await web.handler(new Request("http://host/board/not-a-number"))).status).toBe(400);
    expect((await web.handler(new Request("http://host/board/99"))).status).toBe(404);
    expect((await web.handler(new Request("http://host/summary/99"))).status).toBe(503);
  } finally {
    await web.dispose();
    await runtime.dispose();
  }
});

// Compile-only probes: bad routes must never reach registration.
const typeChecks = () => {
  // @ts-expect-error route params must equal stream params
  Serve.state(board, "/board/:wrong");
  // @ts-expect-error missing stream parameter
  Serve.stream(transitions, "/transitions");
  // @ts-expect-error extra route parameter
  Serve.value(tracker.state, "/summary/:seat/:extra");
  // @ts-expect-error decoded params retain the number codec
  Serve.stream(transitions, "/me", { params: Effect.succeed({ seat: "42" }) });
  // @ts-expect-error a stream declaration does not supply rows collections
  Serve.state(transitions, "/board/:seat");
  const date = Projection.mapAccumBatch({
    commit: Commit.producer,
    id: "date",
    params: {},
    inputs: {},
    outputs: {},
    state: { schema: Schema.Struct({ date: Schema.Date }), initial: { date: new Date(0) } },
    mapAccumBatch: (state) => Effect.succeed([state, {}] as const),
  });
  // @ts-expect-error Date is not a JSON encoded value; rejected before requests
  Serve.value(date.state, "/date");
  const missingState = HttpRouter.toWebHandler(Serve.value(tracker.state, "/summary/:seat"));
  // @ts-expect-error value resolution requires CheckpointStore at the request boundary
  void missingState.handler(new Request("http://host/summary/42"));
  class wrongTarget extends Streamsy.json<wrongTarget>()("serve.test/wrongTarget", "wrong/:other", {
    params: { other: Schema.String },
    schema: Schema.Finite,
  }) {}
  Projection.emit({
    commit: Commit.producer,
    id: "wrong",
    params,
    inputs: { numbers },
    // @ts-expect-error an output target cannot require a parameter absent from the projection
    outputs: { events: wrongTarget },
    emit: () => Effect.succeed({ events: [1] }),
  });
  Projection.emit({
    commit: Commit.producer,
    id: "wrong-item",
    params,
    inputs: { numbers },
    outputs: { events: transitions },
    // @ts-expect-error output item types are retained through a routed identity
    emit: () => Effect.succeed({ events: ["bad"] }),
  });
  const fixed = Projection.mapAccumBatch({
    commit: Commit.producer,
    id: "fixed",
    params: {},
    inputs: { numbers: numbers.at({ seat: 42 }) },
    outputs: {
      board: board.at({ seat: 42 }),
      transitions: transitions.at({ seat: 42 }),
    },
    state: { schema: Schema.Struct({ total: Schema.FiniteFromString }), initial: { total: 0 } },
    mapAccumBatch: (state) => Effect.succeed([state, { board: [], transitions: [] }] as const),
  });
  Serve.state(board, "/board", { params: Effect.succeed({ seat: 42 }) });
  Serve.stream(transitions, "/transitions", { params: Effect.succeed({ seat: 42 }) });
  Serve.value(fixed.state, "/summary");
};
void typeChecks;
