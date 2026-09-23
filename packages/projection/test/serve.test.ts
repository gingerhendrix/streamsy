/* oxlint-disable effecttsgo/async-function, effecttsgo/strict-effect-provide -- These integration tests own the managed runtime and Web handler boundary. */
import { expect, test } from "bun:test";
import { Context, Effect, Layer, ManagedRuntime, Schema, Stream } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { StreamRoute, Streams } from "@streamsy/core";
import { Output, Projection } from "@streamsy/projection";
import * as Memory from "@streamsy/projection/memory";
import { Serve } from "@streamsy/serve";

const params = { seat: Schema.FiniteFromString };
const numbers = StreamRoute.json("numbers/:seat", { params, schema: Schema.Finite });
const board = StreamRoute.bytes("board/:seat", { params });
// This identity intentionally has a different codec; Output.stream owns encoding.
const transitions = StreamRoute.bytes("transitions/:seat", { params });
const Card = Schema.Struct({ id: Schema.String, total: Schema.FiniteFromString });
const tracker = Projection.family({
  id: "tracker",
  params,
  inputs: { numbers },
  outputs: {
    board: Output.rows(Card, { key: "id", stream: board }),
    transitions: Output.stream(Schema.FiniteFromString, { stream: transitions }),
    summary: Output.value(Schema.Struct({ total: Schema.FiniteFromString })),
  },
  process: Projection.fold({ total: 0 }, (state, batch) => {
    const total = state.total + batch.numbers.items.reduce((sum, n) => sum + n, 0);
    return {
      state: { total },
      board: [Output.upsert({ id: "total", total }), Output.remove("old")],
      transitions: [total],
    };
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

test("declared rows, stream and value share one memory app and their declared codecs", async () => {
  const runtime = ManagedRuntime.make(Memory.layerMemory());
  const services = await runtime.runPromise(Effect.context());
  const App = Layer.mergeAll(
    Serve.state(tracker.outputs.board, "/board/:seat"),
    Serve.stream(tracker.outputs.transitions, "/transitions/:seat"),
    Serve.document(tracker.outputs.summary, "/summary/:seat"),
    Serve.stream(tracker.outputs.transitions, "/me", { params: Seat }).pipe(
      Layer.provide(auth.layer),
    ),
    Serve.state(tracker.outputs.board, "/my-board", { params: Seat }).pipe(
      Layer.provide(auth.layer),
    ),
  );
  const web = HttpRouter.toWebHandler(
    App.pipe(HttpRouter.provideRequest(Layer.succeedContext(services))),
    { disableLogger: true },
  );
  try {
    const member = tracker.member({ seat: 42 });
    expect(member.params).toEqual({ seat: "42" });
    await runtime.runPromise(
      Effect.gen(function* () {
        yield* Streams.create(member.inputs.numbers);
        yield* Streams.append(member.inputs.numbers, [2, 3]);
        yield* Projection.run(member);
        const changes = yield* Streams.read(tracker.outputs.board.ref({ seat: 42 })).pipe(
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
        const items = yield* Streams.read(tracker.outputs.transitions.ref({ seat: 42 })).pipe(
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
  // @ts-expect-error route params must equal family params
  Serve.state(tracker.outputs.board, "/board/:wrong");
  // @ts-expect-error missing family parameter
  Serve.stream(tracker.outputs.transitions, "/transitions");
  // @ts-expect-error extra route parameter
  Serve.document(tracker.outputs.summary, "/summary/:seat/:extra");
  // @ts-expect-error decoded params retain the number codec
  Serve.stream(tracker.outputs.transitions, "/me", { params: Effect.succeed({ seat: "42" }) });
  // @ts-expect-error a stream declaration does not supply rows collections
  Serve.state(tracker.outputs.transitions, "/board/:seat");
  const date = Projection.outputs({
    id: "date",
    inputs: {},
    outputs: { date: Output.value(Schema.Struct({ date: Schema.Date })) },
    process: () => Effect.succeed({ state: { date: new Date() } }),
  });
  // @ts-expect-error Date is not a JSON encoded value; rejected before requests
  Serve.document(date.outputs.date, "/date");
  const missingState = HttpRouter.toWebHandler(
    Serve.document(tracker.outputs.summary, "/summary/:seat"),
  );
  // @ts-expect-error document resolution requires Projection.State at the request boundary
  void missingState.handler(new Request("http://host/summary/42"));
  const wrongTarget = StreamRoute.bytes("wrong/:other", { params: { other: Schema.String } });
  // @ts-expect-error an output target cannot require a parameter absent from the family
  Projection.family({
    id: "wrong",
    params,
    inputs: { numbers },
    outputs: { events: Output.stream(Schema.Finite, { stream: wrongTarget }) },
    process: () => Effect.succeed({ events: [1] }),
  });
  // @ts-expect-error output item types are retained through a routed identity
  Projection.family({
    id: "wrong-item",
    params,
    inputs: { numbers },
    outputs: { events: Output.stream(Schema.Finite, { stream: transitions }) },
    process: () => Effect.succeed({ events: ["bad"] }),
  });
  const fixed = tracker.member({ seat: 42 });
  Serve.state(fixed.outputs.board, "/board");
  Serve.stream(fixed.outputs.transitions, "/transitions");
  Serve.document(fixed.outputs.summary, "/summary");
};
void typeChecks;
