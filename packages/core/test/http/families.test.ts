import { expect, it as baseIt } from "@effect/vitest";
import { Effect, Layer, Schema } from "effect";
import { HttpRouter } from "effect/http";
import { Http, Streamsy, MemoryStorage } from "../../src/index.ts";

class Inputs extends Streamsy.bytes<Inputs>()("Inputs", "inputs/:id", {
  params: { id: Schema.String },
}) {}

class Outputs extends Streamsy.bytes<Outputs>()("Outputs", "outputs/:id", {
  params: { id: Schema.String },
}) {}

baseIt.live(
  "HTTP chooses stream ownership and access before entering the complete protocol program",
  () =>
    Effect.gen(function* () {
      const edge = yield* Effect.acquireRelease(
        Effect.sync(() =>
          HttpRouter.toWebHandler(
            Http.routes(Streamsy.group(Inputs, Outputs), {
              prefix: "/streams",
              access: (stream) => (stream === Inputs ? "full" : "read"),
            }).pipe(
              HttpRouter.provideRequest(MemoryStorage.layer(Streamsy.group(Inputs, Outputs))),
            ),
            { disableLogger: true },
          ),
        ),
        (acquired) => Effect.promise(() => acquired.dispose()),
      );

      const request = (path: string, method = "GET", body?: string) =>
        edge.handler(
          new Request(`http://host/streams/${path}`, {
            method,
            // GET and HEAD callers pass no body; only PUT test requests carry data.
            // oxlint-disable-next-line unicorn/no-invalid-fetch-options -- The helper only supplies body for PUT.
            body,
            headers: { "content-type": "application/octet-stream" },
          }),
        );

      expect((yield* Effect.promise(() => request("private/secret"))).status).toBe(404);

      for (const method of ["PUT", "POST", "DELETE"])
        expect((yield* Effect.promise(() => request("outputs/a", method))).status).toBe(405);
      expect((yield* Effect.promise(() => request("inputs/a", "PUT", "hello"))).status).toBe(201);
      expect((yield* Effect.promise(() => request("outputs/a", "POST"))).headers.get("allow")).toBe(
        "GET, HEAD, OPTIONS",
      );
      expect(
        (yield* Effect.promise(() => request("outputs/a", "OPTIONS"))).headers.get(
          "access-control-allow-methods",
        ),
      ).toBe("GET, HEAD, OPTIONS");

      for (const source of ["/streams/private/secret", "/streams/outputs/a"]) {
        const fork = yield* Effect.promise(() =>
          edge.handler(
            new Request("http://host/streams/inputs/fork", {
              method: "PUT",
              headers: { "stream-forked-from": source },
            }),
          ),
        );

        expect(fork.status).toBe(404);
        expect(yield* Effect.promise(() => fork.text())).toContain("Source stream not found");
      }

      const head = yield* Effect.promise(() => request("inputs/a", "HEAD"));
      expect(head.status).toBe(200);
      expect(head.headers.get("stream-next-offset")).not.toBeNull();
      const read = yield* Effect.promise(() => request("inputs/a?offset=-1"));
      expect(read.status).toBe(200);
      expect(yield* Effect.promise(() => read.text())).toBe("hello");

      const readOnly = yield* Effect.acquireRelease(
        Effect.sync(() =>
          HttpRouter.toWebHandler(
            Http.routes(Streamsy.group(Outputs), { access: "read" }).pipe(
              HttpRouter.provideRequest(MemoryStorage.layer(Streamsy.group(Outputs))),
            ),
            { disableLogger: true },
          ),
        ),
        (acquired) => Effect.promise(() => acquired.dispose()),
      );

      expect(
        (yield* Effect.promise(() =>
          readOnly.handler(new Request("http://host/outputs/a", { method: "PUT" })),
        )).status,
      ).toBe(405);
      expect(
        (yield* Effect.promise(() =>
          readOnly.handler(new Request("http://host/outputs/a", { method: "OPTIONS" })),
        )).headers.get("access-control-allow-methods"),
      ).toBe("GET, HEAD, OPTIONS");
    }),
);

baseIt.live("append access allows POST but refuses creation and deletion", () =>
  Effect.gen(function* () {
    const streams = MemoryStorage.layer(Streamsy.group(Inputs));

    const seeded = Layer.effectDiscard(
      Effect.flatMap(Inputs, (inputs) => inputs.create({ id: "a" })),
    ).pipe(Layer.provideMerge(streams));

    const edge = yield* Effect.acquireRelease(
      Effect.sync(() =>
        HttpRouter.toWebHandler(
          Http.routes(Streamsy.group(Inputs), { access: "append" }).pipe(
            HttpRouter.provideRequest(seeded),
          ),
          { disableLogger: true },
        ),
      ),
      (acquired) => Effect.promise(() => acquired.dispose()),
    );

    for (const method of ["PUT", "DELETE"])
      expect(
        (yield* Effect.promise(() => edge.handler(new Request("http://host/inputs/a", { method }))))
          .status,
      ).toBe(405);
    expect(
      (yield* Effect.promise(() =>
        edge.handler(
          new Request("http://host/inputs/a", {
            method: "POST",
            body: "hello",
            headers: { "content-type": "application/octet-stream" },
          }),
        ),
      )).status,
    ).toBe(204);
    expect(
      yield* Effect.gen(function* () {
        const pending = (yield* Effect.promise(() =>
          edge.handler(new Request("http://host/inputs/a?offset=-1")),
        )).text();

        return yield* Effect.promise(() => pending);
      }),
    ).toBe("hello");
  }),
);

baseIt.live("a final catch-all cannot widen a declared read-only stream's HTTP access", () =>
  Effect.gen(function* () {
    const edge = yield* Effect.acquireRelease(
      Effect.sync(() =>
        HttpRouter.toWebHandler(
          Http.routes(Streamsy.group(Outputs, Streamsy.Wildcard), {
            access: (stream) => (stream === Outputs ? "read" : "full"),
          }).pipe(
            HttpRouter.provideRequest(
              MemoryStorage.layer(Streamsy.group(Outputs, Streamsy.Wildcard)),
            ),
          ),
          { disableLogger: true },
        ),
      ),
      (acquired) => Effect.promise(() => acquired.dispose()),
    );

    expect(
      (yield* Effect.promise(() =>
        edge.handler(new Request("http://host/outputs/private", { method: "PUT" })),
      )).status,
    ).toBe(405);
    expect(
      (yield* Effect.promise(() =>
        edge.handler(new Request("http://host/arbitrary/path", { method: "PUT" })),
      )).status,
    ).toBe(201);
  }),
);
