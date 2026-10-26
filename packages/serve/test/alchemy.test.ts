import { expect, it as baseIt } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import { HttpServerError, HttpServerRequest, HttpServerResponse } from "effect/http";
import { StreamRoute } from "@streamsy/core/protocol";
import * as Host from "@streamsy/serve/alchemy";
import { rule } from "../src/cloudflare/placement.ts";

const request = (path: string) =>
  HttpServerRequest.fromWeb(
    new Request(`https://streams.test${path}`, {
      method: "POST",
      body: "payload",
      headers: { "stream-forked-from": "/different/source" },
    }),
  );

baseIt.effect(
  "Alchemy placement strips the prefix and preserves the request and response bodies",
  () =>
    Effect.gen(function* () {
      const names: Array<string> = [];
      const forwarded: Array<HttpServerRequest.HttpServerRequest> = [];
      const input = request("/streams/t1/a%2Fb?offset=-1");
      const output = HttpServerResponse.text("response body", { status: 202 });

      const response = yield* Host.router({
        prefix: "/streams",
        placement: Host.Placement.byKey((path) => path.split("/")[0] ?? ""),
        objects: {
          getByName: (name) => {
            names.push(name);

            return {
              fetch: (received) => {
                forwarded.push(received);

                return Effect.succeed(output);
              },
            };
          },
        },
      }).pipe(Effect.provideService(HttpServerRequest.HttpServerRequest, input));

      expect(names).toEqual(["t1"]);
      expect(forwarded).toEqual([input]);
      expect(yield* input.text).toBe("payload");
      expect(response).toBe(output);
      expect(yield* Effect.promise(() => HttpServerResponse.toWeb(response).text())).toBe(
        "response body",
      );
    }),
);

baseIt.effect("Alchemy router shares prefix and placement failures without forwarding", () =>
  Effect.gen(function* () {
    const route = StreamRoute.json("journal/:user", {
      params: { user: Schema.String },
      schema: Schema.String,
    });

    const cases = [
      { path: "/outside/a", placement: Host.Placement.byStream(), status: 400 },
      { path: "/streams", placement: Host.Placement.byStream(), status: 400 },
      { path: "/streams/a", placement: Host.Placement.byKey(() => ""), status: 400 },
      {
        path: "/streams/a",
        placement: Host.Placement.byKey(() => {
          throw new Error("bad owner");
        }),
        status: 500,
      },
      {
        path: "/streams/other/a",
        placement: Host.Placement.byRoute([rule({ stream: route, owner: ({ user }) => user })]),
        status: 400,
      },
    ];

    for (const row of cases) {
      const response = yield* Host.router({
        prefix: "/streams",
        placement: row.placement,
        objects: {
          getByName: () => {
            throw new Error("must not forward");
          },
        },
      }).pipe(Effect.provideService(HttpServerRequest.HttpServerRequest, request(row.path)));

      expect(response.status).toBe(row.status);
      const web = HttpServerResponse.toWeb(response);
      expect(web.headers.get("x-content-type-options")).toBe("nosniff");
      expect(web.headers.get("cross-origin-resource-policy")).toBe("cross-origin");
    }
  }),
);

baseIt.effect("Alchemy router preserves the stub's HttpServerError", () =>
  Effect.gen(function* () {
    const input = request("/s");

    const error = new HttpServerError.HttpServerError({
      reason: new HttpServerError.RequestParseError({ request: input }),
    });

    const result = yield* Effect.exit(
      Host.router({ objects: { getByName: () => ({ fetch: () => Effect.fail(error) }) } }).pipe(
        Effect.provideService(HttpServerRequest.HttpServerRequest, input),
      ),
    );

    expect(Exit.isFailure(result)).toBe(true);

    if (Exit.isFailure(result))
      expect(result.cause.reasons).toMatchObject([{ _tag: "Fail", error }]);
  }),
);

baseIt.effect("Alchemy router answers 404 for a reserved /__ path without forwarding", () =>
  Effect.gen(function* () {
    const response = yield* Host.router({
      placement: Host.Placement.byKey(() => "index"),
      objects: {
        getByName: () => {
          throw new Error("must not forward");
        },
      },
    }).pipe(Effect.provideService(HttpServerRequest.HttpServerRequest, request("/__wake/wake")));

    expect(response.status).toBe(404);
    expect(HttpServerResponse.toWeb(response).headers.get("x-content-type-options")).toBe(
      "nosniff",
    );
  }),
);

baseIt.effect(
  "Alchemy objectFetch sends a web request to the named stub and returns its answer",
  () =>
    Effect.gen(function* () {
      const names: Array<string> = [];

      const send = Host.objectFetch({
        getByName: (name) => {
          names.push(name);

          return {
            fetch: (received) =>
              Effect.map(received.text, (body) =>
                HttpServerResponse.text(`${received.method} ${body}`, { status: 201 }),
              ),
          };
        },
      });

      expect(names).toEqual([]);

      const response = yield* send(
        "index",
        new Request("https://streamsy.internal/__wake/wake", { method: "POST", body: "row" }),
      );

      expect(names).toEqual(["index"]);
      expect(response.status).toBe(201);
      expect(yield* Effect.promise(() => response.text())).toBe("POST row");
    }),
);

baseIt.effect("Alchemy objectFetch preserves a stub failure as the transport error cause", () =>
  Effect.gen(function* () {
    const input = request("/__wake/wake");

    const failure = new HttpServerError.HttpServerError({
      reason: new HttpServerError.RequestParseError({ request: input }),
    });

    const send = Host.objectFetch({
      getByName: () => ({ fetch: () => Effect.fail(failure) }),
    });

    const error = yield* send("owner", new Request("https://streamsy.internal/__wake/wake")).pipe(
      Effect.flip,
    );

    expect(error).toMatchObject({
      _tag: "HttpClientError",
      reason: { _tag: "TransportError", cause: failure },
    });
  }),
);
