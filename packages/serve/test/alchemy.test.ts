import { expect, test } from "bun:test";
import { Effect, Exit, Schema } from "effect";
import { HttpServerError, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { StreamRoute } from "@streamsy/core";
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

test("Alchemy exports exactly the six host names", () => {
  expect(Object.keys(Host).toSorted()).toEqual([
    "ObjectOptions",
    "Placement",
    "alarm",
    "alarmLayer",
    "fetch",
    "router",
  ]);
});

test("Alchemy placement strips the prefix and preserves the request and response bodies", async () => {
  const names: Array<string> = [];
  const forwarded: Array<HttpServerRequest.HttpServerRequest> = [];
  const input = request("/streams/t1/a%2Fb?offset=-1");
  const output = HttpServerResponse.text("response body", { status: 202 });
  const response = await Effect.runPromise(
    Host.router({
      pathPrefix: "/streams",
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
    }).pipe(Effect.provideService(HttpServerRequest.HttpServerRequest, input)),
  );
  expect(names).toEqual(["t1"]);
  expect(forwarded).toEqual([input]);
  expect(await Effect.runPromise(input.text)).toBe("payload");
  expect(response).toBe(output);
  expect(await HttpServerResponse.toWeb(response).text()).toBe("response body");
});

test("Alchemy router shares prefix and placement failures without forwarding", async () => {
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
      placement: Host.Placement.byRoute([rule({ route, owner: ({ user }) => user })]),
      status: 400,
    },
  ];
  for (const row of cases) {
    const response = await Effect.runPromise(
      Host.router({
        pathPrefix: "/streams",
        placement: row.placement,
        objects: {
          getByName: () => {
            throw new Error("must not forward");
          },
        },
      }).pipe(Effect.provideService(HttpServerRequest.HttpServerRequest, request(row.path))),
    );
    expect(response.status).toBe(row.status);
    const web = HttpServerResponse.toWeb(response);
    expect(web.headers.get("x-content-type-options")).toBe("nosniff");
    expect(web.headers.get("cross-origin-resource-policy")).toBe("cross-origin");
  }
});

test("Alchemy router preserves the stub's HttpServerError", async () => {
  const input = request("/s");
  const error = new HttpServerError.HttpServerError({
    reason: new HttpServerError.RequestParseError({ request: input }),
  });
  const result = await Effect.runPromiseExit(
    Host.router({ objects: { getByName: () => ({ fetch: () => Effect.fail(error) }) } }).pipe(
      Effect.provideService(HttpServerRequest.HttpServerRequest, input),
    ),
  );
  expect(Exit.isFailure(result)).toBe(true);
  if (Exit.isFailure(result)) expect(result.cause.reasons).toMatchObject([{ _tag: "Fail", error }]);
});
