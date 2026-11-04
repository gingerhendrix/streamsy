// oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch, effecttsgo/global-fetch-in-effect -- Exercise actual bytes and headers on the public Bun listener.
import { expect, it } from "vitest";
import { Context, Effect, Layer, Match } from "effect";
import { MemoryStorage } from "@streamsy/core";
import { Cookies, HttpRouter, HttpServer, HttpServerResponse } from "effect/http";
import { listener } from "../src/bun.ts";
import { testHost } from "./support/scoped-host.ts";

it("keeps a plain-text protocol error's content type on the Bun listener", async () => {
  const host = await Effect.runPromise(testHost({ layer: MemoryStorage.layerProtocol(), port: 0 }));

  try {
    const response = await fetch(new URL("invalid", host.url), {
      method: "PUT",
      headers: { "stream-ttl": "-1" },
    });

    expect(response.status).toBe(400);
    expect(response.headers.get("content-type")).toBe("text/plain;charset=utf-8");
    expect(await response.text()).toBe("Invalid Stream-TTL format");
  } finally {
    await Effect.runPromise(host.stop);
  }
});

// Bun 1.4.2 does not enforce Fetch's immutable guards. Enforce the guard here so
// the regression still catches attempts to mutate a fetched or redirect response.
const immutable = () => {
  throw new TypeError("immutable headers");
};

const immutableHeaders = (response: Response): Response => {
  Object.defineProperties(response.headers, {
    set: { value: immutable },
    append: { value: immutable },
    delete: { value: immutable },
  });

  return response;
};

for (const kind of ["fetch", "redirect", "stream"] as const) {
  it(`serves an app's immutable ${kind} response without changing its headers`, async () => {
    const source = await Match.value(kind).pipe(
      Match.when("fetch", () => fetch("data:text/plain,immutable%20body")),
      Match.when("redirect", () => Response.redirect("https://example.com/target")),
      Match.when(
        "stream",
        () =>
          new Response(
            new ReadableStream<Uint8Array>({
              start(controller) {
                controller.enqueue(new TextEncoder().encode("streamed "));
                controller.enqueue(new TextEncoder().encode("body"));
                controller.close();
              },
            }),
          ),
      ),
      Match.exhaustive,
    );

    const original = immutableHeaders(source);

    const before = [...original.headers];

    expect(() => original.headers.set("x-probe", "mutated")).toThrow("immutable headers");

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(
            HttpRouter.serve(
              HttpRouter.add(
                "GET",
                "/immutable",
                HttpServerResponse.raw(original, {
                  status: original.status,
                  headers: { "x-outer": "copied" },
                  cookies: Cookies.setUnsafe(Cookies.empty, "session", "cookie"),
                }),
              ),
              { disableLogger: true, disableListenLog: true },
            ).pipe(Layer.provideMerge(listener({ port: 0 }))),
          );

          const server = Context.get(context, HttpServer.HttpServer);

          const response = yield* Effect.promise(() =>
            fetch(new URL("/immutable", HttpServer.formatAddress(server.address)), {
              redirect: "manual",
            }),
          );

          expect(response.status).toBe(original.status);
          expect(response.headers.get("x-outer")).toBe("copied");
          expect(response.headers.get("set-cookie")).toContain("session=cookie");

          if (kind === "fetch") {
            expect(response.headers.get("content-type")).toBe("text/plain;charset=utf-8");
            expect(yield* Effect.promise(() => response.text())).toBe("immutable body");
          } else if (kind === "stream") {
            expect(response.headers.get("content-type")).toBeNull();
            expect(yield* Effect.promise(() => response.text())).toBe("streamed body");
          } else {
            expect(response.headers.get("location")).toBe("https://example.com/target");
            expect(yield* Effect.promise(() => response.text())).toBe("");
          }

          expect([...original.headers]).toEqual(before);
        }),
      ),
    );
  });
}
