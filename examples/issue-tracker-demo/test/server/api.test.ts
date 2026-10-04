import { BunCrypto, BunHttpServer } from "@effect/platform-bun";
import { MemoryStorage, Streamsy, StorageFault } from "@streamsy/core";
import { afterAll, describe, expect, test } from "bun:test";
import { Effect, Layer, Schema } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { ApiLive, ApiNotFoundLive } from "../../server/api.ts";
import { IssueTracker, IssueTrackerLive, SeedLive } from "../../server/issue-tracker.ts";
import { Workspaces } from "../../server/streams.ts";

// The same API layers as the server, served in process over a memory store.
const { handler, dispose } = HttpRouter.toWebHandler(
  Layer.mergeAll(ApiLive, ApiNotFoundLive).pipe(
    Layer.provide(SeedLive),
    Layer.provide(IssueTrackerLive),
    Layer.provide([
      MemoryStorage.layer(Streamsy.group(Workspaces)),
      BunCrypto.layer,
      BunHttpServer.layerHttpServices,
    ]),
  ),
  { disableLogger: true },
);
afterAll(dispose);

const ErrorBody = Schema.Struct({ _tag: Schema.String, error: Schema.String });

async function send(method: string, path: string, body?: string) {
  const response = await handler(
    new Request(`http://localhost${path}`, {
      method,
      headers: { "content-type": "application/json" },
      body,
    }),
  );
  return { status: response.status, body: await response.json() };
}

async function sendError(method: string, path: string, body?: string) {
  const { status, body: payload } = await send(method, path, body);
  return { status, ...Schema.decodeUnknownSync(ErrorBody)(payload) };
}

describe("request decoding keeps the { error } contract", () => {
  test("invalid JSON answers 400", async () => {
    const error = await sendError("POST", "/api/w/main/projects", "not json");
    expect(error).toMatchObject({ status: 400, _tag: "BadRequest" });
    expect(error.error).toContain("valid JSON");
  });

  test.each([`[]`, `"text"`, `7`, `null`])("the non-object body %s answers 400", async (body) => {
    expect(await sendError("POST", "/api/w/main/projects", body)).toMatchObject({ status: 400 });
  });

  test("a field of the wrong type is named in the message", async () => {
    const error = await sendError("POST", "/api/w/main/issues", `{"title":42}`);
    expect(error.status).toBe(400);
    expect(error.error).toContain("title");
  });

  test("a txid that is not a UUID answers 400", async () => {
    const error = await sendError("POST", "/api/w/main/projects", `{"txid":"nope"}`);
    expect(error.status).toBe(400);
    expect(error.error).toContain("txid");
  });

  test("a malformed workspace id answers 400 before any stream lookup", async () => {
    expect(await sendError("POST", "/api/w/-bad/projects", `{}`)).toEqual({
      status: 400,
      _tag: "BadRequest",
      error: "Invalid workspace id",
    });
  });
});

describe("domain errors map to status codes", () => {
  test("an unknown workspace answers 404", async () => {
    expect(await sendError("POST", "/api/w/nope12345/projects", `{}`)).toMatchObject({
      status: 404,
      error: "Unknown workspace",
    });
  });

  test("an issue under an unknown project answers 400", async () => {
    expect(await sendError("POST", "/api/w/main/issues", `{"projectId":"nope"}`)).toMatchObject({
      status: 400,
      error: "Unknown projectId",
    });
  });

  test("patching a missing issue answers 404", async () => {
    expect(await sendError("PATCH", "/api/w/main/issues/nope", `{}`)).toMatchObject({
      status: 404,
      error: "Issue not found",
    });
  });

  test("other /api paths and methods answer a JSON 404", async () => {
    for (const [method, path] of [
      ["GET", "/api/w/main/projects"],
      ["POST", "/api/unknown"],
    ] as const) {
      expect(await send(method, path)).toEqual({ status: 404, body: { error: "Not found" } });
    }
  });
});

describe("successful writes", () => {
  test("a created issue answers 201 with the entity, awaitOffset and the client txid", async () => {
    const txid = crypto.randomUUID();
    const { status, body } = await send(
      "POST",
      "/api/w/main/issues",
      JSON.stringify({ projectId: "proj_streamsy", title: " Ship ", txid }),
    );
    expect(status).toBe(201);
    expect(body).toMatchObject({ issue: { title: "Ship", status: "open" }, txid });
    expect(body.awaitOffset).toBeString();
  });

  test("a patch decodes an escaped issue id and answers 200", async () => {
    const { status, body } = await send(
      "PATCH",
      "/api/w/main/issues/issue%5Fbootstrap",
      `{"status":"open"}`,
    );
    expect(status).toBe(200);
    expect(body.issue).toMatchObject({ id: "issue_bootstrap", status: "open" });
  });

  test("a new workspace answers 201 with a shareable id", async () => {
    const { status, body } = await send("POST", "/api/workspaces");
    expect(status).toBe(201);
    expect(body.id).toMatch(/^[a-z0-9]{10}$/);
  });
});

test("an operational failure is logged and answered as 500", () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const fault = new StorageFault({
          operation: "read",
          message: "store busy",
          retryable: true,
        });
        const failingTracker = Layer.effect(
          IssueTracker,
          Effect.map(IssueTracker, (tracker) => ({
            ...tracker,
            mutate: () => Effect.fail(fault),
          })),
        ).pipe(
          Layer.provide(IssueTrackerLive),
          Layer.provide([MemoryStorage.layer(Streamsy.group(Workspaces)), BunCrypto.layer]),
        );
        const failing = yield* Effect.acquireRelease(
          Effect.sync(() =>
            HttpRouter.toWebHandler(
              ApiLive.pipe(
                Layer.provide(failingTracker),
                Layer.provide(BunHttpServer.layerHttpServices),
              ),
              { disableLogger: true },
            ),
          ),
          (server) => Effect.promise(() => server.dispose()),
        );
        const response = yield* Effect.promise(() =>
          failing.handler(
            new Request("http://localhost/api/w/main/projects", {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: "{}",
            }),
          ),
        );
        expect(response.status).toBe(500);
        expect(yield* Effect.promise(() => response.json())).toMatchObject({
          error: "Internal server error",
        });
      }),
    ),
  ));

test("a typed workspace create failure answers 500", () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const fault = new StorageFault({
          operation: "create",
          message: "store busy",
          retryable: true,
        });
        const failingTracker = Layer.effect(
          IssueTracker,
          Effect.map(IssueTracker, (tracker) => ({
            ...tracker,
            create: Effect.fail(fault),
          })),
        ).pipe(
          Layer.provide(IssueTrackerLive),
          Layer.provide([MemoryStorage.layer(Streamsy.group(Workspaces)), BunCrypto.layer]),
        );
        const failing = yield* Effect.acquireRelease(
          Effect.sync(() =>
            HttpRouter.toWebHandler(
              ApiLive.pipe(
                Layer.provide(failingTracker),
                Layer.provide(BunHttpServer.layerHttpServices),
              ),
              { disableLogger: true },
            ),
          ),
          (server) => Effect.promise(() => server.dispose()),
        );
        const response = yield* Effect.promise(() =>
          failing.handler(new Request("http://localhost/api/workspaces", { method: "POST" })),
        );
        expect(response.status).toBe(500);
        expect(yield* Effect.promise(() => response.json())).toMatchObject({
          error: "Internal server error",
        });
      }),
    ),
  ));
