import { DateTime } from "effect";
import { it as baseIt } from "@effect/vitest";
import { BunCrypto, BunHttpServer } from "@effect/platform-bun";
import { MemoryStorage, Streamsy, StorageFault } from "@streamsy/core";
import { afterAll, describe, expect } from "vitest";
import { Effect, Layer, Schema } from "effect";
import { HttpRouter } from "effect/http";
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

function send(method: string, path: string, body?: string) {
  return Effect.gen(function* () {
    const response = yield* Effect.promise(() =>
      handler(
        new Request(`http://localhost${path}`, {
          method,
          headers: { "content-type": "application/json" },
          body,
        }),
      ),
    );

    return { status: response.status, body: yield* Effect.promise(() => response.json()) };
  });
}

function sendError(method: string, path: string, body?: string) {
  return Effect.gen(function* () {
    const { status, body: payload } = yield* send(method, path, body);

    return { status, ...(yield* Schema.decodeUnknownEffect(ErrorBody)(payload)) };
  });
}

describe("request decoding keeps the { error } contract", () => {
  baseIt.live("invalid JSON answers 400", () =>
    Effect.gen(function* () {
      const error = yield* sendError("POST", "/api/w/main/projects", "not json");
      expect(error).toMatchObject({ status: 400, _tag: "BadRequest" });
      expect(error.error).toContain("valid JSON");
    }),
  );

  baseIt.live.each([`[]`, `"text"`, `7`, `null`])("the non-object body %s answers 400", (body) =>
    Effect.gen(function* () {
      expect(yield* sendError("POST", "/api/w/main/projects", body)).toMatchObject({ status: 400 });
    }),
  );

  baseIt.live("a field of the wrong type is named in the message", () =>
    Effect.gen(function* () {
      const error = yield* sendError("POST", "/api/w/main/issues", `{"title":42}`);
      expect(error.status).toBe(400);
      expect(error.error).toContain("title");
    }),
  );

  baseIt.live("a txid that is not a UUID answers 400", () =>
    Effect.gen(function* () {
      const error = yield* sendError("POST", "/api/w/main/projects", `{"txid":"nope"}`);
      expect(error.status).toBe(400);
      expect(error.error).toContain("txid");
    }),
  );

  baseIt.live("a malformed workspace id answers 400 before any stream lookup", () =>
    Effect.gen(function* () {
      expect(yield* sendError("POST", "/api/w/-bad/projects", `{}`)).toEqual({
        status: 400,
        _tag: "BadRequest",
        error: "Invalid workspace id",
      });
    }),
  );
});

describe("domain errors map to status codes", () => {
  baseIt.live("an unknown workspace answers 404", () =>
    Effect.gen(function* () {
      expect(yield* sendError("POST", "/api/w/nope12345/projects", `{}`)).toMatchObject({
        status: 404,
        error: "Unknown workspace",
      });
    }),
  );

  baseIt.live("an issue under an unknown project answers 400", () =>
    Effect.gen(function* () {
      expect(yield* sendError("POST", "/api/w/main/issues", `{"projectId":"nope"}`)).toMatchObject({
        status: 400,
        error: "Unknown projectId",
      });
    }),
  );

  baseIt.live("patching a missing issue answers 404", () =>
    Effect.gen(function* () {
      expect(yield* sendError("PATCH", "/api/w/main/issues/nope", `{}`)).toMatchObject({
        status: 404,
        error: "Issue not found",
      });
    }),
  );

  baseIt.live("a fourth issue in progress answers 422 WipLimitReached", () =>
    Effect.gen(function* () {
      const { body: workspace } = yield* send("POST", "/api/workspaces");

      const { body: project } = yield* send(
        "POST",
        `/api/w/${workspace.id}/projects`,
        yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))({ name: "Limits" }),
      );

      const start = () =>
        send(
          "POST",
          `/api/w/${workspace.id}/issues`,
          Schema.encodeSync(Schema.fromJsonString(Schema.Json))({
            projectId: project.project.id,
            status: "in_progress",
          }),
        );

      for (let index = 0; index < 3; index += 1) expect((yield* start()).status).toBe(201);
      expect(yield* start()).toEqual({
        status: 422,
        body: {
          _tag: "WipLimitReached",
          error: "This project already has 3 issues in progress",
        },
      });
    }),
  );

  baseIt.live("other /api paths and methods answer a JSON 404", () =>
    Effect.gen(function* () {
      for (const [method, path] of [
        ["GET", "/api/w/main/projects"],
        ["POST", "/api/unknown"],
      ] as const) {
        expect(yield* send(method, path)).toEqual({ status: 404, body: { error: "Not found" } });
      }
    }),
  );
});

describe("successful writes", () => {
  baseIt.live("a created issue answers 201 with the entity, awaitOffset and the client txid", () =>
    Effect.gen(function* () {
      const txid = "00000000-0000-4000-8000-000000000001";

      const { status, body } = yield* send(
        "POST",
        "/api/w/main/issues",
        yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))({
          projectId: "proj_streamsy",
          title: " Ship ",
          txid,
        }),
      );

      expect(status).toBe(201);
      expect(body).toMatchObject({ issue: { title: "Ship", status: "open" }, txid });
      expect(body.awaitOffset).toBeTypeOf("string");
    }),
  );

  baseIt.live("the server stamps the times and keeps the writer's name", () =>
    Effect.gen(function* () {
      const before = DateTime.formatIso(DateTime.nowUnsafe());

      const { body } = yield* send(
        "POST",
        "/api/w/main/issues",
        yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))({
          projectId: "proj_streamsy",
          title: "Timed",
          updatedBy: "Ann",
          createdAt: "1999-01-01T00:00:00.000Z",
        }),
      );

      expect(body.issue.createdAt >= before).toBe(true);
      expect(body.issue.updatedAt).toBe(body.issue.createdAt);
      expect(body.issue.updatedBy).toBe("Ann");
    }),
  );

  baseIt.live("a patch decodes an escaped issue id and answers 200", () =>
    Effect.gen(function* () {
      const { status, body } = yield* send(
        "PATCH",
        "/api/w/main/issues/issue%5Fbootstrap",
        `{"status":"open"}`,
      );

      expect(status).toBe(200);
      expect(body.issue).toMatchObject({ id: "issue_bootstrap", status: "open" });
    }),
  );

  baseIt.live("a new workspace answers 201 with a shareable id", () =>
    Effect.gen(function* () {
      const { status, body } = yield* send("POST", "/api/workspaces");
      expect(status).toBe(201);
      expect(body.id).toMatch(/^[a-z0-9]{10}$/);
    }),
  );
});

baseIt.live("an operational failure is logged and answered as 500", () =>
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
);

baseIt.live("a typed workspace create failure answers 500", () =>
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
);
