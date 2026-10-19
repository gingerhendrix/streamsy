import { BunHttpServer } from "@effect/platform-bun";
import { StorageFault } from "@streamsy/core";
import { Serve } from "@streamsy/serve";
import { it } from "@effect/vitest";
import { afterAll, describe, expect } from "vitest";
import { Effect, Exit, Layer, Schedule, Schema, Scope } from "effect";
import { HttpRouter } from "effect/http";
import { ApiLive, ApiNotFoundLive } from "../../server/api.ts";
import { BootLive, IssueTracker, IssueTrackerLive } from "../../server/issue-tracker.ts";
import { Workspaces } from "../../server/streams.ts";
import { fixtureTxid, MemoryHost, trackerOn } from "../support.ts";

// The same API and stream layers as the server, served in process over a memory
// store. The host is built once: the stream route reads its services per
// request, and a second build would be a second, empty store.
const hostScope = await Effect.runPromise(Scope.make());

const host = Layer.succeedContext(
  await Effect.runPromise(
    Layer.buildWithScope(BootLive.pipe(Layer.provideMerge(trackerOn(MemoryHost))), hostScope),
  ),
);

const { handler, dispose } = HttpRouter.toWebHandler(
  Layer.mergeAll(
    ApiLive,
    ApiNotFoundLive,
    Serve.mount(Workspaces, "/streams/workspace/:workspaceId"),
  ).pipe(
    Layer.provide(host),
    Layer.provide(BunHttpServer.layerHttpServices),
    HttpRouter.provideRequest(host),
  ),
  { disableLogger: true },
);

afterAll(() =>
  Effect.runPromise(
    Effect.promise(dispose).pipe(Effect.andThen(Scope.close(hostScope, Exit.void))),
  ),
);

const ErrorBody = Schema.Struct({ _tag: Schema.String, error: Schema.String });

const StateChanges = Schema.Array(
  Schema.Struct({
    type: Schema.String,
    key: Schema.String,
    headers: Schema.Struct({ txid: Schema.optionalKey(Schema.String) }),
  }),
);

class NoTxid extends Schema.TaggedError<NoTxid>()("NoTxid", {
  workspaceId: Schema.String,
  txid: Schema.String,
}) {}

const request = (input: Request) =>
  Effect.gen(function* () {
    const response = yield* Effect.promise(() => handler(input));

    return { status: response.status, body: yield* Effect.promise(() => response.json()) };
  });

const send = (method: string, path: string, body?: string) =>
  request(
    new Request(`http://localhost${path}`, {
      method,
      headers: { "content-type": "application/json" },
      body,
    }),
  );

/** Send `value` as the JSON body. */
const sendJson = (method: string, path: string, value: Schema.Json) =>
  Effect.flatMap(Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(value), (body) =>
    send(method, path, body),
  );

const sendError = (method: string, path: string, body?: string) =>
  Effect.gen(function* () {
    const { status, body: payload } = yield* send(method, path, body);

    return { status, ...(yield* Schema.decodeUnknownEffect(ErrorBody)(payload)) };
  });

/** Read the workspace's State stream until it holds a change with `txid`. */
const awaitTxid = (workspaceId: string, txid: string) =>
  request(new Request(`http://localhost/streams/workspace/${workspaceId}?offset=-1`)).pipe(
    Effect.flatMap(({ body }) => Schema.decodeUnknownEffect(StateChanges)(body)),
    Effect.map((changes) => changes.filter((change) => change.headers.txid === txid)),
    Effect.filterOrFail(
      (found) => found.length > 0,
      () => new NoTxid({ workspaceId, txid }),
    ),
    Effect.retry({ while: Schema.is(NoTxid), schedule: Schedule.spaced("10 millis"), times: 300 }),
  );

describe("domain errors map to the base app's status codes", () => {
  it.live("an unknown workspace answers 404", () =>
    Effect.gen(function* () {
      expect(yield* sendError("POST", "/api/w/nope12345/projects", `{}`)).toMatchObject({
        status: 404,
        error: "Unknown workspace",
      });
    }),
  );

  it.live("an issue under an unknown project answers 400", () =>
    Effect.gen(function* () {
      expect(yield* sendError("POST", "/api/w/main/issues", `{"projectId":"nope"}`)).toMatchObject({
        status: 400,
        error: "Unknown projectId",
      });
    }),
  );

  it.live("patching a missing issue answers 404", () =>
    Effect.gen(function* () {
      expect(
        yield* sendError("PATCH", "/api/w/main/issues/nope", `{"status":"done"}`),
      ).toMatchObject({
        status: 404,
        error: "Issue not found",
      });
    }),
  );

  it.live("a patch with nothing to change answers 400", () =>
    Effect.gen(function* () {
      expect(
        yield* sendError("PATCH", "/api/w/main/issues/issue_names", `{"updatedBy":"Ann"}`),
      ).toEqual({ status: 400, _tag: "BadRequest", error: "Nothing to change" });
    }),
  );

  it.live("a fourth issue in progress answers 422 WipLimitReached", () =>
    Effect.gen(function* () {
      const { body: workspace } = yield* send("POST", "/api/workspaces");

      const { body: project } = yield* sendJson("POST", `/api/w/${workspace.id}/projects`, {
        name: "Limits",
      });

      const start = () =>
        sendJson("POST", `/api/w/${workspace.id}/issues`, {
          projectId: project.project.id,
          status: "in_progress",
        });

      for (const created of yield* Effect.all([start(), start(), start()])) {
        expect(created.status).toBe(201);
      }

      expect(yield* start()).toEqual({
        status: 422,
        body: {
          _tag: "WipLimitReached",
          error: "This project already has 3 issues in progress",
        },
      });
    }),
  );

  it.live("a malformed workspace id answers 400 before any stream lookup", () =>
    Effect.gen(function* () {
      expect(yield* sendError("POST", "/api/w/-bad/projects", `{}`)).toEqual({
        status: 400,
        _tag: "BadRequest",
        error: "Invalid workspace id",
      });
    }),
  );
});

describe("successful writes", () => {
  it.live("a created issue answers 201, and its row reaches the State stream with the txid", () =>
    Effect.gen(function* () {
      const txid = fixtureTxid();

      const { status, body } = yield* sendJson("POST", "/api/w/main/issues", {
        projectId: "proj_streamsy",
        title: " Ship ",
        txid,
      });

      expect(status).toBe(201);
      expect(body).toMatchObject({ issue: { title: "Ship", status: "open" }, txid });
      expect(body.awaitOffset).toBeTypeOf("string");
      expect(yield* awaitTxid("main", txid)).toEqual([
        expect.objectContaining({ type: "issue", key: body.issue.id }),
      ]);
    }),
  );

  it.live("a patch with a title and a status lands as two rows with one txid", () =>
    Effect.gen(function* () {
      const txid = fixtureTxid();

      const { status, body } = yield* sendJson("PATCH", "/api/w/main/issues/issue%5Farticle", {
        title: "Part 2",
        status: "done",
        updatedBy: "Bo",
        txid,
      });

      expect(status).toBe(200);
      expect(body.issue).toMatchObject({ id: "issue_article", title: "Part 2", status: "done" });
      expect(yield* awaitTxid("main", txid)).toHaveLength(2);
    }),
  );

  it.live("a new workspace answers 201 with a shareable id and a starter project row", () =>
    Effect.gen(function* () {
      const { status, body } = yield* send("POST", "/api/workspaces");
      expect(status).toBe(201);
      expect(body.id).toMatch(/^[a-z0-9]{10}$/);

      const stream = yield* request(
        new Request(`http://localhost/streams/workspace/${body.id}?offset=-1`),
      );

      expect(stream.status).toBe(200);
    }),
  );
});

it.live("an operational failure is logged and answered as 500", () =>
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
    ).pipe(Layer.provide(IssueTrackerLive.pipe(Layer.provide(trackerOn(MemoryHost)))));

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
);
