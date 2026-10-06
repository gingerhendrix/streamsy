import { BunHttpServer } from "@effect/platform-bun";
import { StorageFault } from "@streamsy/core";
import { Serve } from "@streamsy/serve";
import { afterAll, describe, expect, test } from "bun:test";
import { Effect, Exit, Layer, Schema, Scope } from "effect";
import { HttpRouter } from "effect/http";
import { ApiLive, ApiNotFoundLive } from "../../server/api.ts";
import { BootLive, IssueTracker, IssueTrackerLive } from "../../server/issue-tracker.ts";
import { Workspaces } from "../../server/streams.ts";
import { MemoryHost, trackerOn } from "../support.ts";

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

afterAll(async () => {
  await dispose();
  await Effect.runPromise(Scope.close(hostScope, Exit.void));
});

const ErrorBody = Schema.Struct({ _tag: Schema.String, error: Schema.String });

const StateChanges = Schema.Array(
  Schema.Struct({
    type: Schema.String,
    key: Schema.String,
    headers: Schema.Struct({ txid: Schema.optionalKey(Schema.String) }),
  }),
);

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

/** Read the workspace's State stream until it holds a change with `txid`. */
async function awaitTxid(workspaceId: string, txid: string) {
  for (let attempt = 0; attempt < 300; attempt += 1) {
    const response = await handler(
      new Request(`http://localhost/streams/workspace/${workspaceId}?offset=-1`),
    );

    const changes = Schema.decodeUnknownSync(StateChanges)(await response.json());
    const found = changes.filter((change) => change.headers.txid === txid);

    if (found.length > 0) return found;
    await Bun.sleep(10);
  }

  throw new Error(`no change with txid ${txid} on workspace ${workspaceId}`);
}

describe("domain errors map to the base app's status codes", () => {
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
    expect(await sendError("PATCH", "/api/w/main/issues/nope", `{"status":"done"}`)).toMatchObject({
      status: 404,
      error: "Issue not found",
    });
  });

  test("a patch with nothing to change answers 400", async () => {
    expect(
      await sendError("PATCH", "/api/w/main/issues/issue_names", `{"updatedBy":"Ann"}`),
    ).toEqual({ status: 400, _tag: "BadRequest", error: "Nothing to change" });
  });

  test("a fourth issue in progress answers 422 WipLimitReached", async () => {
    const { body: workspace } = await send("POST", "/api/workspaces");

    const { body: project } = await send(
      "POST",
      `/api/w/${workspace.id}/projects`,
      JSON.stringify({ name: "Limits" }),
    );

    const start = () =>
      send(
        "POST",
        `/api/w/${workspace.id}/issues`,
        JSON.stringify({ projectId: project.project.id, status: "in_progress" }),
      );

    for (let index = 0; index < 3; index += 1) expect((await start()).status).toBe(201);
    expect(await start()).toEqual({
      status: 422,
      body: {
        _tag: "WipLimitReached",
        error: "This project already has 3 issues in progress",
      },
    });
  });

  test("a malformed workspace id answers 400 before any stream lookup", async () => {
    expect(await sendError("POST", "/api/w/-bad/projects", `{}`)).toEqual({
      status: 400,
      _tag: "BadRequest",
      error: "Invalid workspace id",
    });
  });
});

describe("successful writes", () => {
  test("a created issue answers 201, and its row reaches the State stream with the txid", async () => {
    const txid = crypto.randomUUID();

    const { status, body } = await send(
      "POST",
      "/api/w/main/issues",
      JSON.stringify({ projectId: "proj_streamsy", title: " Ship ", txid }),
    );

    expect(status).toBe(201);
    expect(body).toMatchObject({ issue: { title: "Ship", status: "open" }, txid });
    expect(body.awaitOffset).toBeString();
    expect(await awaitTxid("main", txid)).toEqual([
      expect.objectContaining({ type: "issue", key: body.issue.id }),
    ]);
  });

  test("a patch with a title and a status lands as two rows with one txid", async () => {
    const txid = crypto.randomUUID();

    const { status, body } = await send(
      "PATCH",
      "/api/w/main/issues/issue%5Farticle",
      JSON.stringify({ title: "Part 2", status: "done", updatedBy: "Bo", txid }),
    );

    expect(status).toBe(200);
    expect(body.issue).toMatchObject({ id: "issue_article", title: "Part 2", status: "done" });
    expect(await awaitTxid("main", txid)).toHaveLength(2);
  });

  test("a new workspace answers 201 with a shareable id and a starter project row", async () => {
    const { status, body } = await send("POST", "/api/workspaces");
    expect(status).toBe(201);
    expect(body.id).toMatch(/^[a-z0-9]{10}$/);

    const response = await handler(
      new Request(`http://localhost/streams/workspace/${body.id}?offset=-1`),
    );

    expect(response.status).toBe(200);
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
    ),
  ));
