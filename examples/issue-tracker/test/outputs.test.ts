import { expect, test } from "bun:test";
import { Streams, StreamsWriter, TransportFault } from "@streamsy/core";
import { Checkpoints, Projection } from "@streamsy/projection";
import { contractFingerprint } from "@streamsy/serve/contract";
import { Effect, Layer, ManagedRuntime, Stream } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { SqlClient } from "effect/unstable/sql";
import { applicationLayer, createInputs } from "../server/host.ts";
import { outputRoutes } from "../server/app.ts";
import { tracker } from "../server/outputs.ts";
import { issueRows } from "../server/projection.ts";
import { events, refs, userStream } from "../server/streams.ts";

const created = {
  type: "IssueCreated" as const,
  eventId: "e1",
  workspaceId: "live",
  issueId: "i1",
  projectId: "p1",
  title: "Issue",
  status: "todo" as const,
  sequence: 1,
  occurredAt: "2026-09-23T10:00:00Z",
};

test("key-only catalog deletes stay in the member workspace and advance both checkpoints", async () => {
  const runtime = ManagedRuntime.make(applicationLayer(":memory:"));
  try {
    await runtime.runPromise(
      Effect.gen(function* () {
        for (const workspaceId of ["live", "acme"]) {
          yield* createInputs(refs(workspaceId));
          yield* Streams.append(userStream(workspaceId), [
            {
              type: "user",
              key: "ada",
              value: {
                workspaceId,
                userId: "ada",
                name: "Ada",
                updatedAt: created.occurredAt,
              },
              headers: { operation: "upsert" },
            },
          ]);
          yield* Projection.run(issueRows.member({ workspaceId }));
          yield* Projection.run(tracker.member({ workspaceId }));
        }
        const owner = yield* Checkpoints;
        const sqlMember = issueRows.member({ workspaceId: "live" });
        const outputMember = tracker.member({ workspaceId: "live" });
        const sqlBefore = yield* owner.load(sqlMember);
        const outputBefore = yield* owner.load(outputMember);
        yield* Streams.append(userStream("live"), [
          { type: "user", key: "ada", headers: { operation: "delete" } },
        ]);
        yield* Projection.run(sqlMember);
        yield* Projection.run(outputMember);
        expect(yield* owner.load(sqlMember)).not.toEqual(sqlBefore);
        expect(yield* owner.load(outputMember)).not.toEqual(outputBefore);
        const sql = yield* SqlClient.SqlClient;
        expect(yield* sql.unsafe("SELECT workspace_id FROM users")).toEqual([
          { workspace_id: "acme" },
        ]);
        expect((yield* tracker.outputs.workspace.resolve({ workspaceId: "live" })).users).toEqual(
          [],
        );
        expect(
          (yield* tracker.outputs.workspace.resolve({ workspaceId: "acme" })).users,
        ).toHaveLength(1);
      }),
    );
  } finally {
    await runtime.dispose();
  }
});

test("catalog values cannot write another workspace in either projection", async () => {
  const runtime = ManagedRuntime.make(applicationLayer(":memory:"));
  try {
    await runtime.runPromise(
      Effect.gen(function* () {
        yield* createInputs(refs("live"));
        yield* Streams.append(events.ref({ workspaceId: "live" }), [created]);
        yield* Streams.append(userStream("live"), [
          {
            type: "user",
            key: "ada",
            value: {
              workspaceId: "acme",
              userId: "ada",
              name: "Ada",
              updatedAt: created.occurredAt,
            },
            headers: { operation: "upsert" },
          },
        ]);
        for (const member of [
          issueRows.member({ workspaceId: "live" }),
          tracker.member({ workspaceId: "live" }),
        ]) {
          const owner = yield* Checkpoints;
          const before = yield* owner.load(member);
          const result =
            member._tag === "Fused"
              ? yield* Projection.run(member).pipe(Effect.result)
              : yield* Projection.run(member).pipe(Effect.result);
          expect(result).toMatchObject({
            _tag: "Failure",
            failure: {
              _tag: "CatalogWorkspaceMismatch",
              message: "Catalog user/ada names workspace acme, but member workspace is live",
            },
          });
          expect(yield* owner.load(member)).toEqual(before);
        }
        const sql = yield* SqlClient.SqlClient;
        expect(yield* sql.unsafe("SELECT * FROM issues")).toEqual([]);
        expect(yield* sql.unsafe("SELECT * FROM users")).toEqual([]);
      }),
    );
  } finally {
    await runtime.dispose();
  }
});

test("named output replay survives a lost append reply beside the SQL projection", async () => {
  const runtime = ManagedRuntime.make(applicationLayer(":memory:"));
  try {
    await runtime.runPromise(
      Effect.gen(function* () {
        yield* createInputs(refs("live"));
        yield* Streams.append(events.ref({ workspaceId: "live" }), [created]);
        const member = tracker.member({ workspaceId: "live" });
        const writer = yield* StreamsWriter;
        const failed = yield* Projection.run(member).pipe(
          Effect.provideService(StreamsWriter, {
            ...writer,
            append: (id, options) =>
              writer.append(id, options).pipe(
                Effect.andThen(
                  Effect.fail(
                    new TransportFault({
                      reason: "response",
                      operation: "append",
                      message: "lost reply",
                    }),
                  ),
                ),
              ),
          }),
          Effect.result,
        );
        expect(failed._tag).toBe("Failure");
        // Advancing SQL and input history must not change reproduction of the pinned range.
        yield* Streams.append(events.ref({ workspaceId: "live" }), [
          { ...created, type: "IssueStatusChanged", eventId: "e2", status: "done", sequence: 2 },
        ]);
        yield* Projection.run(issueRows.member({ workspaceId: "live" }));
        yield* Projection.run(member);
        const transitions = yield* Streams.read(
          tracker.outputs.transitions.ref({ workspaceId: "live" }),
        ).pipe(Streams.items, Stream.runCollect);
        expect(transitions.map((event) => event.eventId)).toEqual(["e1", "e2"]);
        const board = yield* Streams.read(tracker.outputs.board.ref({ workspaceId: "live" })).pipe(
          Streams.items,
          Stream.runCollect,
        );
        expect(board).toHaveLength(2);
        expect(yield* tracker.outputs.workspace.resolve({ workspaceId: "live" })).toMatchObject({
          issueCount: 1,
          doneCount: 1,
        });
      }),
    );
  } finally {
    await runtime.dispose();
  }
});

test("served route fingerprints pin member identity and reject incompatible contracts without creating data", async () => {
  const runtime = ManagedRuntime.make(applicationLayer(":memory:"));
  const services = await runtime.runPromise(Effect.context());
  const web = HttpRouter.toWebHandler(
    outputRoutes.pipe(HttpRouter.provideRequest(Layer.succeedContext(services))),
    { disableLogger: true },
  );
  try {
    for (const [kind, name, suffix, collection, key] of [
      ["state", "board", "issues", "board", "issueId"],
      ["state", "labelCounts", "label-counts", "labelCounts", "labelId"],
      ["stream", "transitions", "issue-transitions", "", ""],
    ] as const) {
      const route = `/${kind === "state" ? "state" : "feed"}/workspaces/:workspaceId/${suffix}`;
      const url = `http://host${route.replace(":workspaceId", "live")}?offset=-1`;
      const header = `x-streamsy-${kind}-contract`;
      const response = await web.handler(new Request(url));
      expect(response.status).toBe(404);
      expect(response.headers.get(header)).toBe(
        contractFingerprint({
          kind,
          route,
          source: `issue-tracker/${name}`,
          id: `issue-tracker/live/outputs/${name === "labelCounts" ? "label-counts" : name}`,
          collections: kind === "state" ? { [collection]: key } : null,
          params: ["workspaceId"],
          version: 1,
          recovery: "replay-from-start",
          contract: null,
        }),
      );
      const rejected = await web.handler(new Request(url, { headers: { [header]: "outdated" } }));
      expect(rejected.status).toBe(409);
      expect(await rejected.json()).toMatchObject({ _tag: "ResumeRejected" });
      const other = await web.handler(new Request(url.replace("/live/", "/acme/")));
      expect(other.headers.get(header)).not.toBe(response.headers.get(header));
    }
    const document = await web.handler(new Request("http://host/document/workspaces/live/summary"));
    expect(document.status).toBe(503);
    expect(document.headers.get("x-streamsy-document-contract")).toBe(
      contractFingerprint({
        kind: "document",
        source: "issue-tracker/summary",
        route: "/document/workspaces/:workspaceId/summary",
        cache: "private, max-age=0, must-revalidate",
        contract: "compact-summary-v1",
      }),
    );
    const rejected = await web.handler(
      new Request("http://host/document/workspaces/live/summary", {
        headers: { "x-streamsy-document-contract": "outdated" },
      }),
    );
    expect(rejected.status).toBe(409);
    expect(await rejected.json()).toMatchObject({ _tag: "ContractChanged" });
    await runtime.runPromise(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        expect(yield* sql.unsafe("SELECT * FROM streamsy_projection_v1_state")).toEqual([]);
        const owner = yield* Checkpoints;
        expect((yield* owner.load(tracker.member({ workspaceId: "live" }))).record._tag).toBe(
          "None",
        );
      }),
    );
  } finally {
    await web.dispose();
    await runtime.dispose();
  }
});

test("compact summary excludes fold rows and keeps its ETag across a count-neutral assignment", async () => {
  const runtime = ManagedRuntime.make(applicationLayer(":memory:"));
  const services = await runtime.runPromise(Effect.context());
  const web = HttpRouter.toWebHandler(
    outputRoutes.pipe(HttpRouter.provideRequest(Layer.succeedContext(services))),
    { disableLogger: true },
  );
  try {
    await runtime.runPromise(
      Effect.gen(function* () {
        yield* createInputs(refs("live"));
        yield* Streams.append(events.ref({ workspaceId: "live" }), [created]);
        yield* Projection.run(tracker.member({ workspaceId: "live" }));
      }),
    );
    const response = await web.handler(new Request("http://host/document/workspaces/live/summary"));
    expect(await response.json()).toEqual({
      workspaceId: "live",
      issueCount: 1,
      doneCount: 0,
      labelCount: 0,
      projectCount: 0,
    });
    const etag = response.headers.get("etag");
    expect(etag).not.toBeNull();
    await runtime.runPromise(
      Effect.gen(function* () {
        yield* Streams.append(events.ref({ workspaceId: "live" }), [
          { ...created, type: "IssueAssigned", eventId: "e2", assigneeId: "ada", sequence: 2 },
        ]);
        yield* Projection.run(tracker.member({ workspaceId: "live" }));
        expect(
          (yield* tracker.outputs.workspace.resolve({ workspaceId: "live" })).issues[0]?.assigneeId,
        ).toBe("ada");
      }),
    );
    expect(
      (
        await web.handler(
          new Request("http://host/document/workspaces/live/summary", {
            headers: { "if-none-match": etag! },
          }),
        )
      ).status,
    ).toBe(304);
  } finally {
    await web.dispose();
    await runtime.dispose();
  }
});
