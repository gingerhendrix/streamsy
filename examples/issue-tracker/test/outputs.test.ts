import * as Addresses from "./support/addresses.ts";
import { expect, test } from "bun:test";
import { TransportFault } from "@streamsy/core";
import { CheckpointStore, Projection } from "@streamsy/projection";
import { contractFingerprint } from "@streamsy/serve/contract";
import { Effect, Layer, ManagedRuntime } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { SqlClient } from "effect/unstable/sql";
import { applicationLayer, createInputs } from "../server/host.ts";
import { outputRoutes } from "../server/app.ts";
import { tracker, Board, Transitions } from "../server/outputs.ts";
import { issueRows } from "../server/projection.ts";
import { Events, Users } from "../server/streams.ts";

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

test("key-only catalog deletes stay in the address workspace and advance both checkpoints", async () => {
  const runtime = ManagedRuntime.make(applicationLayer(":memory:"));
  try {
    await runtime.runPromise(
      Effect.gen(function* () {
        for (const workspaceId of ["live", "acme"]) {
          yield* createInputs(workspaceId);
          yield* (yield* Users).append({ workspaceId: workspaceId }, [
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
          yield* Projection.run(issueRows.at({ workspaceId }));
          yield* Projection.run(tracker.at({ workspaceId }));
        }
        const owner = yield* CheckpointStore;
        const rows = issueRows.at({ workspaceId: "live" });
        const outputs = tracker.at({ workspaceId: "live" });
        const sqlBefore = yield* owner.load(rows);
        const outputBefore = yield* owner.load(outputs);
        yield* (yield* Users).append({ workspaceId: "live" }, [
          { type: "user", key: "ada", headers: { operation: "delete" } },
        ]);
        yield* Projection.run(rows);
        yield* Projection.run(outputs);
        expect(yield* owner.load(rows)).not.toEqual(sqlBefore);
        expect(yield* owner.load(outputs)).not.toEqual(outputBefore);
        const sql = yield* SqlClient.SqlClient;
        expect(yield* sql.unsafe("SELECT workspace_id FROM users")).toEqual([
          { workspace_id: "acme" },
        ]);
        expect(
          (yield* Projection.read(tracker.at({ workspaceId: "live" })).pipe(
            Effect.map(({ state }) => state),
          )).users,
        ).toEqual([]);
        expect(
          (yield* Projection.read(tracker.at({ workspaceId: "acme" })).pipe(
            Effect.map(({ state }) => state),
          )).users,
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
        yield* createInputs("live");
        yield* (yield* Events).append({ workspaceId: "live" }, [created]);
        yield* (yield* Users).append({ workspaceId: "live" }, [
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
        for (const [address, run] of [
          [
            issueRows.at({ workspaceId: "live" }),
            Projection.run(issueRows.at({ workspaceId: "live" })).pipe(Effect.result),
          ],
          [
            tracker.at({ workspaceId: "live" }),
            Projection.run(tracker.at({ workspaceId: "live" })).pipe(Effect.result),
          ],
        ] as const) {
          const owner = yield* CheckpointStore;
          const before = yield* owner.load(address);
          const result = yield* run;
          expect(result).toMatchObject({
            _tag: "Failure",
            failure: {
              _tag: "CatalogWorkspaceMismatch",
              message: "Catalog user/ada names workspace acme, but address workspace is live",
            },
          });
          expect(yield* owner.load(address)).toEqual(before);
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
        yield* createInputs("live");
        yield* (yield* Events).append({ workspaceId: "live" }, [created]);
        const bound = tracker.at({ workspaceId: "live" });
        const failed = yield* Projection.run(bound).pipe(
          Addresses.patchOutputAppends(
            (append) => (id, options) =>
              append(id, options).pipe(
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
          ),
          Effect.result,
        );
        expect(failed._tag).toBe("Failure");
        // Advancing SQL and input history must not change reproduction of the pinned range.
        yield* (yield* Events).append({ workspaceId: "live" }, [
          { ...created, type: "IssueStatusChanged", eventId: "e2", status: "done", sequence: 2 },
        ]);
        yield* Projection.run(issueRows.at({ workspaceId: "live" }));
        yield* Projection.run(bound);
        const transitions = yield* Addresses.readOutput(Transitions, "live");
        expect(transitions.map((event) => event.eventId)).toEqual(["e1", "e2"]);
        const board = yield* Addresses.readOutput(Board, "live");
        expect(board).toHaveLength(2);
        expect(
          yield* Projection.read(tracker.at({ workspaceId: "live" })).pipe(
            Effect.map(({ state }) => state),
          ),
        ).toMatchObject({
          issueCount: 1,
          doneCount: 1,
        });
      }),
    );
  } finally {
    await runtime.dispose();
  }
});

test("served route fingerprints pin address identity and reject incompatible contracts without creating data", async () => {
  const runtime = ManagedRuntime.make(applicationLayer(":memory:"));
  const services = await runtime.runPromise(Effect.context());
  const web = HttpRouter.toWebHandler(
    outputRoutes.pipe(
      Layer.provide(Layer.succeedContext(services)),
      HttpRouter.provideRequest(Layer.succeedContext(services)),
    ),
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
          source: `issue-tracker/:workspaceId/outputs/${name === "labelCounts" ? "label-counts" : name}`,
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
    const value = await web.handler(new Request("http://host/workspaces/live/summary"));
    expect(value.status).toBe(200);
    expect(await value.json()).toEqual({
      workspaceId: "live",
      issueCount: 0,
      doneCount: 0,
      labelCount: 0,
      projectCount: 0,
    });
    expect(value.headers.get("x-streamsy-value-contract")).toBe(
      contractFingerprint({
        kind: "value",
        route: "/workspaces/:workspaceId/summary",
        cache: "private, max-age=0, must-revalidate",
        contract: "compact-summary-v1",
      }),
    );
    const rejected = await web.handler(
      new Request("http://host/workspaces/live/summary", {
        headers: { "x-streamsy-value-contract": "outdated" },
      }),
    );
    expect(rejected.status).toBe(409);
    expect(await rejected.json()).toMatchObject({ _tag: "ContractChanged" });
    await runtime.runPromise(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        expect(yield* sql.unsafe("SELECT * FROM streamsy_projection_v2_records")).toEqual([]);
        const owner = yield* CheckpointStore;
        expect((yield* owner.load(tracker.at({ workspaceId: "live" }))).record._tag).toBe("None");
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
    outputRoutes.pipe(
      Layer.provide(Layer.succeedContext(services)),
      HttpRouter.provideRequest(Layer.succeedContext(services)),
    ),
    { disableLogger: true },
  );
  try {
    await runtime.runPromise(
      Effect.gen(function* () {
        yield* createInputs("live");
        yield* (yield* Events).append({ workspaceId: "live" }, [created]);
        yield* Projection.run(tracker.at({ workspaceId: "live" }));
      }),
    );
    const response = await web.handler(new Request("http://host/workspaces/live/summary"));
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
        yield* (yield* Events).append({ workspaceId: "live" }, [
          { ...created, type: "IssueAssigned", eventId: "e2", assigneeId: "ada", sequence: 2 },
        ]);
        yield* Projection.run(tracker.at({ workspaceId: "live" }));
        expect(
          (yield* Projection.read(tracker.at({ workspaceId: "live" })).pipe(
            Effect.map(({ state }) => state),
          )).issues[0]?.assigneeId,
        ).toBe("ada");
      }),
    );
    expect(
      (
        await web.handler(
          new Request("http://host/workspaces/live/summary", {
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
