import { freePort } from "@streamsy/test-support/server";
import { BunServices } from "@effect/platform-bun";
import { layer as testLayer } from "@streamsy/effect-test";
import { FetchHttpClient, HttpClient } from "effect/http";
import { Layer } from "effect";

import { expect, test } from "bun:test";
import { Effect, Option } from "effect";
import { CheckpointStore } from "@streamsy/projection";
import { commandView } from "../server/command-view.ts";
import { SqlClient } from "effect/sql";
import { foldIssue, type IssueEvent } from "../domain/issue.ts";
import { applicationLayer, createInputs, sqliteStorage } from "../server/host.ts";
import { Events } from "../server/streams.ts";
import { transact, UnknownIssue } from "../server/state.ts";
import { scratchDirectory, startServer, stopServer, waitForServer } from "./support.ts";

testLayer(Layer.mergeAll(BunServices.layer, FetchHttpClient.layer), { excludeTestServices: true })(
  (it) => {
    const created: IssueEvent = {
      type: "IssueCreated",
      eventId: "e1",
      workspaceId: "live",
      issueId: "i1",
      projectId: "p1",
      title: "Issue",
      status: "todo",
      sequence: 3,
      occurredAt: "2026-09-20T10:00:00.000Z",
    };

    test("the fold keeps the newest sequence when an older fact arrives later", () => {
      const first = foldIssue(undefined, created);

      const older: IssueEvent = {
        type: "IssueStatusChanged",
        eventId: "e0",
        workspaceId: "live",
        issueId: "i1",
        status: "backlog",
        sequence: 2,
        occurredAt: "2026-09-20T09:00:00.000Z",
      };

      expect(foldIssue(first, older)).toBe(first);
    });

    it.effect(
      "transact decides from the in-memory command view and appends at the events offset",
      () =>
        Effect.gen(function* () {
          const runtime = yield* Layer.build(applicationLayer(sqliteStorage(":memory:")));

          yield* Effect.gen(function* () {
            yield* createInputs("live");
            const events = yield* Events;
            // A fact appended outside `transact`: the next command must see it.
            yield* events.append({ workspaceId: "live" }, [created]);

            const first = yield* transact("live", {
              type: "status",
              commandId: "c1",
              issueId: "i1",
              status: "done",
            });

            const assigned = yield* transact("live", {
              type: "assign",
              commandId: "c2",
              issueId: "i1",
              assigneeId: "ada",
            });

            expect(first.event).toMatchObject({ type: "IssueStatusChanged", sequence: 4 });
            expect(assigned.event).toMatchObject({
              status: "done",
              assigneeId: "ada",
              sequence: 5,
            });
            expect(assigned.offset).toBe((yield* events.head({ workspaceId: "live" })).nextOffset);

            for (const issueId of ["nope", "constructor", "toString"]) {
              const unknown = yield* Effect.flip(
                transact("live", { type: "status", commandId: "c3", issueId, status: "done" }),
              );

              expect(unknown).toBeInstanceOf(UnknownIssue);
            }

            expect(
              (yield* (yield* CheckpointStore).load(commandView.at({ workspaceId: "live" })))
                .record,
            ).toEqual(Option.none());
            const sql = yield* SqlClient.SqlClient;
            expect(yield* sql`SELECT * FROM issues`).toEqual([]);
            yield* transact("live", {
              type: "create",
              commandId: "c4",
              issueId: "constructor",
              projectId: "p1",
              title: "Named issue",
            });

            const named = yield* transact("live", {
              type: "status",
              commandId: "c5",
              issueId: "constructor",
              status: "done",
            });

            expect(named.event).toMatchObject({ status: "done", sequence: 7 });
            const { items } = yield* events.collect({ workspaceId: "live" });
            expect(items.map((item) => item.eventId)).toEqual(["e1", "c1", "c2", "c4", "c5"]);
          }).pipe(Effect.provideContext(runtime));
        }),
    );

    it.effect("the watcher writes rows after commands and external appends", () =>
      Effect.gen(function* () {
        const runtime = yield* Layer.build(applicationLayer(sqliteStorage(":memory:"), ["live"]));

        yield* Effect.gen(function* () {
          yield* transact("live", {
            type: "create",
            commandId: "both-1",
            issueId: "both",
            projectId: "p1",
            title: "Both",
            status: "todo",
          });
          const sql = yield* SqlClient.SqlClient;
          yield* (yield* Events).append({ workspaceId: "live" }, [
            { ...created, eventId: "change-2", issueId: "change-only", sequence: 1 },
          ]);
          let count = 0;

          for (let attempt = 0; attempt < 20 && count !== 2; attempt++) {
            const rows = yield* sql.unsafe<{ readonly n: number }>(
              "SELECT COUNT(*) AS n FROM issues",
            );

            count = rows[0]?.n ?? 0;

            if (count !== 2) yield* Effect.sleep("10 millis");
          }

          expect(count).toBe(2);
          expect(
            yield* sql.unsafe("SELECT issue_id, status FROM issues ORDER BY issue_id"),
          ).toEqual([
            { issue_id: "both", status: "todo" },
            { issue_id: "change-only", status: "todo" },
          ]);
        }).pipe(Effect.provideContext(runtime));
      }),
    );

    it.effect("an unknown workspace is a 404 and creates nothing", () =>
      Effect.gen(function* () {
        const scratch = yield* scratchDirectory("streamsy-issue-test");
        const port = yield* freePort;
        yield* Effect.acquireRelease(
          Effect.sync(() => startServer(port, scratch.database)),
          stopServer,
        );

        const baseUrl = `http://127.0.0.1:${port}`;
        yield* waitForServer(baseUrl);
        const response = yield* HttpClient.get(`${baseUrl}/api/workspaces/missing/status`);
        expect(response.status).toBe(404);

        for (const path of [
          "/state/workspaces/missing/issues",
          "/state/workspaces/missing/label-counts",
          "/feed/workspaces/missing/issue-transitions",
        ])
          expect((yield* HttpClient.get(`${baseUrl}${path}?offset=-1`)).status).toBe(404);
        expect((yield* HttpClient.get(`${baseUrl}/workspaces/missing/summary`)).status).toBe(200);
        expect((yield* HttpClient.get(`${baseUrl}/api/workspaces/live/commands`)).status).toBe(404);

        const stream = yield* HttpClient.get(
          `${baseUrl}/streams/bugsy/missing/issue-events?offset=-1`,
        );

        expect(stream.status).toBe(404);
      }),
    );
  },
);
