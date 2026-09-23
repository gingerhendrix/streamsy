import { expect, test } from "bun:test";
import { Streams } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Effect, ManagedRuntime, Schema, Stream } from "effect";
import { initialWorkspace, type WorkspaceState } from "../domain/outputs.ts";
import { applicationLayer, createInputs } from "../server/host.ts";
import { tracker } from "../server/outputs.ts";
import {
  events,
  labelEvents,
  labelStream,
  projectStream,
  refs,
  userStream,
} from "../server/streams.ts";

// Pre-index full-scan builders are an oracle for wire payloads and ordering.
const cards = (state: WorkspaceState) =>
  state.issues.map((issue) => ({
    ...issue,
    labelIds: state.memberships
      .filter((row) => row.issueId === issue.issueId && row.attached)
      .map((row) => row.labelId)
      .sort(),
    projectName: state.projects.find((row) => row.projectId === issue.projectId)?.name ?? null,
    assigneeName: state.users.find((row) => row.userId === issue.assigneeId)?.name ?? null,
  }));
const counts = (state: WorkspaceState) =>
  state.labels.map((label) => ({
    workspaceId: state.workspaceId,
    labelId: label.labelId,
    name: label.name,
    count: state.memberships.filter(
      (row) =>
        row.labelId === label.labelId &&
        row.attached &&
        state.issues.some((issue) => issue.issueId === row.issueId),
    ).length,
  }));
const at = "2026-09-23T10:00:00Z";
const common = { workspaceId: "live", occurredAt: at };
const user = { workspaceId: "live", userId: "ada", name: "Ada", updatedAt: at };
const project = { workspaceId: "live", projectId: "p1", key: "p1", name: "Project", updatedAt: at };
const label = { workspaceId: "live", labelId: "bug", name: "Bug", color: "#123456", updatedAt: at };

test("indexed touched-key output matches full scans byte for byte across catalog and membership changes", async () => {
  const runtime = ManagedRuntime.make(applicationLayer(":memory:"));
  try {
    await runtime.runPromise(
      Effect.gen(function* () {
        yield* createInputs(refs("live"));
        const phases = [
          Effect.gen(function* () {
            yield* Streams.append(labelStream("live"), [
              { type: "label", key: "bug", value: label, headers: { operation: "upsert" } },
            ]);
            yield* Streams.append(labelEvents.ref({ workspaceId: "live" }), [
              {
                ...common,
                type: "LabelAttached",
                eventId: "l1",
                issueId: "i1",
                labelId: "bug",
                membershipId: "i1.bug",
                sequence: 1,
              },
            ]);
          }),
          Streams.append(events.ref({ workspaceId: "live" }), [
            {
              ...common,
              type: "IssueCreated",
              eventId: "e1",
              issueId: "i1",
              projectId: "p1",
              title: "One",
              status: "todo",
              sequence: 1,
            },
            {
              ...common,
              type: "IssueCreated",
              eventId: "e2",
              issueId: "i2",
              projectId: "p1",
              title: "Two",
              status: "todo",
              sequence: 2,
            },
          ]),
          Effect.gen(function* () {
            yield* Streams.append(projectStream("live"), [
              { type: "project", key: "p1", value: project, headers: { operation: "upsert" } },
            ]);
            yield* Streams.append(userStream("live"), [
              { type: "user", key: "ada", value: user, headers: { operation: "upsert" } },
            ]);
            yield* Streams.append(events.ref({ workspaceId: "live" }), [
              {
                ...common,
                type: "IssueAssigned",
                eventId: "e3",
                issueId: "i2",
                assigneeId: "ada",
                status: "todo",
                sequence: 3,
              },
            ]);
          }),
          Effect.gen(function* () {
            yield* Streams.append(userStream("live"), [
              {
                type: "user",
                key: "ada",
                value: { ...user, name: "Augusta" },
                headers: { operation: "upsert" },
              },
            ]);
            yield* Streams.append(projectStream("live"), [
              {
                type: "project",
                key: "p1",
                value: { ...project, name: "Renamed" },
                headers: { operation: "upsert" },
              },
            ]);
            yield* Streams.append(labelEvents.ref({ workspaceId: "live" }), [
              {
                ...common,
                type: "LabelDetached",
                eventId: "l2",
                issueId: "i1",
                labelId: "bug",
                membershipId: "i1.bug",
                sequence: 2,
              },
              {
                ...common,
                type: "LabelAttached",
                eventId: "stale",
                issueId: "i1",
                labelId: "bug",
                membershipId: "i1.bug",
                sequence: 1,
              },
            ]);
          }),
          Streams.append(labelStream("live"), [
            { type: "label", key: "bug", headers: { operation: "delete" } },
            {
              type: "label",
              key: "bug",
              value: { ...label, name: "Readded" },
              headers: { operation: "upsert" },
            },
          ]),
          Effect.gen(function* () {
            yield* Streams.append(labelStream("live"), [
              { type: "label", key: "bug", headers: { operation: "delete" } },
            ]);
            yield* Streams.append(projectStream("live"), [
              { type: "project", key: "p1", headers: { operation: "delete" } },
            ]);
            yield* Streams.append(userStream("live"), [
              { type: "user", key: "ada", headers: { operation: "delete" } },
            ]);
          }),
        ];
        const boardRef = tracker.outputs.board.ref({ workspaceId: "live" });
        const countsRef = tracker.outputs.labelCounts.ref({ workspaceId: "live" });
        const expectedBoard: Array<string | Uint8Array> = [];
        const expectedCounts: Array<string | Uint8Array> = [];
        let previous = initialWorkspace("live");
        for (const phase of phases) {
          yield* phase;
          yield* Projection.run(tracker.member({ workspaceId: "live" }));
          const next = yield* tracker.outputs.workspace.resolve({ workspaceId: "live" });
          const oldCards = new Map(cards(previous).map((row) => [row.issueId, row]));
          const oldCounts = new Map(counts(previous).map((row) => [row.labelId, row]));
          for (const row of cards(next)) {
            if (JSON.stringify(row) !== JSON.stringify(oldCards.get(row.issueId)))
              expectedBoard.push(
                Schema.encodeSync(boardRef.codec)({
                  type: "board",
                  key: row.issueId,
                  value: row,
                  headers: { operation: "upsert" },
                }),
              );
          }
          for (const row of counts(next)) {
            if (JSON.stringify(row) !== JSON.stringify(oldCounts.get(row.labelId)))
              expectedCounts.push(
                Schema.encodeSync(countsRef.codec)({
                  type: "labelCounts",
                  key: row.labelId,
                  value: row,
                  headers: { operation: "upsert" },
                }),
              );
          }
          for (const row of previous.labels) {
            if (!next.labels.some((label) => label.labelId === row.labelId))
              expectedCounts.push(
                Schema.encodeSync(countsRef.codec)({
                  type: "labelCounts",
                  key: row.labelId,
                  headers: { operation: "delete" },
                }),
              );
          }
          // Before the first issue, board is correctly absent; label counts already exist.
          if (expectedBoard.length > 0) {
            const actual = yield* Streams.read(boardRef).pipe(Streams.items, Stream.runCollect);
            expect(actual.map((item) => Schema.encodeSync(boardRef.codec)(item))).toEqual(
              expectedBoard,
            );
          }
          const actual = yield* Streams.read(countsRef).pipe(Streams.items, Stream.runCollect);
          expect(actual.map((item) => Schema.encodeSync(countsRef.codec)(item))).toEqual(
            expectedCounts,
          );
          previous = next;
        }
      }),
    );
  } finally {
    await runtime.dispose();
  }
});
