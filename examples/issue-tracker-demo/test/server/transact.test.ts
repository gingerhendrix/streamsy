import { expect, test } from "bun:test";
import { Streams } from "@streamsy/core";
import { Effect, Stream } from "effect";
import type { Issue } from "../../shared/state-schema.ts";
import { issueUpsert, mutateWorkspace, projectUpsert } from "../../server/state.ts";
import { appendWorkspaceEvent, workspaceEvents } from "../../server/streams.ts";

test("two concurrent Transact writers land without a lost update", async () => {
  const workspaceId = "concurrent-test";
  const project = {
    id: "proj_test",
    name: "Concurrent project",
    description: "",
    createdAt: "2026-09-20T00:00:00.000Z",
  };
  const makeIssue = (id: string): Issue => ({
    id,
    projectId: project.id,
    title: id,
    status: "open",
    createdAt: project.createdAt,
    updatedAt: project.createdAt,
  });
  const transact = (issue: Issue) =>
    mutateWorkspace(workspaceId, (state) => {
      expect(state.getProject(project.id)).toBeDefined();
      return {
        event: issueUpsert(issue),
        respond: ({ offset }) => Response.json({ offset }),
      };
    });

  const program = Effect.gen(function* () {
    yield* Streams.create(workspaceEvents(workspaceId));
    yield* appendWorkspaceEvent(workspaceId, projectUpsert(project));
    yield* Effect.all([transact(makeIssue("issue_a")), transact(makeIssue("issue_b"))], {
      concurrency: "unbounded",
    });
    const batches = yield* Streams.read(workspaceEvents(workspaceId)).pipe(Stream.runCollect);
    return batches.flatMap((batch) => batch.items);
  });

  const events = await Effect.runPromise(program.pipe(Effect.provide(Streams.layerMemory())));
  expect(events.filter((event) => event.type === "issue").map((event) => event.key)).toEqual([
    "issue_a",
    "issue_b",
  ]);
});
