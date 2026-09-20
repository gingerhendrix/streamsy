import { expect, test } from "bun:test";
import { Streams, StreamsReader, ZERO_OFFSET } from "@streamsy/core";
import { Deferred, Effect, Ref, Stream } from "effect";
import type { Issue } from "../../shared/state-schema.ts";
import { issueUpsert, mutateWorkspace, projectUpsert } from "../../server/state.ts";
import { appendWorkspaceEvent, workspaceEvents } from "../../server/streams.ts";

test("two concurrent Transact writers land without a lost update", async () => {
  let attempts = 0;
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
      attempts += 1;
      expect(state.getProject(project.id)).toBeDefined();
      if (attempts === 3) {
        const otherId = issue.id === "issue_a" ? "issue_b" : "issue_a";
        expect(state.getIssue(otherId)).toBeDefined();
      }
      return {
        event: issueUpsert(issue),
        respond: ({ offset }) => Response.json({ offset }),
      };
    });

  const program = Effect.gen(function* () {
    yield* Streams.create(workspaceEvents(workspaceId));
    yield* appendWorkspaceEvent(workspaceId, projectUpsert(project), ZERO_OFFSET);
    const reader = yield* StreamsReader;
    const firstReads = yield* Ref.make(0);
    const bothRead = yield* Deferred.make<void>();
    const barrierReader = StreamsReader.of({
      ...reader,
      read: (id, options) =>
        Effect.gen(function* () {
          const result = yield* reader.read(id, options);
          const readNumber = yield* Ref.updateAndGet(firstReads, (count) => count + 1);
          // The memory store materializes in one read; these are the two writers' first reads.
          if (readNumber <= 2) {
            if (readNumber === 2) yield* Deferred.succeed(bothRead, undefined);
            yield* Deferred.await(bothRead);
          }
          return result;
        }),
    });
    const responses = yield* Effect.all(
      [transact(makeIssue("issue_a")), transact(makeIssue("issue_b"))],
      { concurrency: "unbounded" },
    ).pipe(Effect.provideService(StreamsReader, barrierReader));
    const batches = yield* Streams.read(workspaceEvents(workspaceId)).pipe(Stream.runCollect);
    return { events: batches.flatMap((batch) => batch.items), responses };
  });

  const { events, responses } = await Effect.runPromise(
    program.pipe(Effect.provide(Streams.layerMemory())),
  );
  expect(attempts).toBe(3);
  expect(responses.map((response) => response.status)).toEqual([200, 200]);
  expect(
    events
      .filter((event) => event.type === "issue")
      .map((event) => event.key)
      .toSorted(),
  ).toEqual(["issue_a", "issue_b"]);
});
