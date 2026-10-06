import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, expect, test } from "bun:test";
import { Deferred, Effect, Exit, Ref, Result } from "effect";
import type { CommandView } from "../../server/command-view.ts";
import { createIssue, mainWorkspaceSeed, updateIssue, type Decision } from "../../server/decide.ts";
import { WorkspaceEvent } from "../../server/events.ts";
import { IssueTracker } from "../../server/issue-tracker.ts";
import { WorkspaceEvents, WorkspaceRegistry } from "../../server/streams.ts";
import {
  awaitTxids,
  fileHost,
  MemoryHost,
  stamp,
  stateChanges,
  trackerOn,
  type TrackerServices,
} from "../support.ts";

const directory = mkdtempSync(join(tmpdir(), "issue-tracker-event-sourced-"));
afterAll(() => rmSync(directory, { recursive: true, force: true }));

/** Run after boot, as the server does: the registry and `main` exist. */
const withTracker = <A, E>(effect: Effect.Effect<A, E, TrackerServices>) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.flatMap(IssueTracker, (tracker) => tracker.boot).pipe(
        Effect.andThen(effect),
        Effect.provide(trackerOn(MemoryHost)),
      ),
    ),
  );

/** A workspace with one project and two issues in progress: one place is left. */
const raceWorkspace = Effect.gen(function* () {
  const tracker = yield* IssueTracker;
  const workspaceId = yield* tracker.create;
  const events = yield* WorkspaceEvents;
  const { items } = yield* events.collect({ workspaceId });
  const starter = items[0];
  if (starter === undefined || !WorkspaceEvent.guards.ProjectCreated(starter)) {
    return yield* Effect.die(new Error("a new workspace starts with its project"));
  }
  const issue = (id: string, status: "open" | "in_progress") =>
    tracker.mutate(
      workspaceId,
      createIssue({ id, projectId: starter.projectId, status }, stamp(id)),
    );
  yield* issue("busy_1", "in_progress");
  yield* issue("busy_2", "in_progress");
  yield* issue("contender_a", "open");
  yield* issue("contender_b", "open");
  return workspaceId;
});

/** Hold the first two decisions until both have read the same head. */
const barrier = Effect.gen(function* () {
  const decisions = yield* Ref.make(0);
  const bothRead = yield* Deferred.make<void>();
  return <A, E>(decision: (view: CommandView) => Effect.Effect<Decision<A>, E>) =>
    (view: CommandView) =>
      Effect.gen(function* () {
        const count = yield* Ref.updateAndGet(decisions, (n) => n + 1);
        if (count <= 2) {
          if (count === 2) yield* Deferred.succeed(bothRead, undefined);
          yield* Deferred.await(bothRead);
        }
        return yield* decision(view);
      });
});

test("two writers race for the last in-progress place: the loser's retry breaks the rule", async () => {
  const outcome = await withTracker(
    Effect.gen(function* () {
      const tracker = yield* IssueTracker;
      const workspaceId = yield* raceWorkspace;
      const hold = yield* barrier;
      const start = (issueId: string) =>
        tracker
          .mutate(
            workspaceId,
            hold(updateIssue(issueId, { status: "in_progress" }, stamp(issueId))),
          )
          .pipe(Effect.result);
      const results = yield* Effect.all([start("contender_a"), start("contender_b")], {
        concurrency: "unbounded",
      });
      const { items } = yield* (yield* WorkspaceEvents).collect({ workspaceId });
      const inProgress = items.filter(
        (event) =>
          WorkspaceEvent.guards.IssueStatusChanged(event) && event.status === "in_progress",
      );
      return { results, inProgress };
    }),
  );
  expect(outcome.results.filter(Result.isSuccess)).toHaveLength(1);
  const refused = outcome.results.filter(Result.isFailure).map((result) => result.failure);
  expect(refused).toEqual([expect.objectContaining({ _tag: "TooManyInProgress", limit: 3 })]);
  // Only the winner's event is in the log.
  expect(outcome.inProgress).toHaveLength(1);
});

test("a writer that loses every race answers WriteConflict after three retries", async () => {
  let attempts = 0;
  const exit = await withTracker(
    Effect.gen(function* () {
      const tracker = yield* IssueTracker;
      const workspaceId = yield* raceWorkspace;
      const events = yield* WorkspaceEvents;
      const rival = (view: CommandView) =>
        Effect.gen(function* () {
          attempts += 1;
          // A rival append moves the tail after every read.
          const rivalStamp = stamp(`rival_${attempts}`);
          const rivalEvent = WorkspaceEvent.cases.ProjectCreated.make({
            projectId: rivalStamp.id,
            name: "Rival",
            description: "",
            at: rivalStamp.now,
            txid: rivalStamp.txid,
          });
          yield* events.append({ workspaceId }, [rivalEvent]).pipe(Effect.orDie);
          return yield* createIssue({ projectId: view.projectIds[0] }, stamp("loser"))(view);
        });
      return yield* Effect.exit(tracker.mutate(workspaceId, rival));
    }),
  );
  expect(attempts).toBe(4);
  expect(Exit.isFailure(exit) && String(exit.cause)).toContain("WriteConflict");
});

test("a domain failure is answered at once, not retried", async () => {
  let attempts = 0;
  const exit = await withTracker(
    Effect.gen(function* () {
      const tracker = yield* IssueTracker;
      return yield* Effect.exit(
        tracker.mutate("main", (view) => {
          attempts += 1;
          return updateIssue("nope", { status: "done" }, stamp("x"))(view);
        }),
      );
    }),
  );
  expect(attempts).toBe(1);
  expect(Exit.isFailure(exit) && String(exit.cause)).toContain("IssueNotFound");
});

test("mutating an unknown workspace fails with WorkspaceNotFound", async () => {
  const exit = await withTracker(
    Effect.flatMap(IssueTracker, (tracker) =>
      Effect.exit(tracker.mutate("nope12345", createIssue({}, stamp("x")))),
    ),
  );
  expect(Exit.isFailure(exit) && String(exit.cause)).toContain("WorkspaceNotFound");
});

test("boot seeds main once and the watcher writes its rows", async () => {
  const { events, rows } = await withTracker(
    Effect.gen(function* () {
      const tracker = yield* IssueTracker;
      // `withTracker` booted once. A second boot must not seed again.
      yield* tracker.boot;
      const { items } = yield* (yield* WorkspaceEvents).collect({ workspaceId: "main" });
      const last = items.at(-1);
      return { events: items, rows: yield* awaitTxids("main", last?.txid ?? "") };
    }),
  );
  expect(events).toHaveLength(mainWorkspaceSeed(stamp("seed")).length);
  expect(rows.map((row) => row.key)).toContain("comment_welcome");
});

test("a new workspace is registered, starts with its project and gets a watcher", async () => {
  const created = await withTracker(
    Effect.gen(function* () {
      const tracker = yield* IssueTracker;
      const workspaceId = yield* tracker.create;
      const { items } = yield* (yield* WorkspaceEvents).collect({ workspaceId });
      const registered = yield* Effect.flatMap(WorkspaceRegistry, (registry) =>
        registry.collect({}),
      );
      return {
        workspaceId,
        registered: registered.items,
        rows: yield* awaitTxids(workspaceId, items[0]?.txid ?? ""),
      };
    }),
  );
  expect(created.registered.map((record) => record.workspaceId)).toEqual([created.workspaceId]);
  expect(created.rows).toEqual([
    expect.objectContaining({
      type: "project",
      value: expect.objectContaining({ name: "Getting started" }),
    }),
  ]);
});

test("with a database file, a restart resumes the read model of a shared workspace (R11)", async () => {
  const filename = join(directory, "restart.sqlite");
  const run = <A, E>(effect: Effect.Effect<A, E, TrackerServices>) =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          yield* (yield* IssueTracker).boot;
          return yield* effect;
        }).pipe(Effect.provide(trackerOn(fileHost(filename)))),
      ),
    );
  const before = await run(
    Effect.gen(function* () {
      const tracker = yield* IssueTracker;
      const workspaceId = yield* tracker.create;
      const { items } = yield* (yield* WorkspaceEvents).collect({ workspaceId });
      const projectId =
        items[0] !== undefined && WorkspaceEvent.guards.ProjectCreated(items[0])
          ? items[0].projectId
          : "";
      const first = stamp("issue_before");
      yield* tracker.mutate(workspaceId, createIssue({ projectId }, first));
      yield* awaitTxids(workspaceId, first.txid);
      return { workspaceId, projectId };
    }),
  );
  const after = await run(
    Effect.gen(function* () {
      const tracker = yield* IssueTracker;
      const second = stamp("issue_after");
      yield* tracker.mutate(
        before.workspaceId,
        createIssue({ projectId: before.projectId }, second),
      );
      yield* awaitTxids(before.workspaceId, second.txid);
      return yield* stateChanges(before.workspaceId);
    }),
  );
  // The starter project and both issues, each once: the runner resumed from its checkpoint.
  expect(after.map((change) => change.key)).toEqual([
    before.projectId,
    "issue_before",
    "issue_after",
  ]);
});
