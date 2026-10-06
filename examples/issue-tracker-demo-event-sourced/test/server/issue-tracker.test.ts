import { BunServices } from "@effect/platform-bun";
import { it } from "@streamsy/effect-test";
import { expect } from "bun:test";
import { Deferred, Effect, Exit, FileSystem, Path, Ref, Result } from "effect";
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
  withLayer,
  type TrackerServices,
} from "../support.ts";

/** Run after boot, as the server does: the registry and `main` exist. */
const withTracker = <A, E>(effect: Effect.Effect<A, E, TrackerServices>) =>
  Effect.flatMap(IssueTracker, (tracker) => tracker.boot).pipe(
    Effect.andThen(effect),
    withLayer(trackerOn(MemoryHost)),
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

it.live("two writers race for the last in-progress place: the loser's retry breaks the rule", () =>
  Effect.gen(function* () {
    const outcome = yield* withTracker(
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
  }),
);

it.live("a writer that loses every race answers WriteConflict after three retries", () =>
  Effect.gen(function* () {
    const attempts = yield* Ref.make(0);

    const exit = yield* withTracker(
      Effect.gen(function* () {
        const tracker = yield* IssueTracker;
        const workspaceId = yield* raceWorkspace;
        const events = yield* WorkspaceEvents;

        const rival = (view: CommandView) =>
          Effect.gen(function* () {
            const attempt = yield* Ref.updateAndGet(attempts, (n) => n + 1);
            // A rival append moves the tail after every read.
            const rivalStamp = stamp(`rival_${attempt}`);

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

    expect(yield* Ref.get(attempts)).toBe(4);
    expect(Exit.isFailure(exit) && String(exit.cause)).toContain("WriteConflict");
  }),
);

it.live("a domain failure is answered at once, not retried", () =>
  Effect.gen(function* () {
    const attempts = yield* Ref.make(0);

    const exit = yield* withTracker(
      Effect.gen(function* () {
        const tracker = yield* IssueTracker;

        return yield* Effect.exit(
          tracker.mutate("main", (view) =>
            Ref.update(attempts, (n) => n + 1).pipe(
              Effect.andThen(updateIssue("nope", { status: "done" }, stamp("x"))(view)),
            ),
          ),
        );
      }),
    );

    expect(yield* Ref.get(attempts)).toBe(1);
    expect(Exit.isFailure(exit) && String(exit.cause)).toContain("IssueNotFound");
  }),
);

it.live("mutating an unknown workspace fails with WorkspaceNotFound", () =>
  Effect.gen(function* () {
    const exit = yield* withTracker(
      Effect.flatMap(IssueTracker, (tracker) =>
        Effect.exit(tracker.mutate("nope12345", createIssue({}, stamp("x")))),
      ),
    );

    expect(Exit.isFailure(exit) && String(exit.cause)).toContain("WorkspaceNotFound");
  }),
);

it.live("boot seeds main once and the watcher writes its rows", () =>
  Effect.gen(function* () {
    const { events, rows } = yield* withTracker(
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
  }),
);

it.live("a new workspace is registered, starts with its project and gets a watcher", () =>
  Effect.gen(function* () {
    const created = yield* withTracker(
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
  }),
);

it.live("with a database file, a restart resumes the read model of a shared workspace (R11)", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "issue-tracker-event-sourced-" });
    const filename = (yield* Path.Path).join(directory, "restart.sqlite");

    // Each run builds the tracker on the file and closes it after, like a server restart.
    const run = <A, E>(effect: Effect.Effect<A, E, TrackerServices>) =>
      Effect.gen(function* () {
        yield* (yield* IssueTracker).boot;

        return yield* effect;
      }).pipe(withLayer(trackerOn(fileHost(filename))));

    const before = yield* run(
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

    const after = yield* run(
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
  }).pipe(withLayer(BunServices.layer)),
);
