import { it as baseIt } from "@streamsy/effect-test";
import { BunCrypto } from "@effect/platform-bun";
import { MemoryStorage, Streamsy, ZERO_OFFSET, StorageFault } from "@streamsy/core";
import { expect } from "bun:test";
import { Context, Deferred, Effect, Layer, Ref, Result, Stream } from "effect";
import { IssueTracker, IssueTrackerLive, SeedLive } from "../../server/issue-tracker.ts";
import {
  createIssue,
  issueUpsert,
  projectUpsert,
  updateIssue,
  type Stamp,
} from "../../server/state.ts";
import { workspaceEvents, Workspaces, type WorkspaceChange } from "../../server/streams.ts";
import type { Issue, Project } from "../../shared/state-schema.ts";

const workspaceId = "concurrent-test";

const createdAt = "2026-09-20T00:00:00.000Z";

const stamp = (id: string): Stamp => ({
  id,
  now: createdAt,
  txid: "00000000-0000-4000-8000-000000000001",
});

const project: Project = {
  id: "proj_test",
  name: "Concurrent project",
  description: "",
  createdAt,
};

const makeIssue = (id: string, status: Issue["status"] = "open"): Issue => ({
  id,
  projectId: project.id,
  title: id,
  status,
  createdAt,
  updatedAt: createdAt,
});

/** Append one change with no offset check, as a rival writer would. */
const appendUnchecked = (change: WorkspaceChange) =>
  Effect.flatMap(Workspaces, (workspaces) =>
    workspaces.append({ workspaceId }, workspaceEvents(workspaceId, ZERO_OFFSET, [change])),
  );

/** A memory store holding one workspace with one project. */
const seeded = Effect.gen(function* () {
  yield* (yield* Workspaces).create({ workspaceId });
  yield* appendUnchecked(projectUpsert(project, stamp(project.id)));
});

const issueKeys = Effect.gen(function* () {
  const batches = yield* (yield* Workspaces).read({ workspaceId }).pipe(Stream.runCollect);

  return batches
    .flatMap((batch) => batch.items)
    .filter((event) => event.type === "issue")
    .map((event) => event.key)
    .toSorted();
});

const runWithStore = <A, E>(effect: Effect.Effect<A, E, Workspaces>) =>
  effect.pipe(withLayer(MemoryStorage.layer(Streamsy.group(Workspaces))));

/**
 * A tracker whose first two decisions wait until both have folded the same
 * head, so exactly one of the two conditional appends must lose.
 */
const racingTracker = Effect.gen(function* () {
  const reader = yield* Workspaces;
  const firstReads = yield* Ref.make(0);
  const bothRead = yield* Deferred.make<void>();

  const barrierReader = Workspaces.of({
    ...reader,
    decide: (params, spec) =>
      reader.decide(params, {
        ...spec,
        decide: (state, at) =>
          Effect.gen(function* () {
            const readNumber = yield* Ref.updateAndGet(firstReads, (count) => count + 1);

            if (readNumber <= 2) {
              if (readNumber === 2) yield* Deferred.succeed(bothRead, undefined);
              yield* Deferred.await(bothRead);
            }

            return yield* spec.decide(state, at);
          }),
      }),
  });

  return yield* IssueTracker.pipe(
    withLayer(IssueTrackerLive.pipe(Layer.provide(BunCrypto.layer))),
    Effect.provideService(Workspaces, barrierReader),
  );
});

baseIt.live("two concurrent writers land without a lost update", () =>
  Effect.gen(function* () {
    let attempts = 0;

    const { keys, written } = yield* runWithStore(
      Effect.gen(function* () {
        yield* seeded;
        const tracker = yield* racingTracker;

        const write = (issue: Issue) =>
          tracker.mutate(workspaceId, (state) => {
            attempts += 1;
            expect(state.getProject(project.id)).toBeDefined();

            // The third attempt is the loser's retry: it must see the winner's issue.
            if (attempts === 3) {
              const otherId = issue.id === "issue_a" ? "issue_b" : "issue_a";
              expect(state.getIssue(otherId)).toBeDefined();
            }

            return Effect.succeed({ change: issueUpsert(issue, stamp(issue.id)), value: issue });
          });

        const results = yield* Effect.all(
          [write(makeIssue("issue_a")), write(makeIssue("issue_b"))],
          { concurrency: "unbounded" },
        );

        return { keys: yield* issueKeys, written: results };
      }),
    );

    expect(attempts).toBe(3);
    expect(written.map(({ value }) => value.id)).toEqual(["issue_a", "issue_b"]);
    expect(keys).toEqual(["issue_a", "issue_b"]);
  }),
);

baseIt.live(
  "two writers race for the last in-progress place: the loser's retry breaks the rule",
  () =>
    Effect.gen(function* () {
      const { results, inProgress } = yield* runWithStore(
        Effect.gen(function* () {
          yield* seeded;

          for (const issue of [
            makeIssue("issue_busy_1", "in_progress"),
            makeIssue("issue_busy_2", "in_progress"),
            makeIssue("issue_a"),
            makeIssue("issue_b"),
          ]) {
            yield* appendUnchecked(issueUpsert(issue, stamp(issue.id)));
          }

          const tracker = yield* racingTracker;

          // Both writers fold a head with two issues in progress, so both pass the
          // rule. One append lands. The other retries on the new head, where the
          // project is at the limit.
          const start = (issueId: string) =>
            tracker
              .mutate(workspaceId, updateIssue(issueId, { status: "in_progress" }, stamp(issueId)))
              .pipe(Effect.result);

          const answers = yield* Effect.all([start("issue_a"), start("issue_b")], {
            concurrency: "unbounded",
          });

          const { state } = yield* tracker.materialize(workspaceId);

          return { results: answers, inProgress: state.countInProgress(project.id) };
        }),
      );

      const failures = results.filter(Result.isFailure).map((result) => result.failure);
      expect(results.filter(Result.isSuccess)).toHaveLength(1);
      expect(failures).toEqual([
        expect.objectContaining({ _tag: "TooManyInProgress", projectId: project.id, limit: 3 }),
      ]);
      expect(inProgress).toBe(3);
    }),
);

baseIt.live("a writer that loses every race answers WriteConflict after three retries", () =>
  Effect.gen(function* () {
    let attempts = 0;

    const { failure, keys } = yield* runWithStore(
      Effect.gen(function* () {
        yield* seeded;
        const workspaces = yield* Workspaces;

        const tracker = yield* IssueTracker.pipe(
          withLayer(IssueTrackerLive.pipe(Layer.provide(BunCrypto.layer))),
        );

        // Each attempt lets a rival writer append first, so the head the
        // attempt folded is never the tail when its own append arrives.
        const conflict = yield* tracker
          .mutate(workspaceId, () =>
            Effect.gen(function* () {
              attempts += 1;
              const rival = makeIssue(`issue_rival_${attempts}`);
              yield* appendUnchecked(issueUpsert(rival, stamp(rival.id)));
              const loser = makeIssue("issue_loser");

              return { change: issueUpsert(loser, stamp(loser.id)), value: loser };
            }).pipe(Effect.provideService(Workspaces, workspaces), Effect.orDie),
          )
          .pipe(Effect.flip);

        return { failure: conflict, keys: yield* issueKeys };
      }),
    );

    expect(failure).toMatchObject({ _tag: "WriteConflict", workspaceId });
    expect(attempts).toBe(4);
    expect(keys).not.toContain("issue_loser");
    expect(keys).toHaveLength(4);
  }),
);

baseIt.live("mutating an unknown workspace fails with WorkspaceNotFound", () =>
  Effect.gen(function* () {
    const failure = yield* runWithStore(
      Effect.gen(function* () {
        const tracker = yield* IssueTracker.pipe(
          withLayer(IssueTrackerLive.pipe(Layer.provide(BunCrypto.layer))),
        );

        return yield* Effect.flip(
          tracker.mutate("missing", () => Effect.die("the attempt must not run")),
        );
      }),
    );

    expect(failure).toMatchObject({ _tag: "WorkspaceNotFound", workspaceId: "missing" });
  }),
);

baseIt.live("a domain failure is answered at once, not retried", () =>
  Effect.gen(function* () {
    let attempts = 0;

    const failure = yield* runWithStore(
      Effect.gen(function* () {
        yield* seeded;

        const tracker = yield* IssueTracker.pipe(
          withLayer(IssueTrackerLive.pipe(Layer.provide(BunCrypto.layer))),
        );

        return yield* Effect.flip(
          tracker.mutate(workspaceId, (state) => {
            attempts += 1;

            return createIssue({ projectId: "proj_missing" }, stamp("issue_x"))(state);
          }),
        );
      }),
    );

    expect(failure).toMatchObject({ _tag: "UnknownReference", field: "projectId" });
    expect(attempts).toBe(1);
  }),
);

baseIt.live("materialize and mutate preserve operational failures in the error channel", () =>
  runWithStore(
    Effect.scoped(
      Effect.gen(function* () {
        const workspaces = yield* Workspaces;

        const fault = new StorageFault({
          operation: "read",
          message: "store busy",
          retryable: true,
        });

        const failing = Layer.succeed(Workspaces, {
          ...workspaces,
          fold: () => Effect.fail(fault),
          decide: () => Effect.fail(fault),
        });

        const trackerLayer = IssueTrackerLive.pipe(Layer.provide([failing, BunCrypto.layer]));
        const tracker = Context.get(yield* Layer.build(trackerLayer), IssueTracker);
        expect(yield* Effect.flip(tracker.materialize(workspaceId))).toBe(fault);
        expect(
          yield* Effect.flip(tracker.mutate(workspaceId, () => Effect.die("must not decide"))),
        ).toBe(fault);
      }),
    ),
  ),
);

baseIt.live("create and the seed layer expose typed storage failures", () =>
  runWithStore(
    Effect.scoped(
      Effect.gen(function* () {
        const workspaces = yield* Workspaces;

        const fault = new StorageFault({
          operation: "create",
          message: "store busy",
          retryable: true,
        });

        const failing = Layer.succeed(Workspaces, {
          ...workspaces,
          create: () => Effect.fail(fault),
        });

        const trackerLayer = IssueTrackerLive.pipe(Layer.provide([failing, BunCrypto.layer]));
        const tracker = Context.get(yield* Layer.build(trackerLayer), IssueTracker);
        expect(yield* Effect.flip(tracker.create)).toBe(fault);
        const seed = SeedLive.pipe(Layer.provide(trackerLayer));
        expect(yield* Layer.build(seed).pipe(Effect.flip)).toBe(fault);
      }),
    ),
  ),
);

/** Build local fixtures in a scope and supply their Context to the program. */
const withLayer =
  <ROut, LE, RIn>(fixture: Layer.Layer<ROut, LE, RIn>) =>
  <A, E, R>(program: Effect.Effect<A, E, R>) =>
    Effect.scoped(
      Effect.gen(function* () {
        const services = yield* Layer.build(fixture);

        return yield* program.pipe(Effect.provideContext(services));
      }),
    );
