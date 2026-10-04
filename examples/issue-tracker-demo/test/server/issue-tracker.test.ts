import { BunCrypto } from "@effect/platform-bun";
import { MemoryStorage, Streamsy, ZERO_OFFSET, StorageFault } from "@streamsy/core";
import { expect, test } from "bun:test";
import { Context, Deferred, Effect, Layer, Ref, Stream } from "effect";
import { IssueTracker, IssueTrackerLive, SeedLive } from "../../server/issue-tracker.ts";
import { createIssue, issueUpsert, projectUpsert, type Stamp } from "../../server/state.ts";
import { workspaceEvents, Workspaces, type WorkspaceChange } from "../../server/streams.ts";
import type { Issue, Project } from "../../shared/state-schema.ts";

const workspaceId = "concurrent-test";
const createdAt = "2026-09-20T00:00:00.000Z";
const stamp = (id: string): Stamp => ({ id, now: createdAt, txid: crypto.randomUUID() });
const project: Project = {
  id: "proj_test",
  name: "Concurrent project",
  description: "",
  createdAt,
};
const makeIssue = (id: string): Issue => ({
  id,
  projectId: project.id,
  title: id,
  status: "open",
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
  Effect.runPromise(effect.pipe(Effect.provide(MemoryStorage.layer(Streamsy.group(Workspaces)))));

test("two concurrent writers land without a lost update", async () => {
  let attempts = 0;
  const { keys, written } = await runWithStore(
    Effect.gen(function* () {
      yield* seeded;
      const reader = yield* Workspaces;
      const firstReads = yield* Ref.make(0);
      const bothRead = yield* Deferred.make<void>();
      // Hold both writers' first decisions until each has folded the same head,
      // so exactly one of the two conditional appends must lose.
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
      const tracker = yield* IssueTracker.pipe(
        Effect.provide(IssueTrackerLive.pipe(Layer.provide(BunCrypto.layer))),
        Effect.provideService(Workspaces, barrierReader),
      );
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
});

test("a writer that loses every race answers WriteConflict after three retries", async () => {
  let attempts = 0;
  const { failure, keys } = await runWithStore(
    Effect.gen(function* () {
      yield* seeded;
      const workspaces = yield* Workspaces;
      const tracker = yield* IssueTracker.pipe(
        Effect.provide(IssueTrackerLive.pipe(Layer.provide(BunCrypto.layer))),
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
});

test("mutating an unknown workspace fails with WorkspaceNotFound", async () => {
  const failure = await runWithStore(
    Effect.gen(function* () {
      const tracker = yield* IssueTracker.pipe(
        Effect.provide(IssueTrackerLive.pipe(Layer.provide(BunCrypto.layer))),
      );
      return yield* Effect.flip(
        tracker.mutate("missing", () => Effect.die("the attempt must not run")),
      );
    }),
  );
  expect(failure).toMatchObject({ _tag: "WorkspaceNotFound", workspaceId: "missing" });
});

test("a domain failure is answered at once, not retried", async () => {
  let attempts = 0;
  const failure = await runWithStore(
    Effect.gen(function* () {
      yield* seeded;
      const tracker = yield* IssueTracker.pipe(
        Effect.provide(IssueTrackerLive.pipe(Layer.provide(BunCrypto.layer))),
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
});

test("materialize and mutate preserve operational failures in the error channel", () =>
  runWithStore(
    Effect.gen(function* () {
      const workspaces = yield* Workspaces;
      const fault = new StorageFault({ operation: "read", message: "store busy", retryable: true });
      const tracker = yield* IssueTracker.pipe(
        Effect.provide(IssueTrackerLive.pipe(Layer.provide(BunCrypto.layer))),
        Effect.provideService(Workspaces, {
          ...workspaces,
          fold: () => Effect.fail(fault),
          decide: () => Effect.fail(fault),
        }),
      );
      expect(yield* Effect.flip(tracker.materialize(workspaceId))).toBe(fault);
      expect(
        yield* Effect.flip(tracker.mutate(workspaceId, () => Effect.die("must not decide"))),
      ).toBe(fault);
    }),
  ));

test("create and the seed layer expose typed storage failures", () =>
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
  ));
