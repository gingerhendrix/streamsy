import { MaterializedState } from "@durable-streams/state";
import {
  ZERO_OFFSET,
  type ReadFailure,
  type AppendFailure,
  type CreateFailure,
  type StreamNotFound,
  type StreamGone,
  type OffsetMismatch,
} from "@streamsy/core";
import {
  Context,
  Crypto,
  DateTime,
  Effect,
  Layer,
  Result,
  Schema,
  type PlatformError,
} from "effect";
import type { Issue, Project } from "../shared/state-schema.ts";
import { mainWorkspaceId } from "./config.ts";
import {
  mainWorkspaceSeed,
  projectUpsert,
  starterProject,
  type Mutation,
  type Stamp,
  type WorkspaceState,
} from "./state.ts";
import {
  workspaceEvents,
  Workspaces,
  type WorkspaceChange,
  type WorkspaceEvent,
} from "./streams.ts";

export class WorkspaceNotFound extends Schema.TaggedError<WorkspaceNotFound>()(
  "WorkspaceNotFound",
  { workspaceId: Schema.String },
) {}

/** The CAS append still lost after three retries. */
export class WriteConflict extends Schema.TaggedError<WriteConflict>()("WriteConflict", {
  workspaceId: Schema.String,
}) {}

/** A folded workspace and the head offset that is its CAS token. */
export interface Materialized {
  readonly state: WorkspaceState;
  readonly headOffset: string;
}

/** A landed write: the value to answer with and the offset the browser waits for. */
export interface Appended<A> {
  readonly value: A;
  readonly awaitOffset: string;
}

/** The issue tracker's domain: every read and write of a workspace stream. */
export class IssueTracker extends Context.Service<
  IssueTracker,
  {
    /** Fold one workspace from its durable stream. */
    readonly materialize: (
      workspaceId: string,
    ) => Effect.Effect<
      Materialized,
      WorkspaceNotFound | Exclude<ReadFailure, StreamNotFound | StreamGone>
    >;
    /** Read, decide, append with `Workspaces.decide`; a lost race is retried three times. */
    readonly mutate: <A, E>(
      workspaceId: string,
      attempt: (state: WorkspaceState) => Effect.Effect<Mutation<A>, E>,
    ) => Effect.Effect<
      Appended<A>,
      | E
      | WorkspaceNotFound
      | WriteConflict
      | Exclude<
          ReadFailure | AppendFailure<{ expectedOffset: string }>,
          StreamNotFound | StreamGone | OffsetMismatch
        >
    >;
    /** Create a shared workspace with a starter project and return its id. */
    readonly create: Effect.Effect<
      string,
      CreateFailure | AppendFailure<{ expectedOffset: string }> | PlatformError.PlatformError
    >;
    /** Ensure and seed the known demo workspace. */
    readonly seed: Effect.Effect<
      void,
      | CreateFailure
      | AppendFailure<{ expectedOffset: string }>
      | Exclude<ReadFailure, StreamNotFound | StreamGone>
      | WorkspaceNotFound
    >;
    /** Choose the id, time and txid for one write. */
    readonly stamp: (prefix: string, txid?: string) => Effect.Effect<Stamp>;
  }
>()("IssueTrackerDemo/IssueTracker") {}

const fold = (materialized: MaterializedState, event: WorkspaceEvent) => {
  materialized.apply(event);

  return materialized;
};

const view = (state: MaterializedState): WorkspaceState => ({
  getProject: (projectId: string) => state.get<Project>("project", projectId),
  getIssue: (issueId: string) => state.get<Issue>("issue", issueId),
  hasProjects: () => state.getType("project").size > 0,
  countInProgress: (projectId: string) =>
    [...state.getType("issue").keys()]
      .map((issueId) => state.get<Issue>("issue", issueId))
      .filter((issue) => issue?.projectId === projectId && issue.status === "in_progress").length,
});

export const IssueTrackerLive = Layer.effect(
  IssueTracker,
  Effect.gen(function* () {
    const workspaces = yield* Workspaces;
    const crypto = yield* Crypto.Crypto;

    const stamp = Effect.fn("IssueTracker.stamp")(function* (prefix: string, txid?: string) {
      const now = DateTime.formatIso(yield* DateTime.now);
      const id = `${prefix}_${(yield* crypto.randomUUIDv4).slice(0, 8)}`;

      return { id, now, txid: txid ?? (yield* crypto.randomUUIDv4) };
    }, Effect.orDie);

    // A seed or a new workspace appends at the head it read, and only lands if
    // that head is still the tail.
    const append = (workspaceId: string, change: WorkspaceChange, head: string) =>
      workspaces.append({ workspaceId }, workspaceEvents(workspaceId, head, [change]), {
        expectedOffset: head,
      });

    const materialize = Effect.fn("IssueTracker.materialize")(function* (workspaceId: string) {
      const { state, offset } = yield* workspaces
        .fold({ workspaceId }, () => new MaterializedState(), fold)
        .pipe(
          Effect.catchTag(
            ["StreamNotFound", "StreamGone"],
            () => new WorkspaceNotFound({ workspaceId }),
          ),
        );

      return { state: view(state), headOffset: offset };
    });

    const mutate = <A, E>(
      workspaceId: string,
      attempt: (state: WorkspaceState) => Effect.Effect<Mutation<A>, E>,
    ) =>
      workspaces
        .decide(
          { workspaceId },
          {
            initial: () => new MaterializedState(),
            fold,
            // Each change records the head it was validated against as its source
            // offset; the same offset is the append's `expectedOffset`. A domain
            // failure is an answer at that head, so it appends nothing.
            decide: (state, { offset }) =>
              Effect.map(Effect.result(attempt(view(state))), (answer) => ({
                value: answer,
                items: Result.isSuccess(answer)
                  ? workspaceEvents(workspaceId, offset, [answer.success.change])
                  : [],
              })),
          },
        )
        .pipe(
          Effect.catchTag("OffsetMismatch", () => Effect.fail(new WriteConflict({ workspaceId }))),
          Effect.catchTag(
            ["StreamNotFound", "StreamGone"],
            () => new WorkspaceNotFound({ workspaceId }),
          ),
          Effect.flatMap(({ value: answer, offset }) =>
            Effect.map(Effect.fromResult(answer), ({ value }) => ({ value, awaitOffset: offset })),
          ),
          Effect.withSpan("IssueTracker.mutate"),
        );

    const create = Effect.gen(function* () {
      const bytes = yield* crypto.randomBytes(10);
      const workspaceId = Array.from(bytes, (byte) => (byte % 36).toString(36)).join("");
      yield* workspaces.create({ workspaceId });
      const starter = yield* stamp("proj");
      yield* append(workspaceId, projectUpsert(starterProject(starter), starter), ZERO_OFFSET);

      return workspaceId;
    }).pipe(Effect.withSpan("IssueTracker.create"));

    const seed = Effect.gen(function* () {
      yield* workspaces.create({ workspaceId: mainWorkspaceId });
      const { state, headOffset } = yield* materialize(mainWorkspaceId);

      if (state.hasProjects()) return;
      let head = headOffset;

      for (const change of mainWorkspaceSeed(yield* stamp("seed"))) {
        head = (yield* append(mainWorkspaceId, change, head)).offset;
      }
    }).pipe(Effect.withSpan("IssueTracker.seed"));

    return IssueTracker.of({ materialize, mutate, create, seed, stamp });
  }),
);

/** Boot step: seed the known workspace before the listener starts. */
export const SeedLive = Layer.effectDiscard(
  Effect.flatMap(IssueTracker, (tracker) => tracker.seed),
);
