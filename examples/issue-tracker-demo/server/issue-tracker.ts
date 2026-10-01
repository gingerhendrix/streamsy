import { MaterializedState } from "@durable-streams/state";
import { ZERO_OFFSET } from "@streamsy/core";
import {
  Context,
  Crypto,
  DateTime,
  Effect,
  Layer,
  Predicate,
  Schedule,
  Schema,
  Stream,
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
import { workspaceEvents, Workspaces, type WorkspaceChange } from "./streams.ts";

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
    readonly materialize: (workspaceId: string) => Effect.Effect<Materialized, WorkspaceNotFound>;
    /** Read, fold, append with `expectedOffset`; retry only an `OffsetMismatch`. */
    readonly mutate: <A, E>(
      workspaceId: string,
      attempt: (state: WorkspaceState) => Effect.Effect<Mutation<A>, E>,
    ) => Effect.Effect<Appended<A>, E | WorkspaceNotFound | WriteConflict>;
    /** Create a shared workspace with a starter project and return its id. */
    readonly create: Effect.Effect<string>;
    /** Ensure and seed the known demo workspace. */
    readonly seed: Effect.Effect<void>;
    /** Choose the id, time and txid for one write. */
    readonly stamp: (prefix: string, txid?: string) => Effect.Effect<Stamp>;
  }
>()("IssueTrackerDemo/IssueTracker") {}

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

    // Each change records the head it was validated against as its source
    // offset, and the append only lands if that head is still the tail. A lost
    // race stays typed for the retry; any other storage failure is a defect.
    const append = (workspaceId: string, change: WorkspaceChange, head: string) =>
      workspaces
        .append({ workspaceId }, workspaceEvents(workspaceId, head, [change]), {
          expectedOffset: head,
        })
        .pipe(
          Effect.catch((error) =>
            Predicate.isTagged(error, "OffsetMismatch") ? Effect.fail(error) : Effect.die(error),
          ),
        );

    const materialize = Effect.fn("IssueTracker.materialize")(function* (workspaceId: string) {
      const batches = yield* workspaces.read({ workspaceId }).pipe(
        Stream.runCollect,
        Effect.catch((error) =>
          Predicate.isTagged(error, "StreamNotFound") || Predicate.isTagged(error, "StreamGone")
            ? Effect.fail(new WorkspaceNotFound({ workspaceId }))
            : Effect.die(error),
        ),
      );
      const state = new MaterializedState();
      for (const batch of batches) for (const event of batch.items) state.apply(event);
      return {
        state: {
          getProject: (projectId: string) => state.get<Project>("project", projectId),
          getIssue: (issueId: string) => state.get<Issue>("issue", issueId),
          hasProjects: () => state.getType("project").size > 0,
        },
        headOffset: batches.at(-1)?.nextOffset ?? ZERO_OFFSET,
      };
    });

    const mutate = <A, E>(
      workspaceId: string,
      attempt: (state: WorkspaceState) => Effect.Effect<Mutation<A>, E>,
    ) =>
      Effect.gen(function* () {
        const { state, headOffset } = yield* materialize(workspaceId);
        const { change, value } = yield* attempt(state);
        const ack = yield* append(workspaceId, change, headOffset);
        return { value, awaitOffset: ack.offset };
      }).pipe(
        Effect.retry({
          schedule: Schedule.recurs(3),
          while: Predicate.isTagged("OffsetMismatch"),
        }),
        Effect.catchTag("OffsetMismatch", () => Effect.fail(new WriteConflict({ workspaceId }))),
        Effect.withSpan("IssueTracker.mutate"),
      );

    const create = Effect.gen(function* () {
      const bytes = yield* crypto.randomBytes(10);
      const workspaceId = Array.from(bytes, (byte) => (byte % 36).toString(36)).join("");
      yield* workspaces.create({ workspaceId });
      const starter = yield* stamp("proj");
      yield* append(workspaceId, projectUpsert(starterProject(starter), starter), ZERO_OFFSET);
      return workspaceId;
    }).pipe(Effect.orDie, Effect.withSpan("IssueTracker.create"));

    const seed = Effect.gen(function* () {
      yield* workspaces.create({ workspaceId: mainWorkspaceId });
      const { state, headOffset } = yield* materialize(mainWorkspaceId);
      if (state.hasProjects()) return;
      let head = headOffset;
      for (const change of mainWorkspaceSeed(yield* stamp("seed"))) {
        head = (yield* append(mainWorkspaceId, change, head)).offset;
      }
    }).pipe(Effect.orDie, Effect.withSpan("IssueTracker.seed"));

    return IssueTracker.of({ materialize, mutate, create, seed, stamp });
  }),
);

/** Boot step: seed the known workspace before the listener starts. */
export const SeedLive = Layer.effectDiscard(
  Effect.flatMap(IssueTracker, (tracker) => tracker.seed),
);
