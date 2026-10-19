import type {
  AppendFailure,
  CreateFailure,
  HeadFailure,
  OffsetMismatch,
  ReadFailure,
  StreamGone,
  StreamNotFound,
} from "@streamsy/core";
import { CheckpointStore, Projection, type ProjectionFault } from "@streamsy/projection";
import {
  Array,
  Context,
  Crypto,
  DateTime,
  Effect,
  Layer,
  Result,
  Schema,
  type PlatformError,
} from "effect";
import { commandView, type CommandView } from "./command-view.ts";
import {
  mainWorkspaceSeed,
  starterProject,
  createProject,
  type Decision,
  type Stamp,
} from "./decide.ts";
import { mainWorkspaceId } from "./config.ts";
import { ProjectionWatchers } from "./watchers.ts";
import { WorkspaceEvents, WorkspaceRegistry, Workspaces } from "./streams.ts";

export class WorkspaceNotFound extends Schema.TaggedError<
  WorkspaceNotFound,
  { readonly brand: unique symbol }
>()("WorkspaceNotFound", { workspaceId: Schema.String }) {}

/** The CAS append still lost after three retries. */
export class WriteConflict extends Schema.TaggedError<
  WriteConflict,
  { readonly brand: unique symbol }
>()("WriteConflict", {
  workspaceId: Schema.String,
}) {}

/** A store failure that the API answers with 500. */
export type StoreFailure =
  | Exclude<HeadFailure, StreamNotFound | StreamGone>
  | Exclude<AppendFailure<{ expectedOffset: string }>, OffsetMismatch>
  | CreateFailure
  | AppendFailure
  | ReadFailure
  | ProjectionFault
  | PlatformError.PlatformError;

export type IssueTrackerFailure = WorkspaceNotFound | WriteConflict | StoreFailure;

/**
 * A landed command: the value to answer with, and the event stream offset after
 * its events. The browser waits on the txid instead, because the State stream
 * has its own offsets.
 */
export interface Appended<A> {
  readonly value: A;
  readonly awaitOffset: string;
}

/** Run a decision against the command view at the head of the event stream, then append. */
const decideAt = <A, E>(
  workspaceId: string,
  decision: (view: CommandView) => Effect.Effect<Decision<A>, E>,
) =>
  Projection.decide(commandView.at({ workspaceId }), {
    input: "events",
    // A domain failure is an answer at the offset that was read, so it appends nothing.
    decide: ({ state }) =>
      Effect.map(Effect.result(decision(state)), (answer) => ({
        value: answer,
        items: Result.isSuccess(answer) ? answer.success.events : [],
      })),
  }).pipe(
    Effect.catchTag("OffsetMismatch", () => Effect.fail(new WriteConflict({ workspaceId }))),
    Effect.flatMap(({ value: answer, offset }) =>
      Effect.map(Effect.fromResult(answer), ({ value }) => ({ value, awaitOffset: offset })),
    ),
  );

/** The issue tracker's domain: every command on a workspace's event stream. */
export class IssueTracker extends Context.Service<
  IssueTracker,
  {
    /** Decide against the command view and append the events. A lost race is retried three times. */
    readonly mutate: <A, E>(
      workspaceId: string,
      decision: (view: CommandView) => Effect.Effect<Decision<A>, E>,
    ) => Effect.Effect<Appended<A>, E | IssueTrackerFailure>;
    /** Create a shared workspace with a starter project and return its id. */
    readonly create: Effect.Effect<string, IssueTrackerFailure>;
    /** Ensure and seed the known demo workspace, and start every projection watcher. */
    readonly boot: Effect.Effect<void, IssueTrackerFailure>;
    /** Choose the id, time and txid for one write. */
    readonly stamp: (prefix: string, txid?: string) => Effect.Effect<Stamp>;
  }
>()("IssueTrackerEventSourced/IssueTracker") {}

export const IssueTrackerLive = Layer.effect(
  IssueTracker,
  Effect.gen(function* () {
    const events = yield* WorkspaceEvents;
    const rows = yield* Workspaces;
    const registry = yield* WorkspaceRegistry;
    const watchers = yield* ProjectionWatchers;
    const crypto = yield* Crypto.Crypto;
    // The projection runners need the storage services at each call.
    const context = yield* Effect.context<CheckpointStore | WorkspaceEvents>();

    const stamp = Effect.fn("IssueTracker.stamp")(function* (prefix: string, txid?: string) {
      const now = DateTime.formatIso(yield* DateTime.now);
      const id = `${prefix}_${(yield* crypto.randomUUIDv4()).slice(0, 8)}`;

      return { id, now, txid: txid ?? (yield* crypto.randomUUIDv4()) };
    }, Effect.orDie);

    /** The event stream and the State stream of a workspace, and its projection watcher. */
    const open = Effect.fn("IssueTracker.open")(function* (workspaceId: string) {
      yield* events.create({ workspaceId });
      yield* rows.create({ workspaceId });
      yield* watchers.start(workspaceId);
    });

    const mutate = <A, E>(
      workspaceId: string,
      decision: (view: CommandView) => Effect.Effect<Decision<A>, E>,
    ) =>
      events.head({ workspaceId }).pipe(
        Effect.catchTag(
          ["StreamNotFound", "StreamGone"],
          () => new WorkspaceNotFound({ workspaceId }),
        ),
        Effect.andThen(decideAt(workspaceId, decision)),
        Effect.provide(context),
        Effect.withSpan("IssueTracker.mutate"),
      );

    const create = Effect.gen(function* () {
      const bytes = yield* crypto.randomBytes(10);

      const workspaceId = Array.join(
        globalThis.Array.from(bytes, (byte) => (byte % 36).toString(36)),
        "",
      );

      yield* open(workspaceId);
      const starter = yield* stamp("proj");
      yield* registry.append({}, [{ workspaceId, createdAt: starter.now }]);
      yield* mutate(workspaceId, createProject(starterProject(starter), starter));

      return workspaceId;
    }).pipe(Effect.withSpan("IssueTracker.create"));

    // The seed appends at the head it read, and only lands if that head is
    // still the tail. A view that has projects is already seeded.
    const seed = Effect.gen(function* () {
      const seedStamp = yield* stamp("seed");
      yield* mutate(mainWorkspaceId, (view) =>
        Effect.succeed({
          events: view.projectIds.length > 0 ? [] : mainWorkspaceSeed(seedStamp),
          value: undefined,
        }),
      );
    });

    const boot = Effect.gen(function* () {
      yield* registry.create({});
      yield* open(mainWorkspaceId);
      yield* seed;
      const registered = yield* registry.collect({});

      for (const { workspaceId } of registered.items) yield* watchers.start(workspaceId);
    }).pipe(Effect.withSpan("IssueTracker.boot"));

    return IssueTracker.of({ mutate, create, boot, stamp });
  }),
);

/** Boot step: open and seed `main`, and start the watchers, before the listener starts. */
export const BootLive = Layer.effectDiscard(
  Effect.flatMap(IssueTracker, (tracker) => tracker.boot),
);
