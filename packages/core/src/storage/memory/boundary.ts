import { Context, Effect, Option, Semaphore } from "effect";
import { CommitHooks, commitHookRegistry } from "../../internal/commit-hooks.ts";
import { OwnerRecords } from "../../internal/owner-records.ts";
import { ProjectionMarks } from "../../internal/projection-marks.ts";
import { attachOwner } from "../../internal/owner.ts";
import { CommitBoundary } from "../commit-boundary.ts";
import type { StreamId } from "../../schema/index.ts";
import { copyState, type State } from "./state.ts";
import * as Marks from "./marks.ts";

/**
 * The memory owner's transaction. Nested calls join (no savepoints); failure must
 * escape the outer body to roll back. Bodies stay on the owner fiber and must not
 * wait for their own post-commit wakes. Each outer transaction copies the host
 * state; intended for bounded memory hosts.
 */
type WithTransaction = <A, E, R>(body: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;

interface Draft {
  readonly owner: object;
  readonly fiberId: number;
  readonly state: State;
  readonly records: Map<string, string>;
  readonly marks: Marks.MarkTable;
  readonly ids: Set<StreamId>;
  active: boolean;
  changed: boolean;
}

class ActiveDraft extends Context.Service<ActiveDraft, Draft>()(
  "@streamsy/core/internal/MemoryActiveDraft",
) {}

export const makeBoundary = (
  committed: State,
  publish: (ids: ReadonlySet<StreamId>) => Effect.Effect<void>,
) =>
  Effect.gen(function* () {
    const owner = {};
    const lock = yield* Semaphore.make(1);
    let committedRecords = new Map<string, string>();
    let committedMarks: Marks.MarkTable = new Map();
    const hooks = commitHookRegistry();

    const current = Effect.flatMap(Effect.serviceOption(ActiveDraft), (draft) =>
      Effect.withFiber((fiber) => {
        if (
          Option.isSome(draft) &&
          (draft.value.owner !== owner || !draft.value.active || draft.value.fiberId !== fiber.id)
        )
          return Effect.die(new Error("Memory boundary requires the active owner fiber"));

        return Effect.succeed(draft);
      }),
    );

    const withTransaction: WithTransaction = (body) =>
      Effect.flatMap(current, (ambient) => {
        if (Option.isSome(ambient)) return body;

        return Effect.withFiber((fiber) =>
          Effect.suspend(() => {
            const draft: Draft = {
              owner,
              fiberId: fiber.id,
              state: copyState(committed),
              records: new Map(committedRecords),
              marks: Marks.copyMarks(committedMarks),
              ids: new Set(),
              active: true,
              changed: false,
            };

            // Hooks run in the draft, so their writes commit or roll back with the body.
            // Memory hooks only write the draft, so a hook failure is a defect.
            const withHooks = body.pipe(
              Effect.tap(() => Effect.orDie(hooks.run(draft.ids))),
              Effect.provideService(ActiveDraft, draft),
            );

            return Effect.uninterruptibleMask((restore) =>
              restore(withHooks).pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    Object.assign(committed, draft.state);
                    committedRecords = draft.records;
                    committedMarks = draft.marks;
                  }).pipe(
                    Effect.andThen(
                      Effect.suspend(() => (draft.changed ? publish(draft.ids) : Effect.void)),
                    ),
                  ),
                ),
                Effect.ensuring(
                  Effect.sync(() => {
                    draft.active = false;
                  }),
                ),
              ),
            );
          }).pipe(Semaphore.withPermit(lock)),
        );
      });

    const access = <A>(f: (state: State) => A) =>
      Effect.map(current, (draft) => f(Option.isSome(draft) ? draft.value.state : committed));

    const changed = (ids: Iterable<StreamId>) =>
      Effect.map(current, (draft) => {
        if (Option.isNone(draft)) throw new Error("Memory mutation requires an owner");
        draft.value.changed = true;

        for (const id of ids) draft.value.ids.add(id);
      });

    const boundary = CommitBoundary.of({ withTransaction });

    const records = OwnerRecords.of({
      read: (key) =>
        Effect.map(current, (draft) =>
          Option.fromUndefinedOr(
            (Option.isSome(draft) ? draft.value.records : committedRecords).get(key),
          ),
        ),
      remove: (key) =>
        withTransaction(
          Effect.map(current, (draft) => {
            if (Option.isNone(draft)) throw new Error("Memory record delete requires an owner");
            draft.value.records.delete(key);
          }),
        ),
      write: (key, encoded) =>
        withTransaction(
          Effect.map(current, (draft) => {
            if (Option.isNone(draft)) throw new Error("Memory record write requires an owner");
            draft.value.records.set(key, encoded);
          }),
        ),
    });

    const readMarks = <A>(f: (marks: Marks.MarkTable) => A) =>
      Effect.map(current, (draft) => f(Option.isSome(draft) ? draft.value.marks : committedMarks));

    const writeMarks = (f: (marks: Marks.MarkTable) => void) =>
      withTransaction(
        Effect.map(current, (draft) => {
          if (Option.isNone(draft)) throw new Error("Memory mark write requires an owner");
          f(draft.value.marks);
        }),
      );

    const marks = ProjectionMarks.of({
      mark: (projection, params) => writeMarks((table) => Marks.mark(table, projection, params)),
      nextDue: (projection) => readMarks((table) => Marks.nextDue(table, projection)),
      due: (projection, now, limit) =>
        readMarks((table) => Marks.due(table, projection, now, limit)),
      read: (projection, params) => readMarks((table) => Marks.read(table, projection, params)),
      clear: (projection, params, seq) =>
        writeMarks((table) => Marks.clear(table, projection, params, seq)),
      remove: (projection, params) =>
        writeMarks((table) => Marks.remove(table, projection, params)),
      defer: (projection, params, attempts, dueAt) =>
        writeMarks((table) => Marks.defer(table, projection, params, attempts, dueAt)),
      deferred: (projection) => readMarks((table) => Marks.deferred(table, projection)),
    });

    return {
      boundary: attachOwner(boundary, owner),
      marks: attachOwner(marks, owner),
      records: attachOwner(records, owner),
      hooks: attachOwner(CommitHooks.of({ beforeCommit: hooks.beforeCommit }), owner),
      withTransaction,
      access,
      changed,
    };
  });
