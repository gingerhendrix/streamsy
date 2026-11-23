import { Array, Cause, Context, Effect, Exit, Option, Predicate, Schema, Semaphore } from "effect";
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
  readonly participants: Set<TransactionParticipant>;
  active: boolean;
  changed: boolean;
}

class ActiveDraft extends Context.Service<ActiveDraft, Draft>()(
  "@streamsy/core/internal/MemoryActiveDraft",
) {}

const activeDraft = (
  owner: Draft["owner"],
  reject = () => Effect.die(new Error("Memory boundary requires the active owner fiber")),
) =>
  Effect.flatMap(Effect.serviceOption(ActiveDraft), (draft) =>
    Effect.withFiber((fiber) => {
      if (
        Option.isSome(draft) &&
        (draft.value.owner !== owner || !draft.value.active || draft.value.fiberId !== fiber.id)
      )
        return reject();

      return Effect.succeed(draft);
    }),
  );

export interface TransactionParticipant {
  /** Runs synchronously after the draft becomes committed, before wakes. Must not throw. */
  readonly onCommit: () => void;
  /**
   * Runs synchronously when the outer transaction rolls back. Must not throw.
   * A throw adds a defect to the transaction's cause. Effect's typed recovery
   * gives defects precedence even when the exit still contains a typed failure.
   */
  readonly onRollback: () => void;
}

const MemoryEnlistDefectType = Symbol.for("@streamsy/core/internal/MemoryEnlistDefect");

class MemoryEnlistDefect extends Schema.TaggedError<
  MemoryEnlistDefect,
  { readonly brand: unique symbol }
>()("MemoryEnlistDefect", {
  message: Schema.String,
}) {
  readonly [MemoryEnlistDefectType] = true;
}

/** A participant could not join a memory transaction on its owner fiber. */
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- This predicate narrows a runtime defect.
export const isMemoryEnlistDefect = (value: unknown): value is MemoryEnlistDefect =>
  Predicate.hasProperty(value, MemoryEnlistDefectType) && value[MemoryEnlistDefectType] === true;

const rejectEnlist = () =>
  Effect.die(
    new MemoryEnlistDefect({ message: "Enlisting requires the active memory owner fiber" }),
  );

/** Join the memory owner transaction open on this fiber. */
// oxlint-disable-next-line anti-slop/no-object-parameters -- The owner is an opaque object identity.
export const enlist = (owner: object, participant: TransactionParticipant): Effect.Effect<void> =>
  Effect.flatMap(activeDraft(owner, rejectEnlist), (draft) => {
    if (Option.isNone(draft)) return rejectEnlist();

    draft.value.participants.add(participant);

    return Effect.void;
  });

const notifyParticipants = (
  participants: Iterable<TransactionParticipant>,
  callback: "onCommit" | "onRollback",
) => {
  const defects: unknown[] = [];

  for (const participant of participants) {
    try {
      participant[callback]();
    } catch (defect) {
      defects.push(defect);
    }
  }

  if (defects.length > 0)
    throw new AggregateError(defects, `Memory participant ${callback} failed`);
};

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

    const current = activeDraft(owner);

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
              participants: new Set(),
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
              Effect.exit(restore(withHooks)).pipe(
                Effect.flatMap((exit) => {
                  if (Exit.isFailure(exit))
                    return Effect.sync(() =>
                      notifyParticipants(
                        Array.reverse(Array.fromIterable(draft.participants)),
                        "onRollback",
                      ),
                    ).pipe(
                      // Keep the body's Fail reasons alongside the callback defect in the exit.
                      Effect.catchCause((rollback) =>
                        Effect.failCause(Cause.combine(exit.cause, rollback)),
                      ),
                      Effect.andThen(exit),
                    );

                  return Effect.sync(() => {
                    Object.assign(committed, draft.state);
                    committedRecords = draft.records;
                    committedMarks = draft.marks;
                    notifyParticipants(draft.participants, "onCommit");
                  }).pipe(
                    // Even a defective participant cannot suppress wakes for committed writes.
                    Effect.ensuring(
                      Effect.suspend(() => (draft.changed ? publish(draft.ids) : Effect.void)),
                    ),
                    Effect.as(exit.value),
                  );
                }),
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
      list: (prefix) =>
        Effect.map(current, (draft) =>
          Array.filter(
            Array.fromIterable(Option.isSome(draft) ? draft.value.records : committedRecords),
            ([key]) => key.startsWith(prefix),
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

    const writeMarks = <A>(f: (marks: Marks.MarkTable) => A) =>
      withTransaction(
        Effect.map(current, (draft) => {
          if (Option.isNone(draft)) throw new Error("Memory mark write requires an owner");

          return f(draft.value.marks);
        }),
      );

    const marks = ProjectionMarks.of({
      mark: (projection, params) => writeMarks((table) => Marks.mark(table, projection, params)),
      nextDue: (projection) => readMarks((table) => Marks.nextDue(table, projection)),
      summary: (projection, now) => readMarks((table) => Marks.summary(table, projection, now)),
      due: (projection, now, limit) =>
        readMarks((table) => Marks.due(table, projection, now, limit)),
      read: (projection, params) => readMarks((table) => Marks.read(table, projection, params)),
      clear: (projection, params, seq) =>
        writeMarks((table) => Marks.clear(table, projection, params, seq)),
      sleep: (projection, params, seq, wakeAt) =>
        writeMarks((table) => Marks.sleep(table, projection, params, seq, wakeAt)),
      remove: (projection, params) =>
        writeMarks((table) => Marks.remove(table, projection, params)),
      fail: (projection, params, options) =>
        writeMarks((table) => Marks.fail(table, projection, params, options)),
      resume: (projection) => writeMarks((table) => Marks.resume(table, projection)),
      failed: (projection, limit) => readMarks((table) => Marks.failed(table, projection, limit)),
      retry: (projection, params) => writeMarks((table) => Marks.retry(table, projection, params)),
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
