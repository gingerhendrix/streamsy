import { Context, Effect, Option, Semaphore } from "effect";
import { CommitHooks, commitHookRegistry } from "../../internal/commit-hooks.ts";
import { OwnerRecords } from "../../internal/owner-records.ts";
import { attachOwner } from "../../internal/owner.ts";
import { CommitBoundary } from "../commit-boundary.ts";
import type { StreamId } from "../../schema/index.ts";
import { copyState, type State } from "./state.ts";

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

    return {
      boundary: attachOwner(boundary, owner),
      records: attachOwner(records, owner),
      hooks: attachOwner(CommitHooks.of({ beforeCommit: hooks.beforeCommit }), owner),
      withTransaction,
      access,
      changed,
    };
  });
