import { Context, Effect, Option, Semaphore } from "effect";
import { copyState, type State } from "./state.ts";

/**
 * Internal fused-host seam. Encoded records are owned by the caller.
 * Nested calls join (no savepoints); failure must escape the outer body to roll back.
 * Bodies stay on the owner fiber and must not wait for their own post-commit wakes.
 * Each outer transaction copies the host state; intended for bounded memory hosts.
 */
export interface MemoryCommitBoundaryApi {
  readonly withTransaction: <A, E, R>(body: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  readonly read: (key: string) => Effect.Effect<Option.Option<string>>;
  readonly remove: (key: string) => Effect.Effect<void>;
  readonly write: (key: string, encoded: string) => Effect.Effect<void>;
}
export class MemoryCommitBoundary extends Context.Service<
  MemoryCommitBoundary,
  MemoryCommitBoundaryApi
>()("@streamsy/core/internal/MemoryCommitBoundary") {}

interface Draft {
  readonly owner: symbol;
  readonly fiberId: number;
  readonly state: State;
  readonly records: Map<string, string>;
  active: boolean;
  changed: boolean;
}
class ActiveDraft extends Context.Service<ActiveDraft, Draft>()(
  "@streamsy/core/internal/MemoryActiveDraft",
) {}

export const makeBoundary = (committed: State, publish: Effect.Effect<void>) =>
  Effect.gen(function* () {
    const owner = Symbol();
    const lock = yield* Semaphore.make(1);
    let records = new Map<string, string>();
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
    const withTransaction: MemoryCommitBoundaryApi["withTransaction"] = (body) =>
      Effect.flatMap(current, (ambient) => {
        if (Option.isSome(ambient)) return body;
        return Effect.withFiber((fiber) =>
          Effect.suspend(() => {
            const draft: Draft = {
              owner,
              fiberId: fiber.id,
              state: copyState(committed),
              records: new Map(records),
              active: true,
              changed: false,
            };
            return Effect.uninterruptibleMask((restore) =>
              restore(body.pipe(Effect.provideService(ActiveDraft, draft))).pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    Object.assign(committed, draft.state);
                    records = draft.records;
                  }).pipe(
                    Effect.andThen(Effect.suspend(() => (draft.changed ? publish : Effect.void))),
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
    const changed = Effect.map(current, (draft) => {
      if (Option.isNone(draft)) throw new Error("Memory mutation requires an owner");
      draft.value.changed = true;
    });
    const api = MemoryCommitBoundary.of({
      withTransaction,
      read: (key) =>
        Effect.map(current, (draft) =>
          Option.fromUndefinedOr((Option.isSome(draft) ? draft.value.records : records).get(key)),
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
    return { api, access, changed };
  });
