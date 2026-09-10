import { expect } from "bun:test";
import { Effect } from "effect";

/** An unexpected success stays a failure so the enclosing test cannot pass. */
export const expectFailureTag = <A, E extends { readonly _tag: string }, R>(
  effect: Effect.Effect<A, E, R>,
  tag: E["_tag"],
) =>
  effect.pipe(
    Effect.flip,
    Effect.tap((error) => Effect.sync(() => expect(error._tag).toBe(tag))),
  );
