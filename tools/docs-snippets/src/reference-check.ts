/**
 * Checks for the signatures on reference pages. A reference block declares the
 * signature that readers see, then hides a call that compares it with the real
 * export, for example `same<typeof next, typeof Producer.next>(true)`. The call
 * does not compile when the two types differ.
 */
import type { Cause } from "effect";

/**
 * `true` when `A` and `B` are the same type. A missing optional field, a missing
 * `readonly` or a missing optional parameter makes them differ. Parameter names
 * do not count.
 */
export type Same<A, B> =
  // oxlint-disable-next-line typescript/no-unnecessary-type-parameters -- the unused T makes TypeScript compare A and B as identical types, not as assignable ones.
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

/** Compiles only when `A` and `B` are the same type. */
export declare const same: <A, B>(check: Same<A, B>) => void;

/**
 * The fields of a `Schema.TaggedError` instance: its `_tag` and schema fields,
 * without the members that every Effect error has.
 */
export type Fields<E> = Pick<E, Exclude<keyof E, keyof Cause.YieldableError>>;

/**
 * The properties of `T` in one object type. `Same` can report two intersections as
 * different when their members are the same, so compare `Flat<A>` with `Flat<B>` when
 * a type is an intersection.
 */
export type Flat<T> = { [K in keyof T]: T[K] };
