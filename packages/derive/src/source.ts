import type { Effect } from "effect";
import type { DeriveFault } from "./fault.ts";

export interface PullLimits {
  readonly items: number;
  readonly bytes?: number;
}
export interface Boundary<A> {
  readonly _tag: "Boundary";
  readonly items: ReadonlyArray<A>;
  readonly endPosition: string;
  readonly bytes?: number;
  readonly upToDate: boolean;
  readonly closed: boolean;
}
export interface Source<A> {
  readonly identity: string;
  readonly initialPosition: string;
  readonly pull: (
    after: string,
    limits: PullLimits,
  ) => Effect.Effect<Boundary<A> | { readonly _tag: "LimitReached" }, DeriveFault>;
  /** A hint only. A missed wake is repaired by follow's timeout and authoritative pull. */
  readonly wait: (after: string) => Effect.Effect<void, DeriveFault>;
}
