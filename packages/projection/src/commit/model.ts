import { extend } from "./range.ts";
import { Data, Schema, type Effect } from "effect";
import { pipeArguments, type Pipeable } from "effect/Pipeable";
import type { StreamAddress } from "@streamsy/core";
import type { CheckpointRecord, Loaded, ProjectionKey } from "../checkpoint-store.ts";
import type { ProjectionFault } from "../fault.ts";
import type { Unit } from "../unit.ts";

export const TypeId = "~@streamsy/projection/Commit";
export interface Variance<in Result, in Provides, out Requires> {
  readonly result: (_: Result) => void;
  readonly provides: (_: Provides) => void;
  readonly requires: () => Requires;
}
export interface Commit<in Result, in Provides = never, out Requires = never> extends Pipeable {
  readonly [TypeId]: Variance<Result, Provides, Requires>;
  readonly name: string;
  readonly guarantee: "exactly-once" | "effectively-once" | "at-least-once";
  readonly load: (key: ProjectionKey) => Effect.Effect<Loaded, ProjectionFault, Requires>;
  readonly apply: (
    pending: Pending<Result, Provides>,
  ) => Effect.Effect<void, ProjectionFault | HandlerFailed, Requires>;
  readonly remove?: (key: ProjectionKey) => Effect.Effect<void, ProjectionFault, Requires>;
}
export interface Pending<Result, Provides = never> {
  readonly key: ProjectionKey;
  readonly unit: Unit;
  readonly loaded: Loaded;
  readonly next: CheckpointRecord;
  readonly run: Effect.Effect<Result, HandlerFailed, Provides>;
}
/** Opaque to Commit authors; the kernel restores the original typed handler error. */
export class HandlerFailed extends Schema.TaggedError<HandlerFailed>()("HandlerFailed", {
  error: Schema.Unknown,
}) {}
export interface Folded<S> {
  readonly _tag: "Folded";
  readonly value: S;
  readonly encoded: string;
}
export interface Emitted<O> {
  readonly _tag: "Emitted";
  readonly outputs: OutputBatch<O>;
}
export interface Accumulated<S, O> {
  readonly _tag: "Accumulated";
  readonly value: S;
  readonly encoded: string;
  readonly outputs: OutputBatch<O>;
}
/** Output names survive type erasure; each value is represented by its resolved batch entry. */
export type OutputNames = Readonly<Record<string, never>>;
/** Resolved addresses and encoded row changes, in declaration and entry order. */
export type OutputBatch<O> = {
  readonly [K in keyof O]: {
    readonly address: StreamAddress.StreamAddress<unknown>;
    readonly items: ReadonlyArray<unknown>;
  };
};
export const { Folded, Emitted, Accumulated } = Data.taggedEnum<
  Folded<unknown> | Emitted<OutputNames> | Accumulated<unknown, OutputNames>
>();

/** Custom Commits extend the input range on retry. Never store next when run fails;
 * condition the write on loaded.token (checkpoint / token-conflict when it moved),
 * and return token "0" for an absent record.
 * load must return the stored state. If run and the write are not atomic,
 * declare at-least-once. The guarantee must describe the actual write. */
export const make = <Result, Provides = never, Requires = never>(
  options: Omit<Commit<Result, Provides, Requires>, typeof TypeId | "pipe">,
): Commit<Result, Provides, Requires> =>
  extend({
    ...options,
    [TypeId]: {
      result: (_: Result) => {},
      provides: (_: Provides) => {},
      requires: () => {
        throw new Error("Commit variance is type-only");
      },
    },
    pipe() {
      return pipeArguments(this, arguments);
    },
  });
